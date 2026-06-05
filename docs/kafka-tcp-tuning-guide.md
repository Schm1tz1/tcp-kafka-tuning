# TCP Kafka Tuning

> **Technical Reference** — *Measurement-driven configuration guide for TCP and Apache Kafka*

|                |                                                                                                                                                                             |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Subject**    | Network performance analysis, TCP congestion control, distributed streaming systems                                                                                         |
| **Scope**      | Local datacenter through multi-region and satellite deployments                                                                                                             |
| **Topics**     | TCP windowing, Bandwidth-Delay Product, Little's Law, Mathis equation, BBR congestion control, IP fragmentation, MSS/MTU, Apache Kafka batch geometry, lz4/zstd compression |
| **Tools**      | ping, iperf3, ss, sysctl, kafka-producer-perf-test.sh                                                                                                                       |
| **Automation** | kafka-tcp-measure.sh / kafka-tcp-analyze.sh                                                                                                                                 |
| **Status**     | For review and distribution                                                                                                                                                 |

**Abstract**

This document presents a quantitative framework for diagnosing and optimising throughput in deployments combining TCP/IP networks with Apache Kafka streaming systems. The analysis derives from established results in queueing theory and network performance modelling — principally Little's Law \[1\], the Bandwidth-Delay Product formulation of RFC 1323 \[2\], the Mathis throughput bound \[3\], and the BBR congestion-control model \[4\] — and applies them systematically across the IP, TCP, and Kafka layers. A structured measurement methodology based on ping and iperf3 is described, together with formulae for deriving optimal kernel buffer ceilings, Kafka batch geometry, and congestion-control parameters from empirical path measurements. Recommendations are presented for seven representative deployment scenarios, ranging from sub-millisecond intra-datacenter paths to high-latency satellite links.

**Keywords:** *TCP congestion control, Bandwidth-Delay Product, Apache Kafka, throughput optimisation, BBR, window scaling, batch sizing, network tuning*


> **Note on this document**
>
> Section numbering follows standard technical report conventions. Formula identifiers (F1–F12) are used consistently throughout for cross-referencing. Reference citations are given in bracketed numeric form [n] with full entries in Section 10. All configuration parameters are presented in monospace font and are case-sensitive.


# 1.  Introduction

The achievable throughput of a TCP connection is determined by the interplay of three physical quantities: available bandwidth, round-trip propagation delay (RTT), and the sender's congestion window. When these are misaligned — in particular when the window is smaller than the product of bandwidth and delay — the sending host is forced to idle while awaiting acknowledgements, permanently wasting link capacity. This phenomenon, well characterised in the RFC 1323 Long Fat Network analysis \[2\], becomes acute at modern link speeds and for any path with non-trivial propagation delay.

Apache Kafka producers introduce an additional buffering layer above TCP: the record batch. Because Kafka transmits data in discrete batches governed by batch.size and linger.ms, its effective TCP window is the product of batch size and pipeline depth. When this product falls below the Bandwidth-Delay Product (BDP) of the underlying path, Kafka producers exhibit the same stall behaviour as an undersized TCP window, regardless of the configured socket buffers.

This document provides: (i) the theoretical basis for throughput calculation at each layer; (ii) a structured empirical measurement procedure; (iii) algebraic derivations of all tuning parameters from measurement results; and (iv) scenario-specific configuration recommendations. The treatment is self-contained, with all referenced formulae stated explicitly and attributed to primary sources.

## 1.1  Notation

|            |               |                                                   |
|------------|---------------|---------------------------------------------------|
| **Symbol** | **Unit**      | **Definition**                                    |
| W          | bytes         | TCP receive/congestion window size                |
| RTT        | seconds       | Round-trip propagation delay                      |
| BDP        | bytes         | Bandwidth-Delay Product = B × RTT                 |
| B          | bytes/s       | Available bottleneck bandwidth                    |
| MSS        | bytes         | Maximum Segment Size = MTU − 40                   |
| MTU        | bytes         | Maximum Transmission Unit of the path             |
| p          | dimensionless | Packet loss probability ∈ \[0, 1\]                |
| cwnd       | MSS           | Congestion window (sender-side, in segments)      |
| T_RTO      | seconds       | Retransmission timeout                            |
| BtlBw      | bytes/s       | BBR bottleneck bandwidth estimate                 |
| RTprop     | seconds       | BBR round-trip propagation estimate (minimum RTT) |

# 2.  Theoretical Foundations

## 2.1  Little's Law and the window-limited throughput equation

The fundamental throughput constraint of a TCP connection follows directly from Little's Law \[1\], a result from queueing theory which states that for any stable system the mean number of items in the system *L* equals the mean arrival rate *λ* multiplied by the mean sojourn time *W*:

```
L = λ × W (Little, 1961)
```

Applied to a TCP connection: the number of bytes in flight corresponds to *L*; the throughput corresponds to *λ*; and the round-trip time corresponds to *W*. Solving for throughput yields the **window-limited throughput equation**:

```
T = W / RTT (F1)
```

This result, while elementary, is the single most consequential formula in TCP performance analysis. RFC 1323 \[2\] used it explicitly to motivate the introduction of the window scale option: for a 1 Gbit/s link with RTT = 100 ms, F1 requires W ≥ 12.5 MB to sustain full utilisation, far exceeding the 65,535-byte ceiling of the original 16-bit field \[5\].

## 2.2  The Bandwidth-Delay Product

The **Bandwidth-Delay Product** (BDP) is the minimum window size required to keep a connection fully utilised. It is obtained by substituting the link capacity *B* for *T* in F1 and solving for *W*:

```
BDP = B × RTT (F2)
```

RFC 1323 \[2\] introduced the term in the context of Long Fat Networks (LFNs), defined as paths with BDP exceeding 10<sup>5</sup> bits. Three equivalent physical interpretations of the BDP are summarised in Table 1.

**Table 1.** *Physical interpretations of the Bandwidth-Delay Product.*

|                      |                                                                                   |
|----------------------|-----------------------------------------------------------------------------------|
| **Interpretation**   | **Description**                                                                   |
| **Pipe volume**      | The total number of bytes simultaneously in flight on the connection              |
| **Work in progress** | Data transmitted by the sender but not yet acknowledged by the receiver           |
| **Minimum buffer**   | The volume the receiver must accommodate before the first acknowledgement returns |

## 2.3  Loss-limited throughput: the Mathis equation

In the presence of packet loss, the achievable throughput is bounded not by the window but by the rate at which losses can be recovered. Mathis et al. \[3\] derived the following macroscopic model for TCP congestion avoidance under steady-state loss:

```
T ≤ (MSS / RTT) × (1 / √p) (F4)
```

where *p* is the packet loss probability. This bound is independent of the configured window size; even an arbitrarily large buffer cannot overcome the throughput penalty imposed by loss. At *p* = 0.01 (1%) and RTT = 50 ms with MSS = 1460 bytes, F4 yields a ceiling of approximately 2.3 Mbit/s, regardless of available bandwidth.

A more precise model accounting for retransmission timeout (RTO) events was derived by Padhye et al. \[6\]:

```
T ≈ MSS / \[ RTT × √(2p/3) + T_RTO × min(1, 3√(3p/8)) × p × (1 + 32p²) \] (F5)
```

F5 reduces to F4 when RTO events are negligible (low loss). At loss rates above approximately 1%, the RTO term in F5 dominates and F4 substantially overestimates achievable throughput.

## 2.4  Congestion control: AIMD and slow start

Jacobson \[7\] introduced two mechanisms that govern the evolution of cwnd:

- **Slow start:** cwnd doubles each round-trip until it reaches the slow-start threshold ssthresh, providing exponential growth from connection establishment.

- **Additive Increase Multiplicative Decrease (AIMD):** in the congestion-avoidance phase, \[object Object\]

These mechanisms are formalised in RFC 5681 \[9\]. The sawtooth pattern they produce implies that a CUBIC or Reno connection operates chronically below the BDP between loss events, particularly on high-BDP paths — a structural inefficiency that BBR is designed to eliminate (Section 2.5).

## 2.5  BBR: model-based congestion control

BBR (Bottleneck Bandwidth and Round-trip propagation time) \[4\] departs from loss-based signalling by continuously estimating two path properties:

- **BtlBw**: the maximum observed delivery rate over a recent time window, representing the bottleneck bandwidth.

- **RTprop**: the minimum observed RTT over a 10-second window, representing the propagation delay in the absence of queuing.

From these estimates, BBR derives its target operating point:

```
Inflight target = BtlBw × RTprop = BDP (F9)
```

BBR thus implements F2 as a real-time control law, targeting exactly the BDP without inducing queue growth. The pacing rate is set to BtlBw, requiring the Fair Queue (fq) packet scheduler \[10\] to enforce per-flow pacing; without fq, BBR's pacing guarantees are void.


> **Implementation note**
>
> BBR requires net.core.default_qdisc = fq to function correctly. The default Linux scheduler pfifo_fast does not support per-flow pacing. Setting tcp_congestion_control = bbr without the corresponding fq qdisc degrades BBR to approximately CUBIC behaviour and eliminates its latency advantages.


# 3.  Formula Summary

Table 2 consolidates all formulae referenced in this document, with their primary sources. F1 is the master relation; all others are either derivations of F1 or describe conditions under which F1 cannot be maintained.

**Table 2.** *Formula reference with primary citations.*

|         |                                |                                           |                                     |
|---------|--------------------------------|-------------------------------------------|-------------------------------------|
| **ID**  | **Name**                       | **Formula**                               | **Primary source**                  |
| **F1**  | **Window-limited throughput**  | T = W / RTT                               | Little \[1\]; RFC 1323 \[2\]        |
| **F2**  | **Bandwidth-Delay Product**    | BDP = B × RTT                             | RFC 1323 \[2\]                      |
| **F3**  | **Throughput, practical form** | T_max = (W × 8) / RTT \[bit/s\]           | Stevens \[11\], Ch. 20              |
| **F4**  | **Mathis bound**               | T ≤ (MSS/RTT) × (1/√p)                    | Mathis et al. \[3\]                 |
| **F5**  | **Padhye refined model**       | T ≈ MSS / \[RTT×√(2p/3) + T_RTO×…\]       | Padhye et al. \[6\]                 |
| **F6**  | **Slow start**                 | cwnd(t+1) = 2 × cwnd(t) per RTT           | Jacobson \[7\]; RFC 5681 \[9\]      |
| **F7**  | **AIMD (cong. avoidance)**     | cwnd += MSS²/cwnd per ACK                 | Jacobson \[7\]; Chiu & Jain \[8\]   |
| **F8**  | **MSS from MTU**               | MSS = MTU − 40 (IPv4, no options)         | RFC 879 \[5\]; RFC 1191 \[12\]      |
| **F9**  | **BBR operating point**        | Inflight = BtlBw × RTprop                 | Cardwell et al. \[4\]               |
| **F10** | **Kafka effective window**     | W_eff = batch.size × max.in.flight        | Derived from F1 (application layer) |
| **F11** | **Kafka linger (throughput)**  | linger_t = (batch.size×8)/B × 1000 \[ms\] | Derived from F1                     |
| **F12** | **Kafka linger (latency)**     | linger_l = budget − RTT − t_broker \[ms\] | Latency budget decomposition        |
| **F13** | **Consumer fetch window**      | max.partition.fetch.bytes ≥ batch.size    | Derived from F1 (receive side)      |
| **F14** | **Consumer fetch wait**        | fetch.max.wait.ms (tradeoff parameter)    | Symmetric to F11 (receive batching) |
| **F15** | **Replica lag timeout**        | RTT×4 + fetch.max.wait + 5000 \[ms\]      | Timeout budget decomposition        |
| **F16** | **Replica fetcher count**      | num.fetchers = ceil(partitions / 6)       | Parallelism scaling heuristic       |
| **F17** | **Replica fetch size**         | replica.fetch.max.bytes ≥ batch.size      | Full-batch replication              |
| **F18** | **Replication amplification**  | write_amplification = 1 + (RF−1)          | Leader node bandwidth multiplier    |
| **F19** | **Per-broker bandwidth budget**| producer + consumer + repl_in + repl_out  | NIC saturation constraint (cloud)   |
| **F20** | **Effective MSS in hybrid paths** | MSS_eff = min(MTU_vpc, MTU_internet) − 40 | Cloud egress constraint             |

# 4.  Layer Overhead Analysis

## 4.1  Protocol header overhead

On a standard Ethernet path (MTU = 1500 bytes), the usable Kafka record payload per TCP segment is constrained by cumulative header overhead across four protocol layers:

```
Ethernet frame 1518 bytes (1522 with 802.1Q VLAN tag)

IPv4 header 20 bytes (no options)

TCP header 32 bytes (with SACK and timestamps, RFC 1323)

Kafka RecordBatch header 61 bytes

Kafka Record header 14 bytes (variable-length integer encoding)

─────────────────────────────────────────

Usable application payload ~1373 bytes ≈ 8.5% header overhead
```

With jumbo frames (MTU = 9000 bytes), the fixed overhead of 52 bytes yields a ratio of 52 / 9000 ≈ 0.6%, a 14-fold reduction. This improvement is realised only when the MTU is consistent end-to-end; a single intermediate hop at standard MTU will trigger fragmentation or Path MTU Discovery (PMTUD) renegotiation \[12\].

## 4.2  Tunnel encapsulation overhead

Tunnel protocols impose additional fixed overhead that reduces the effective MSS available to inner TCP connections. Table 3 summarises common cases.

**Table 3.** *Effective MSS under common tunnel protocols (outer MTU = 1500 bytes).*

|                       |                      |                   |                                             |
|-----------------------|----------------------|-------------------|---------------------------------------------|
| **Encapsulation**     | **Overhead (bytes)** | **Effective MSS** | **Risk if unmitigated**                     |
| None (baseline)       | 0                    | 1460              | —                                           |
| WireGuard (IPv4/UDP)  | ~60                  | ~1400             | Fragmentation if MSS not clamped            |
| VXLAN                 | 50                   | 1410              | Common in Kubernetes overlay networks       |
| IPsec ESP (transport) | ~60                  | ~1400             | Variable with cipher block size and padding |


> **PMTUD black holes**
>
> ICMP type 3 code 4 ("Fragmentation Needed") messages are required for PMTUD [12]. If these are filtered by an intermediate firewall, the sender never learns to reduce MSS. The resulting failure mode — the PMTUD black hole — presents as successful TCP handshake (small packets traverse the path) followed by stalled bulk transfers (large segments are silently dropped). The recommended mitigation is an iptables MSS clamp on tunnel ingress: --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu.


## 4.3  IP Fragmentation and Path MTU Discovery

When a TCP segment exceeds the MTU of any intermediate link, one of three outcomes occurs:

1. **Fragmentation** — If the IP Don't Fragment (DF) bit is not set, the router fragments the packet. TCP sets DF by default (RFC 1191 §5), so this is uncommon on modern paths.

2. **ICMP Fragmentation Needed** — The router sends ICMP type 3 code 4 ("Fragmentation Needed and DF Set") with the next-hop MTU. The sender reduces MSS accordingly.

3. **Silent drop** — If ICMP is filtered (the PMTUD black hole), the packet is discarded with no notification. The connection stalls after the handshake.

### Path MTU Discovery (PMTUD)

RFC 1191 [12] defines PMTUD: the sender begins with the interface MTU and iteratively reduces MSS in response to ICMP type 3/4 messages until segments traverse the path. Modern Linux kernels enable PMTUD by default (`net.ipv4.ip_no_pmtu_disc = 0`).

**PMTUD failure modes:**

| Symptom | Cause | Mitigation |
|---------|-------|------------|
| Handshake succeeds, bulk transfer stalls | ICMP type 3/4 blocked by firewall | iptables MSS clamp on tunnel ingress |
| Throughput < 50% expected, retransmit rate > 5% | Fragmentation occurring despite DF bit | Reduce interface MTU or enable MSS clamp |
| Connection reset after SYN-ACK | MSS advertisement exceeds path capacity | Verify `net.ipv4.tcp_mtu_probing = 1` |

### Cloud Jumbo Frame Considerations

Cloud providers support jumbo frames (MTU > 1500) on intra-VPC paths but revert to MTU 1500 at internet egress. Table 4 summarises MTU limits by provider.

**Table 4.** *Cloud provider MTU limits (as of 2025).*

| Provider | Intra-VPC MTU | Internet Egress MTU | Notes |
|----------|---------------|---------------------|-------|
| AWS      | 9001          | 1500                | Enhanced Networking required; check instance type support |
| GCP      | 8896          | 1460                | VPC default; external IP reduces to 1460 due to GRE encapsulation |
| Azure    | 9000          | 1400                | VNet default; internet uses 1400 due to VXLAN overhead |

**Formula F20: Effective MSS in cloud hybrid paths**

For workloads spanning VPC and internet:

```
MSS_eff = min(MTU_vpc, MTU_internet) − 40
```

If Kafka producers are in-VPC (MTU 9001) but consumers are internet-hosted (MTU 1500), the effective MSS is **1460 bytes**. Jumbo frame benefits apply only to intra-VPC replication traffic.

### Diagnostic Procedure

```bash
# 1. Detect interface MTU
ip link show eth0 | grep mtu

# 2. Discover path MTU (requires ICMP echo permission)
tracepath broker.example.com

# 3. Verify negotiated MSS on active connection
ss -tin dst broker.example.com | grep mss

# 4. Test PMTUD with forced DF bit
ping -M do -s 8972 broker.example.com  # 9000 MTU - 28-byte ICMP header
```

If `tracepath` reports a smaller MTU than the interface, an intermediate router is fragmenting. If `ss` shows `mss:1400` despite interface MTU 9000, MSS clamping is active (common with VPN/tunnel interfaces).

### MSS Clamping

When PMTUD is unreliable (ICMP filtered), MSS clamping forces the advertised MSS to a safe value. On Linux:

```bash
# Clamp MSS to PMTU on tunnel interface
iptables -t mangle -A FORWARD -p tcp --tcp-flags SYN,RST SYN \
    -o tun0 -j TCPMSS --clamp-mss-to-pmtu

# Clamp to fixed value (e.g., 1400 for WireGuard)
iptables -t mangle -A FORWARD -p tcp --tcp-flags SYN,RST SYN \
    -o wg0 -j TCPMSS --set-mss 1400
```

The clamp modifies the MSS option in the TCP SYN packet during the handshake. This is transparent to the application and more reliable than relying on ICMP delivery.


# 5.  Measurement Methodology

The following procedure derives empirical values for BDP, achievable bandwidth, and effective window from active measurements, providing inputs for the calculations in Section 6. Measurements should be performed under representative load conditions and repeated before and after each configuration change.

## 5.1  Prerequisites

- iperf3 server running on each broker host: iperf3 -s -D -p 5201

- ICMP echo and TCP port 5201 permitted between producer and broker hosts

- Measurements performed from the producer host against each broker independently

- At least 200 RTT samples; at least 15 seconds per iperf3 interval

## 5.2  Phase 1: RTT characterisation

The minimum RTT (RTT<sup>min</sup>) is the propagation delay in the absence of queuing and is used as the RTT value in F2. The mean and standard deviation (mdev) characterise operational conditions and jitter respectively.

```
ping -c 200 -i 0.05 <broker-ip>
```

**Table 4.** *Interpretation of ping statistics.*

|               |                        |                                                                                   |
|---------------|------------------------|-----------------------------------------------------------------------------------|
| **Statistic** | **Use**                | **Diagnostic interpretation**                                                     |
| min           | BDP calculation (F2)   | Propagation delay only; use this value exclusively for buffer sizing              |
| avg           | Linger.ms sizing (F12) | Includes typical queue occupancy at idle; use for latency budget calculations     |
| mdev          | Jitter diagnostic      | mdev > 0.3 × avg indicates persistent switch buffer inflation; enable fq         |
| loss%         | Mathis bound (F4)      | Loss > 0.01% materially limits throughput via F4; remediate before buffer tuning |

## 5.3  Phase 2: Window sweep

A single-stream iperf3 test is repeated across a logarithmic range of window sizes. The window size at which throughput ceases to increase identifies the empirical BDP: increasing the window further yields no additional throughput, confirming the pipe is full.

```bash
for WIN in 4 8 16 32 64 128 256 512 1024 2048 4096; do

iperf3 -c <broker-ip> -w \${WIN}K -t 15 -J > win\_\${WIN}k.json

sleep 2

done
```

The plateau is defined as the smallest window *W\** for which the throughput gain from doubling the window falls below 5%. If the empirical BDP significantly exceeds the theoretical value from F2, intermediate switch buffers are inflating the operational RTT; this discrepancy is itself a diagnostic finding.

## 5.4  Phase 3: Parallel stream characterisation

Kafka producers establish multiple concurrent TCP connections per broker. A single TCP flow may be window-limited before the physical link is saturated. The following test sweeps the number of parallel streams at the plateau window:

```bash
for STREAMS in 1 2 4 8; do

iperf3 -c <broker-ip> -P \$STREAMS -w <W\*\_bytes> -t 15

sleep 2

done
```

If aggregate throughput with *n* streams substantially exceeds that of a single stream at the same window, the connection count (and by extension max.in.flight.requests.per.connection) is a material tuning lever in addition to buffer size.

## 5.5  Phase 4: Nagle algorithm effect

Nagle's algorithm \[13\] coalesces small writes into full MSS segments, introducing up to one RTT of additional latency. Apache Kafka sets TCP_NODELAY by default since version 2.1. The following test quantifies the Nagle contribution on the test path:

```bash
iperf3 -c <broker-ip> -w <W\*\_bytes> -t 15 \# Nagle enabled

iperf3 -c <broker-ip> -w <W\*\_bytes> -t 15 --no-delay \# Nagle disabled
```

## 5.6  In-flight verification

During an active transfer, the kernel's per-connection TCP state can be inspected to confirm that the measured window and congestion state match expectations:

```
ss -tin dst <broker-ip> \| grep -E 'rtt\|cwnd\|mss\|rcv_space\|pacing_rate'
```

The product cwnd × mss should approach the empirical BDP. The pacing_rate field confirms that BBR pacing via fq is operational; its absence indicates that fq is not installed.

# 6.  Derivation of Configuration Parameters

## 6.1  TCP socket buffer ceiling

The kernel socket buffer ceiling must be sufficient to accommodate the BDP across all concurrent connections, with a safety margin. The empirical BDP from Phase 2 is preferred over the theoretical value from F2, as it accounts for any path buffering not reflected in the ping minimum. A headroom factor of 2 is applied:

```
buf_ceil = BDP_empirical × N_conns × 2 (derived from F2)
```

This value is rounded up to the nearest power of two for alignment with the kernel's autotuning boundaries, and applied to both send and receive buffers:

```bash
net.core.rmem_max = net.core.wmem_max = buf_ceil

net.ipv4.tcp_rmem = 4096 1048576 buf_ceil

net.ipv4.tcp_wmem = 4096 1048576 buf_ceil
```

## 6.2  Kafka batch.size

From F10, the Kafka producer's effective TCP window equals batch.size × max.in.flight.requests.per.connection. For this to equal or exceed the BDP, the minimum batch.size is:

```
batch_min = BDP_empirical / max.in.flight (derived from F10)
```

In practice, batch.size is rounded up to the nearest standard value (16, 32, 64, 128, 256, 512, or 1024 KiB). Larger batches are preferable where latency permits, as lz4 compression ratio improves with batch context size.

## 6.3  Kafka linger.ms

Two distinct targets for linger.ms are derived depending on the optimisation objective:

- **Throughput profile (F11):** linger.ms is set to the time required to drain one batch at the measured bandwidth, ensuring the next batch is ready before the sender window empties:

```properties
linger_t = (batch.size × 8) / B × 1000 \[ms\] (F11)
```

- **Latency profile (F12):** linger.ms is set to the remaining budget after subtracting network and broker overhead from the end-to-end latency SLA:

```
linger_l = SLA_ms − RTT_avg_ms − t_broker_ms (F12)
```


> **Throughput-latency trade-off**
>
> When linger_t &gt; linger_l, the throughput and latency objectives are mutually incompatible for the given path. The constraint is fundamental: the pipe cannot be kept full while also meeting the latency SLA. Compression (lz4 or zstd) partially resolves this by reducing effective bytes-on-wire, allowing a larger batch to be transmitted within the same latency window. This interaction should be quantified with kafka-producer-perf-test.sh under representative load.


## 6.4  Consumer fetch parameters (F13, F14)

Consumer fetch settings follow the same principles as producer batch settings but operate on the receive path:

```
max.partition.fetch.bytes ≥ batch.size (F13)
```

This ensures the consumer can receive a full producer batch without fragmenting it across multiple fetch requests. Fragmentation increases broker CPU overhead and fetch request count.

```
fetch.min.bytes = batch.size / 2 (heuristic)
```

The broker waits to accumulate at least fetch.min.bytes before responding, improving batching efficiency. Setting this to half the producer batch size balances throughput and latency.

```
fetch.max.wait.ms = linger_t (throughput) or 100–500 ms (latency) (F14)
```

This is the maximum time the broker waits to accumulate fetch.min.bytes. It is symmetric to the producer's linger.ms: both control batch accumulation time at their respective ends of the pipeline.

## 6.5  Broker replication parameters (F15, F16, F17)

**Replica fetch size (F17):**

```
replica.fetch.max.bytes ≥ batch.size
```

Followers fetch from leaders using the same fetch protocol as consumers. Setting replica.fetch.max.bytes ≥ batch.size ensures replicas can fetch complete producer batches in one request, reducing fetch overhead.

**Fetcher parallelism (F16):**

```
num.replica.fetchers = ceil(partitions / 6)
```

Each fetcher thread handles approximately 6 partitions sequentially. For 12 partitions: 2 fetchers. For 64 partitions: 11 fetchers. Monitor UnderReplicatedPartitions metric to verify fetcher count is sufficient.

**Replica lag timeout (F15):**

```
replica.lag.time.max.ms = RTT × 4 + fetch.max.wait.ms + 5000
```

This accounts for: fetch request (RTT/2), broker wait (fetch.max.wait.ms), response (RTT/2), processing, plus a 5000 ms safety margin for GC pauses and network variance. The factor of 4 on RTT provides headroom (2 RTTs × 2 safety factor).

Example: RTT = 60 ms, fetch.max.wait.ms = 50 ms:

```
replica.lag.time.max.ms = 60×4 + 50 + 5000 = 5290 ms → round to 6000 ms
```

## 6.6  Bottleneck classification

Table 5 provides a systematic diagnostic for identifying the active bottleneck from measurement observations.

**Table 5.** *Bottleneck classification from measurement observations.*

|                                                      |                     |                                                                       |
|------------------------------------------------------|---------------------|-----------------------------------------------------------------------|
| **Observation**                                      | **Classification**  | **Recommended remediation**                                           |
| High retransmit rate, low throughput                 | Loss / congestion   | Enable BBR; verify MTU end-to-end; inspect switch buffer depth        |
| Low throughput, zero retransmits                     | Window-limited      | Increase rmem/wmem ceiling per Section 6.1                            |
| CPU utilisation near 100% (sender or receiver)       | CPU-bound           | Configure IRQ affinity; disable TCP timestamps; increase thread count |
| Multi-stream throughput >> single-stream           | Single-flow limited | Increase max.in.flight or producer parallelism                        |
| High RTT variance (mdev >> RTT_min)                | Buffer bloat        | Enable fq qdisc; review switch QoS and buffer allocation              |
| Control-plane traffic succeeds; bulk transfers stall | PMTUD black hole    | Verify ICMP type 3/4 forwarding; apply MSS clamp on tunnel            |

# 7.  Configuration Reference

## 7.1  Prioritised tuning sequence

The following sequence is recommended. Each step should be applied in isolation and benchmarked before proceeding to the next. Steps 1–5 require no network infrastructure changes and together typically account for the majority of achievable gain.

1.  **lz4 compression** — a single producer configuration change. Reduces bytes-on-wire by 2–4× for structured payloads; compresses the entire batch, so larger batches yield higher ratios. No infrastructure dependency.

2.  **Kafka batch.size and linger.ms** — aligned to BDP / max.in.flight (Section 6.2). Ensures the effective Kafka window meets or exceeds the path BDP. Compounds with compression.

3.  **TCP socket buffers** — rmem/wmem ceiling set per Section 6.1. Takes effect for new connections immediately; no service restart is required.

4.  **BBR + fq** — two kernel parameters. Eliminates queuing-induced latency; accelerates post-restart connection convergence. Mandatory pairing.

5.  **TCP_NODELAY** — verify Nagle is disabled on all Kafka sockets (default since Kafka 2.1). Confirm with ss -tinp \| grep nonagle.

6.  **Verify path MTU** — Use `tracepath <broker-ip>` to confirm end-to-end MTU before enabling jumbo frames. If path MTU < interface MTU, apply MSS clamping (Section 4.3) or reduce interface MTU to match path. Cloud-specific considerations:
    - **AWS:** Verify Enhanced Networking enabled; test with instance type that supports MTU 9001
    - **GCP:** VPC default is 8896; no additional config needed for intra-VPC paths
    - **Azure:** VNet default is 9000; verify Accelerated Networking enabled for full throughput

7.  **Jumbo frames (Kafka network segment only)** — requires coordinated switch port configuration; must be restricted to the Kafka network segment to avoid disrupting hosts on shared segments expecting standard MTU. Reduces header overhead from 2.7% to 0.4% per byte delivered. Internet-bound traffic will be clamped to MTU 1500 at egress (see Table 4).

8.  **Disable TCP timestamps** — saves 10 bytes per segment (0.7% on 1500-byte frames, 0.1% on 9000-byte frames). Justified only if CPU is the confirmed bottleneck; incurs loss of PAWS and RTTM precision.

## 7.2  Linux kernel parameters

The following sysctl template is generated by kafka-tcp-analyze.sh from measurement results. Substitute `<buf_ceil>` with the computed value from Section 6.1.

```bash
# /etc/sysctl.d/99-kafka-tcp.conf

# Buffer ceilings — BDP_empirical × N_conns × 2, rounded to power of 2

net.core.rmem_max = <buf_ceil>

net.core.wmem_max = <buf_ceil>

net.ipv4.tcp_rmem = 4096 1048576 <buf_ceil>

net.ipv4.tcp_wmem = 4096 1048576 <buf_ceil>

net.ipv4.tcp_moderate_rcvbuf = 1

# Congestion control — mandatory pairing

net.ipv4.tcp_congestion_control = bbr

net.core.default_qdisc = fq

# Keep-alive — dead broker detection within 30 s

net.ipv4.tcp_keepalive_time = 30

net.ipv4.tcp_keepalive_intvl = 5

net.ipv4.tcp_keepalive_probes = 3

# Path MTU Discovery — required for jumbo frame paths
net.ipv4.ip_no_pmtu_disc = 0              # Enable PMTUD (default)
net.ipv4.tcp_mtu_probing = 1              # Probe MTU on connection timeout (fallback if ICMP filtered)
net.ipv4.tcp_base_mss = 1024              # Minimum MSS to try during probing
```

## 7.3  Kafka producer parameters

Table 6 summarises key Kafka producer parameters, their default values, and recommended values derived from the measurement-based calculation procedure.

**Table 6.** *Kafka producer parameter recommendations.*

|                  |             |                 |                                                           |
|------------------|-------------|-----------------|-----------------------------------------------------------|
| **Parameter**    | **Default** | **Recommended** | **Derivation**                                            |
| batch.size       | 16384       | ≥ BDP/inflight  | Section 6.2; larger improves compression ratio            |
| linger.ms        | 0           | 5–100 ms        | F11 (throughput) or F12 (latency)                         |
| compression.type | none        | lz4             | 2–4× ratio; zstd for latency-tolerant high-RTT paths      |
| max.in.flight    | 5           | 5               | F10: effective window = batch.size × this value           |
| acks             | 1           | all             | Requires min.insync.replicas ≥ 2 for durability guarantee |
| buffer.memory    | 33554432    | 2 × buf_ceil    | Total producer buffer; must cover all in-flight batches   |

## 7.4  Kafka consumer parameters

Consumer fetch settings control the receive-side tradeoff between throughput (batching) and latency. The semantics are symmetric to producer linger.ms but operate on the broker → consumer path.

**Table 6b.** *Kafka consumer parameter recommendations.*

|                            |             |                 |                                                                                   |
|----------------------------|-------------|-----------------|-----------------------------------------------------------------------------------|
| **Parameter**              | **Default** | **Recommended** | **Derivation**                                                                    |
| max.partition.fetch.bytes  | 1048576     | ≥ batch.size    | F13: must accommodate full producer batches to avoid fragmentation                |
| fetch.min.bytes            | 1           | batch.size / 2  | Minimum data before broker responds; higher = more batching, higher latency      |
| fetch.max.wait.ms          | 500         | 5–100 ms        | F14: broker wait time to accumulate fetch.min.bytes; symmetric to linger.ms      |
| receive.buffer.bytes       | 65536       | ≥ BDP           | TCP receive buffer; same logic as producer send.buffer.bytes                      |
| max.poll.records           | 500         | tunable         | Records returned per poll(); higher = more batching but longer processing pauses |

### Throughput vs latency profiles

- **Throughput profile:** fetch.min.bytes = batch.size / 2, fetch.max.wait.ms = linger_t (F11). The broker accumulates data for up to linger_t milliseconds before responding, improving batching and CPU efficiency. Appropriate when end-to-end latency budget exceeds linger_t + RTT + 10 ms.

- **Latency profile:** fetch.min.bytes = 1, fetch.max.wait.ms = 100–500 ms. The broker responds immediately when any data is available (minimum 1 byte), minimising wait time. Use when latency SLA is constrained.

The parameter fetch.max.wait.ms is the receive-side analogue of the producer's linger.ms. Both control batch accumulation time; the producer accumulates outbound records into a batch, while the broker accumulates inbound fetch requests. The values should be aligned: a producer with linger.ms = 10 ms paired with a consumer fetch.max.wait.ms = 500 ms introduces unnecessary 500 ms latency on the consumer side even though the producer is configured for low latency.

## 7.5  Kafka broker replication parameters

Replication introduces additional fetch paths: each follower replica fetches from its partition leader. At replication factor *RF*, a topic with *N* partitions has *N* × (*RF* − 1) follower → leader fetch connections.

**Table 6c.** *Kafka broker replication parameter recommendations.*

|                                   |             |                          |                                                                                             |
|-----------------------------------|-------------|--------------------------|---------------------------------------------------------------------------------------------|
| **Parameter**                     | **Default** | **Recommended**          | **Derivation**                                                                              |
| replica.fetch.max.bytes           | 1048576     | ≥ batch.size             | F17: followers should fetch complete producer batches in one request                        |
| replica.socket.receive.buffer.bytes | -1 (OS)   | ≥ BDP                    | Follower receive buffer when fetching from leader; same logic as consumer receive buffer   |
| num.replica.fetchers              | 1           | ceil(partitions / 6)     | F16: one fetcher thread per ~6 partitions; more fetchers = better parallelism              |
| replica.lag.time.max.ms           | 10000       | RTT×4 + wait + 5000      | F15: timeout before replica marked out-of-sync; see Section 7.5.1                          |
| min.insync.replicas               | 1           | 2                        | Minimum replicas that must acknowledge a write when acks=all; ensures durability           |

### 7.5.1  Replica lag timeout derivation (F15)

A follower replica is considered in-sync if it has fetched up to the leader's high-water mark within replica.lag.time.max.ms. The timeout must cover the worst-case fetch cycle:

1. **Fetch request transmission** (RTT / 2): follower → leader
2. **Broker processing + wait time**: leader accumulates data or waits up to fetch.max.wait.ms (internal broker fetch interval, typically matches consumer fetch.max.wait.ms)
3. **Fetch response transmission** (RTT / 2): leader → follower
4. **Follower processing**: append to local log, update offsets
5. **Safety margin**: GC pauses, network variance, disk I/O spikes

**Formula:**

```
replica.lag.time.max.ms = RTT × 4 + fetch.max.wait.ms + 5000 ms
```

The factor of 4 on RTT accounts for two round-trips with headroom (nominal 2 RTTs × safety factor 2). The 5000 ms margin accommodates transient pauses. On high-RTT paths (e.g., cross-region with RTT = 60 ms), this yields:

```
replica.lag.time.max.ms = 60×4 + 50 + 5000 = 5290 ms
```

Rounding up to 6000–10000 ms is typical. Setting this too low causes false positives (replicas marked out-of-sync during transient slowdowns); too high delays detection of genuinely failed replicas.

### 7.5.2  Replication bandwidth budget

At replication factor *RF* and *N* partitions, the total number of follower fetch connections is:

```
follower_connections = N × (RF − 1)
```

Each follower fetches at approximately the same rate as the producer writes to the leader. For *RF* = 3 and 12 partitions:

```
follower_connections = 12 × 2 = 24 connections
```

If the producer sustains 500 Mbit/s to leaders, followers collectively fetch 500 Mbit/s × 2 = 1 Gbit/s. This must fit within the broker's network capacity alongside producer and consumer traffic. A rule of thumb: budget 50% of link bandwidth for replication, 50% for producers + consumers.

### 7.5.3  Fetcher thread parallelism (F16)

The parameter num.replica.fetchers controls how many threads concurrently fetch replica data. Each thread handles a subset of partitions sequentially. Increasing fetcher count improves parallelism but consumes more broker threads and memory.

**Heuristic:**

```
num.replica.fetchers = ceil(partitions / 6)
```

For 12 partitions: ceil(12 / 6) = 2 fetchers. For 100 partitions: ceil(100 / 6) ≈ 17 fetchers. Monitor broker CPU and replica lag metrics (UnderReplicatedPartitions, ReplicaFetcherMaxLag) to tune.

### 7.5.4  Per-broker bandwidth model (F18, F19)

**CRITICAL:** In cloud environments (AWS, GCP, Azure), bandwidth constraints are applied **per-VM** (per broker), not per-cluster. Each broker VM has a network interface bandwidth limit (e.g., AWS EC2 m5.4xlarge = 10 Gbps). This single NIC handles ALL traffic:

- Producer writes (ingress to leader partitions)
- Consumer reads (egress from leader partitions)
- Replication OUT (leader → followers)
- Replication IN (follower ← other leaders)

**Replication write amplification (F18):**

At replication factor *RF*, each producer write is replicated (*RF* − 1) times. A broker hosting a leader partition experiences *RF*× write amplification on its NIC:

```
write_amplification = 1 + (RF − 1) = RF
```

Example: 1 MB producer write at RF=3 → 1 MB ingress + 2 MB replication egress = 3 MB total broker NIC utilization.

**Per-broker bandwidth budget (F19):**

With *B* brokers, *N* partitions evenly distributed, and replication factor *RF*:

```
partitions_per_broker = N / B
leader_partitions_per_broker ≈ N / B (assuming even distribution)
follower_partitions_per_broker ≈ N − (N / B) = N(B−1)/B
```

Per-broker bandwidth breakdown:

```
producer_ingress   = throughput × (leader_partitions / N)
replication_OUT    = producer_ingress × (RF − 1)     [F18 amplification]
replication_IN     = throughput × (follower_partitions / N)
consumer_egress    = producer_ingress                 [steady-state: reads match writes]

total_per_broker   = producer_ingress + replication_OUT + replication_IN + consumer_egress
```

**Constraint (F19):**

```
total_per_broker ≤ NIC_bandwidth_limit
```

If violated, the broker NIC saturates, throttling all traffic (producers, consumers, AND replication).

**Example:** 3 brokers, 12 partitions, RF=3, producer writes 300 Mbps aggregate:

```
partitions_per_broker = 12 / 3 = 4
producer_ingress = 300 Mbps × (4 / 12) = 100 Mbps
replication_OUT  = 100 Mbps × 2 = 200 Mbps
replication_IN   = 300 Mbps × (8 / 12) = 200 Mbps
consumer_egress  = 100 Mbps

total_per_broker = 100 + 200 + 200 + 100 = 600 Mbps
```

At NIC limit = 1000 Mbps (1 Gbps), utilization = 60%. Increasing RF to 4 or partitions to 16 would push toward saturation.

**Scaling implications:**

- **More partitions with fixed brokers:** does NOT increase per-partition throughput if per-broker NIC is the bottleneck. It divides the same NIC capacity across more partitions.
- **More brokers:** distributes load, reduces partitions/broker, lowers per-broker NIC utilization. This is how you scale past NIC limits.
- **Higher replication factor:** increases per-broker NIC utilization by (*RF* − 1) / *RF_old* − 1. Going RF=2 → RF=3 adds 50% replication bandwidth.

# 8.  Complete Configuration Example

This section presents a worked example deriving all configuration parameters from measurement data for a cross-availability-zone deployment.

## 8.1  Input measurements

- **Per-broker bandwidth limit:** 1000 Mbps (125 MB/s) — cloud VM NIC limit
- **Brokers in cluster:** 3
- **RTT minimum:** 8 ms (ping min)
- **RTT average:** 12 ms (ping avg)
- **Packet loss:** 0% (no retransmits observed)
- **MTU:** 1500 bytes
- **Concurrent producer connections:** 4 (producer parallelism)
- **Partitions:** 12 (topic configuration, distributed across 3 brokers)
- **Replication factor:** 3 (durability requirement)
- **Latency budget:** 50 ms (end-to-end SLA)

## 8.2  Derived network parameters

**MSS (F8):**
```
MSS = MTU - 40 = 1500 - 40 = 1460 bytes
```

**Theoretical BDP (F2):**
```
BDP = bandwidth × RTT_min
    = (1000 Mbit/s × 10^6 / 8) × (8 ms / 1000)
    = 125,000,000 bytes/s × 0.008 s
    = 1,000,000 bytes = 976.6 KB
```

**Empirical BDP:** Plateau observed at 1024 KB window in iperf3 sweep (use this value for calculations).

**Buffer ceiling:**
```
buf_ceil = BDP_empirical × N_conns × 2
         = 1,048,576 × 4 × 2
         = 8,388,608 bytes
         → round to next power of 2 = 8,388,608 (already power of 2)
         = 8 MB
```

## 8.3  Producer configuration

**batch.size minimum (F10):**
```
batch_min = BDP / max.in.flight
          = 1,048,576 / 5
          = 209,715 bytes
          → round to nearest standard: 256 KB
```

**linger.ms for throughput (F11):**
```
linger_t = (batch.size × 8) / bandwidth × 1000
         = (262,144 × 8) / (1000 × 10^6) × 1000
         = 2,097,152 / 1,000,000,000 × 1000
         = 2.1 ms
         → round to 5 ms for stability
```

**linger.ms for latency (F12):**
```
linger_l = latency_budget - RTT_avg - broker_overhead
         = 50 - 12 - 2
         = 36 ms
```

## 8.4  Consumer configuration

**max.partition.fetch.bytes (F13):**
```
max.partition.fetch.bytes ≥ batch.size
                          ≥ 262,144 bytes
                          → set to 1 MB (1,048,576) for headroom
```

**fetch.min.bytes:**
```
fetch.min.bytes = batch.size / 2
                = 262,144 / 2
                = 131,072 bytes
```

**fetch.max.wait.ms (F14):**
```
Throughput profile: match linger_t = 5 ms
Latency profile:    100–500 ms (minimize for low latency)
```

**receive.buffer.bytes:**
```
receive.buffer.bytes ≥ BDP
                     ≥ 1,048,576 bytes
                     → set to buf_ceil = 8 MB
```

## 8.5  Broker replication configuration

**Total follower connections:**
```
follower_connections = partitions × (RF - 1)
                     = 12 × (3 - 1)
                     = 24 connections
```

**replica.fetch.max.bytes (F17):**
```
replica.fetch.max.bytes ≥ batch.size
                        ≥ 262,144 bytes
                        → set to 1 MB
```

**num.replica.fetchers (F16):**
```
num.replica.fetchers = ceil(partitions / 6)
                     = ceil(12 / 6)
                     = 2 fetcher threads
```

**replica.lag.time.max.ms (F15):**
```
replica.lag.time.max.ms = RTT×4 + fetch.max.wait + margin
                        = 12×4 + 5 + 5000
                        = 48 + 5 + 5000
                        = 5053 ms
                        → round to 10,000 ms (default is reasonable)
```

**replica.socket.receive.buffer.bytes:**
```
replica.socket.receive.buffer.bytes ≥ BDP
                                    ≥ 1,048,576 bytes
                                    → set to buf_ceil = 8 MB
```

**Per-broker bandwidth breakdown (F18, F19):**

With 3 brokers, 12 partitions evenly distributed:
```
partitions_per_broker = 12 / 3 = 4 (each broker is leader for 4 partitions)
leader_partitions_per_broker = 4
follower_partitions_per_broker = 8 (follower for 8 partitions on other brokers)
```

Assume aggregate producer throughput = 300 Mbps (feasible at window = batch.size × inflight = 256KB × 5):

```
per_broker_producer_ingress = 300 Mbps × (4 / 12) = 100 Mbps
per_broker_replication_OUT  = 100 Mbps × (RF - 1) = 100 × 2 = 200 Mbps  [F18]
per_broker_replication_IN   = 300 Mbps × (8 / 12) = 200 Mbps
per_broker_consumer_egress  = 100 Mbps (reads match writes in steady state)

total_per_broker = 100 + 200 + 200 + 100 = 600 Mbps
utilization = 600 / 1000 = 60%  [F19 constraint satisfied]
```

At RF=3, each broker handles 3× its leader partition throughput (1× ingress + 2× replication OUT). The replication amplification (F18) is the dominant bandwidth consumer.

## 8.6  Summary configuration files

**sysctl (99-kafka-tcp.conf):**
```
net.core.rmem_max = 8388608
net.core.wmem_max = 8388608
net.ipv4.tcp_rmem = 4096 1048576 8388608
net.ipv4.tcp_wmem = 4096 1048576 8388608
net.ipv4.tcp_moderate_rcvbuf = 1
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc = fq
net.ipv4.tcp_keepalive_time = 30
net.ipv4.tcp_keepalive_intvl = 5
net.ipv4.tcp_keepalive_probes = 3
```

**Producer (producer.properties):**
```
batch.size = 262144
linger.ms = 5
compression.type = lz4
max.in.flight.requests.per.connection = 5
acks = all
enable.idempotence = true
buffer.memory = 16777216
send.buffer.bytes = 524288
receive.buffer.bytes = 65536
```

**Consumer (consumer.properties):**
```
max.partition.fetch.bytes = 1048576
fetch.min.bytes = 131072
fetch.max.wait.ms = 5
receive.buffer.bytes = 8388608
```

**Broker (server.properties):**
```
socket.send.buffer.bytes = 8388608
socket.receive.buffer.bytes = 8388608
socket.request.max.bytes = 104857600
num.network.threads = 8
num.io.threads = 8
replica.fetch.max.bytes = 1048576
replica.socket.receive.buffer.bytes = 8388608
num.replica.fetchers = 2
replica.lag.time.max.ms = 10000
min.insync.replicas = 2
```

# 9.  Deployment Scenario Reference

Table 7 presents recommended parameter values for seven representative deployment scenarios. Values assume lz4 compression and acks=all. The "Kafka defaults" row reflects out-of-the-box configuration and is included for reference.

**Table 7.** *Recommended parameter values by deployment scenario.*

|                            |          |         |          |                |              |            |        |
|----------------------------|----------|---------|----------|----------------|--------------|------------|--------|
| **Scenario**               | **B**    | **RTT** | **BDP**  | **batch.size** | **rmem_max** | **linger** | **CC** |
| **Kafka defaults**         | —        | —       | —        | 16 KB          | 256 KB       | 0          | cubic  |
| **Intra-datacenter**       | 10 Gbps  | 0.2 ms  | ~250 KB  | 128 KB         | 32 MB        | 5 ms       | bbr    |
| **Same availability zone** | 1 Gbps   | 5 ms    | ~625 KB  | 128 KB         | 128 MB       | 10 ms      | bbr    |
| **Cross-AZ**               | 1 Gbps   | 20 ms   | ~2.5 MB  | 256 KB         | 256 MB       | 20 ms      | bbr    |
| **Cross-region**           | 500 Mbps | 60 ms   | ~3.75 MB | 512 KB         | 512 MB       | 50 ms      | bbr    |
| **Multi-region**           | 200 Mbps | 150 ms  | ~3.75 MB | 1 MB           | 1 GB         | 100 ms     | bbr    |
| **Satellite link**         | 50 Mbps  | 600 ms  | ~3.75 MB | 1 MB           | 2 GB         | 500 ms     | bbr    |


> **Jumbo frames**
>
> The intra-datacenter row assumes MTU = 1500. Enabling jumbo frames (MTU = 9000) on the Kafka network segment reduces header overhead from 3.5% to 0.6% and permits proportionally smaller batch sizes for the same effective window. The change must be restricted to network segments on which all attached hosts support MTU = 9000; hosts on shared segments operating at standard MTU are unaffected provided the configuration is applied at the VLAN or port-profile level.


# 10.  Measurement Automation

Two shell scripts automate the measurement and analysis procedures described in Section 5. Both scripts produce machine-readable output and require no external dependencies beyond iperf3, ping, bc, and python3.

## 10.1  kafka-tcp-measure.sh

Executes all four measurement phases from the producer host and writes structured CSV files and a meta.env file for consumption by the analysis script.

```bash
iperf3 -s -D -p 5201 \# on broker host

./kafka-tcp-measure.sh -t <broker-ip> -s 8 \\

-m 1500 -o ./results
```

**Table 8.** *Measurement phases and outputs of kafka-tcp-measure.sh.*

|         |                        |                   |                                                                       |
|---------|------------------------|-------------------|-----------------------------------------------------------------------|
| **Ph.** | **Output file**        | **Tool**          | **Quantity measured**                                                 |
| 1       | ping.csv               | ping -c 200       | RTT_min, RTT_avg, mdev, packet loss percentage                        |
| 2       | window_sweep.csv       | iperf3 -w         | Throughput vs. window size (4 KB–4 MB); empirical BDP plateau         |
| 3       | parallel_sweep.csv     | iperf3 -P         | Aggregate throughput at 1, 2, 4, 8 parallel streams at plateau window |
| 4       | nodelay_comparison.csv | iperf3 --no-delay | Throughput delta with and without TCP_NODELAY                         |

## 10.2  kafka-tcp-analyze.sh

Reads the output of kafka-tcp-measure.sh, applies the calculations from Section 6, classifies the bottleneck per Table 5, and writes four ready-to-apply configuration files.

```bash
./kafka-tcp-analyze.sh -d ./results \\

-c 8 \\ \# concurrent Kafka connections

-m 1500 \\ \# path MTU

-l 50 \# latency SLA in ms
```

**Table 9.** *Output files of kafka-tcp-analyze.sh.*

|                                |                                                                               |
|--------------------------------|-------------------------------------------------------------------------------|
| **Output file**                | **Application**                                                               |
| 99-kafka-tcp.conf              | Linux kernel parameters; apply with: sudo sysctl -p results/99-kafka-tcp.conf |
| producer-throughput.properties | Kafka producer configuration, throughput profile; add to producer.properties  |
| producer-latency.properties    | Kafka producer configuration, latency profile; add to producer.properties     |
| broker-additions.properties    | Kafka broker additions; add to server.properties and restart broker           |

# 11.  References

**\[1\]** Little, J.D.C. (1961). A proof for the queuing formula: L = λW. Operations Research, 9(3), 383–387.

**\[2\]** Jacobson, V., Braden, R., & Borman, D. (1992). TCP Extensions for High Performance. RFC 1323, IETF. (Superseded by RFC 7323, 2014.)

**\[3\]** Mathis, M., Semke, J., Mahdavi, J., & Ott, T. (1997). The macroscopic behavior of the TCP congestion avoidance algorithm. ACM SIGCOMM Computer Communication Review, 27(3), 67–82.

**\[4\]** Cardwell, N., Cheng, Y., Gunn, C.S., Yeganeh, S.H., & Jacobson, V. (2016). BBR: Congestion-based congestion control. ACM Queue, 14(5), 20–53.

**\[5\]** Postel, J. (1983). TCP Maximum Segment Size and Related Topics. RFC 879, IETF.

**\[6\]** Padhye, J., Firoiu, V., Towsley, D., & Kurose, J. (1998). Modeling TCP throughput: a simple model and its empirical validation. Proceedings of ACM SIGCOMM 1998, 303–314.

**\[7\]** Jacobson, V. (1988). Congestion avoidance and control. Proceedings of ACM SIGCOMM 1988, 314–329.

**\[8\]** Chiu, D.-M., & Jain, R. (1989). Analysis of the increase and decrease algorithms for congestion avoidance in computer networks. Computer Networks and ISDN Systems, 17(1), 1–14.

**\[9\]** Allman, M., Paxson, V., & Blanton, E. (2009). TCP Congestion Control. RFC 5681, IETF.

**\[10\]** Høiland-Jørgensen, T. et al. (2016). The FlowQueue-CoDel Packet Scheduler and Active Queue Management Algorithm. RFC 8290, IETF.

**\[11\]** Stevens, W.R. (1994). TCP/IP Illustrated, Volume 1: The Protocols. Addison-Wesley.

**\[12\]** Mogul, J., & Deering, S. (1990). Path MTU Discovery. RFC 1191, IETF.

**\[13\]** Nagle, J. (1984). Congestion Control in IP/TCP Internetworks. RFC 896, IETF.

**\[14\]** Borman, D., Braden, R., Jacobson, V., & Scheffenegger, R. (2014). TCP Extensions for High Performance. RFC 7323, IETF. (Obsoletes RFC 1323.)

**\[15\]** Apache Software Foundation. (2024). Apache Kafka Documentation: Producer Configurations. https://kafka.apache.org/documentation/#producerconfigs

**\[16\]** Shannon, C.E. (1948). A mathematical theory of communication. Bell System Technical Journal, 27(3), 379–423.


> **Formula chain**
>
> [1] Little 1961 → [2] RFC 1323 (1992) → [7,9] Jacobson / RFC 5681 (1988–2009) → [3] Mathis (1997) → [4] BBR (2016) → [15] Kafka F10–F17 (application layer). Each step adds precision about what happens when the ideal full-pipe condition of F1 cannot be sustained: packet loss (F4, F5), congestion signalling dynamics (F6, F7), header overhead (F8), application-layer batching (F10–F12), receive-side batching (F13–F14), or replication topology (F15–F17).

