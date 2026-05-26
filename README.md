# TCP Kafka Tuning

> *Measurement-driven configuration guide for TCP and Apache Kafka — from theory to production settings.*

A self-contained reference for diagnosing and optimising throughput across the IP, TCP, and Apache Kafka layers. Grounded in published network performance theory: Little's Law [1], RFC 1323 [2], the Mathis throughput bound [3], and the BBR congestion-control model [4].

---

## Repository Structure

```
├── docs/
│   └── kafka-tcp-tuning-guide.md        Full technical reference (Markdown)
│
├── scripts/
│   ├── kafka-tcp-measure.sh             Measurement script (ping + iperf3)
│   ├── kafka-tcp-analyze.sh             Analysis script — computes BDP, writes configs
│   └── kafka-tcp-k8s.yaml               Kubernetes PVC, Job, and reader pod manifests
│
├── dashboards/
│   ├── tcp-throughput-explainer.jsx     Interactive TCP explainer (React)
│   └── kafka-tcp-tuning.jsx             Kafka TCP tuning dashboard (React)
│
├── docker/
│   ├── Dockerfile                       Multi-stage build (node:alpine → alpine)
│   ├── docker-compose.yml               Convenience compose file
│   ├── .dockerignore
│   ├── scripts/
│   │   └── entrypoint.sh                Starts both busybox httpd instances
│   ├── tcp/                             Vite project for tcp-throughput-explainer
│   │   ├── package.json
│   │   ├── vite.config.js
│   │   ├── index.html
│   │   └── src/
│   │       ├── main.jsx
│   │       └── App.jsx                  ← mirrors dashboards/tcp-throughput-explainer.jsx
│   └── kafka/                           Vite project for kafka-tcp-tuning
│       ├── package.json
│       ├── vite.config.js
│       ├── index.html
│       └── src/
│           ├── main.jsx
│           └── App.jsx                  ← mirrors dashboards/kafka-tcp-tuning.jsx
│
├── .gitignore
└── README.md
```

> **Note on `docker/*/src/App.jsx`:** these are kept in sync with `dashboards/`. When updating a dashboard, copy the changed `.jsx` into both locations, or replace `docker/tcp/src/App.jsx` and `docker/kafka/src/App.jsx` with symlinks.

---

## Background

The achievable throughput of a TCP connection is governed by a single identity derived from Little's Law (1961):

```
T = W / RTT
```

where `T` is throughput, `W` is the window size, and `RTT` is the round-trip time. The window must be at least as large as the **Bandwidth-Delay Product** (`BDP = B × RTT`) for the link to be fully utilised. When it is not — at either the TCP layer or the Kafka batch layer — the sender stalls and capacity is permanently lost.

Kafka producers add a second window on top of TCP: the effective Kafka window is `batch.size × max.in.flight.requests.per.connection`. Both windows must meet or exceed the path BDP.

---

## Quick Start

### Prerequisites

| Tool | Used by |
|---|---|
| `iperf3 ≥ 3.6` | Both scripts |
| `ping` | `kafka-tcp-measure.sh` |
| `bc`, `python3 ≥ 3.8` | Both scripts |

Tested on `nicolaka/netshoot`. No GNU grep (`-P`) required — all parsing uses `python3`.

### Step 1 — Measure

Start an `iperf3` server on each Kafka broker host:

```bash
iperf3 -s -D -p 5201
```

Run the measurement suite from the producer host:

```bash
chmod +x scripts/kafka-tcp-measure.sh scripts/kafka-tcp-analyze.sh

scripts/kafka-tcp-measure.sh \
  -t <broker-ip> \   # broker IP or hostname
  -s 8 \             # parallel streams — match Kafka producer concurrency
  -m 1500 \          # path MTU (9000 for jumbo frames)
  -o ./results
```

| Phase | Tool | Measures |
|---|---|---|
| 1 — RTT baseline | `ping -c 200` | RTT_min, RTT_avg, jitter (mdev), packet loss |
| 2 — Window sweep | `iperf3 -w` | Throughput at 4 KB → 4 MB windows; empirical BDP plateau |
| 3 — Parallel streams | `iperf3 -P` | Aggregate throughput at 1, 2, 4, 8 streams |
| 4 — Nagle effect | `iperf3 --no-delay` | Throughput delta with/without `TCP_NODELAY` |

### Step 2 — Analyse

```bash
scripts/kafka-tcp-analyze.sh \
  -d ./results \   # directory from Step 1
  -c 8 \           # Kafka connections per broker
  -m 1500 \        # path MTU
  -l 50            # end-to-end latency SLA in ms
```

Four configuration files are written to `./results/`:

| Output file | How to apply |
|---|---|
| `99-kafka-tcp.conf` | `sudo sysctl -p results/99-kafka-tcp.conf` |
| `producer-throughput.properties` | Add to `producer.properties` — throughput profile |
| `producer-latency.properties` | Add to `producer.properties` — latency profile |
| `broker-additions.properties` | Add to `server.properties`; restart broker |

### Step 3 — Verify

```bash
# Confirm BBR and fq are active
sysctl net.ipv4.tcp_congestion_control     # → bbr
tc qdisc show dev eth0                     # → fq

# Inspect window on live Kafka connections
ss -tin dst <broker-ip> | grep -E 'rtt|cwnd|mss|pacing_rate'
# cwnd × mss should approach the empirical BDP

# Confirm TCP_NODELAY (Nagle disabled)
ss -tinp | grep <kafka-port> | grep nonagle
```

---

## Interactive Dashboards

The `.jsx` files in `dashboards/` are self-contained React components with no external API calls.

| Option | Requires | Daemon? | Best for |
|---|---|---|---|
| A — Docker | Docker | Yes | Persistent local hosting |
| B — Podman / nerdctl | podman or nerdctl | No | Daemonless drop-in replacement |
| C — Buildah | buildah | No | Daemonless OCI image build |
| D — Static files | node + python3 | No | Dev, netshoot, no image needed |
| E — Local dev server | node | No | Development with hot reload |
| F — CodeSandbox | browser | — | Shareable online sandbox |

---

**Option A — Docker**

```bash
# From repo root
docker compose -f docker/docker-compose.yml up

# Or build and run directly
docker build -t tcp-kafka-viz docker/
docker run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
```

The image is ~15 MB (Alpine + busybox-extras + two ~570 KB JS bundles). No Node at runtime — `httpd` from `busybox-extras` serves the static build output. Note: invoke as `httpd` directly, not `busybox httpd` — the latter form fails on Alpine.

---

**Option B — Podman / nerdctl (daemonless, drop-in replacement)**

Both tools are compatible with the existing `Dockerfile` and `docker-compose.yml` without any changes.

Podman (common on RHEL/Fedora; available via `brew install podman` on macOS):

```bash
podman build -t tcp-kafka-viz docker/
podman run -p 3001:3001 -p 3002:3002 tcp-kafka-viz

# Or with the compose file
podman compose -f docker/docker-compose.yml up
```

nerdctl (containerd-native; common on k3s, Rancher, EKS nodes):

```bash
nerdctl build -t tcp-kafka-viz docker/
nerdctl run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
```

Both are rootless by default and produce standard OCI images pushable to any registry.

---

**Option C — Buildah (daemonless, no root required)**

Buildah builds OCI-compliant images without a Docker daemon and without root. The existing `Dockerfile` works unchanged:

```bash
buildah bud -t tcp-kafka-viz docker/
```

Run with Podman (which is typically paired with Buildah):

```bash
podman run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
```

Or export to a tar archive for transfer:

```bash
buildah push tcp-kafka-viz docker-archive:tcp-kafka-viz.tar
```

---

**Option D — Static files (no image, no daemon)**

Both apps build to plain static HTML + JS. The built output can be served by anything — including tools already present in `nicolaka/netshoot`.

Build first (requires `node`):

```bash
cd docker/tcp   && npm install && npm run build   # → docker/tcp/dist/
cd docker/kafka && npm install && npm run build   # → docker/kafka/dist/
```

Serve with Python (present in netshoot and most systems):

```bash
python3 -m http.server 3001 --directory docker/tcp/dist &
python3 -m http.server 3002 --directory docker/kafka/dist &
```

Or with `npx serve` (downloads on first run if node is available):

```bash
npx serve -l 3001 docker/tcp/dist &
npx serve -l 3002 docker/kafka/dist &
```

Both are single commands with no persistent process or port — stop with `kill %1 %2` or `pkill -f http.server`.

---

**Option E — Local dev server (hot reload)**

```bash
cd docker/tcp   && npm install && npm run dev   # → http://localhost:5173
cd docker/kafka && npm install && npm run dev   # → http://localhost:5174
```

Vite's dev server watches for changes and reloads instantly — useful when editing the `.jsx` files directly.

---

**Option F — CodeSandbox**

Open [codesandbox.io/s/new](https://codesandbox.io/s/new), replace `App.js` with the `.jsx` content, and run. Add `recharts` to the sandbox dependencies.

---

| URL | App |
|---|---|
| `http://localhost:3001` | TCP Throughput Explainer |
| `http://localhost:3002` | Kafka TCP Tuning Dashboard |

---

## Tuning Sequence

Steps 1–5 require no network infrastructure changes.

| # | Change | Impact | Network change? |
|---|---|---|---|
| 1 | `compression.type = lz4` | ★★★★★ | No |
| 2 | `batch.size` + `linger.ms` | ★★★★☆ | No |
| 3 | TCP socket buffers (`rmem`/`wmem`) | ★★★★☆ | No |
| 4 | BBR + `fq` qdisc | ★★★☆☆ | No |
| 5 | `TCP_NODELAY` (verify) | ★★☆☆☆ | No |
| 6 | Jumbo frames (Kafka VLAN only) | ★★★★☆ | Yes — switch port config |
| 7 | Disable TCP timestamps | ★☆☆☆☆ | No |

> **BBR + fq pairing is mandatory.** BBR is a pacing algorithm; without `net.core.default_qdisc = fq` its pacing guarantees are void and behaviour degrades to approximately CUBIC.

---

## Deployment Scenarios

| Scenario | Bandwidth | RTT | BDP | `batch.size` | `rmem_max` | `linger.ms` | CC |
|---|---|---|---|---|---|---|---|
| Kafka defaults | — | — | — | 16 KB | 256 KB | 0 | cubic |
| Intra-datacenter | 10 Gbps | 0.2 ms | ~250 KB | 128 KB | 32 MB | 5 | bbr |
| Same-AZ | 1 Gbps | 5 ms | ~625 KB | 128 KB | 128 MB | 10 | bbr |
| Cross-AZ | 1 Gbps | 20 ms | ~2.5 MB | 256 KB | 256 MB | 20 | bbr |
| Cross-region | 500 Mbps | 60 ms | ~3.75 MB | 512 KB | 512 MB | 50 | bbr |
| Multi-region | 200 Mbps | 150 ms | ~3.75 MB | 1 MB | 1 GB | 100 | bbr |
| Satellite | 50 Mbps | 600 ms | ~3.75 MB | 1 MB | 2 GB | 500 | bbr |

Values assume `compression.type = lz4` and `acks = all`.

---

## Kubernetes Deployment

```bash
# Create ConfigMap from script files
kubectl create configmap kafka-tcp-scripts \
  --from-file=scripts/kafka-tcp-measure.sh \
  --from-file=scripts/kafka-tcp-analyze.sh \
  --namespace confluent

# Apply PVC, Job, and reader pod
kubectl apply -f scripts/kafka-tcp-k8s.yaml
```

The Job command captures a single timestamp and passes it to both scripts:

```yaml
command:
  - bash
  - -c
  - |
    TIMESTAMP=$(date +%Y%m%d-%H%M%S) &&
    /scripts/kafka-tcp-measure.sh -t <broker-ip> -p 5201 -s 8 -o /results/$TIMESTAMP &&
    /scripts/kafka-tcp-analyze.sh  -d /results/$TIMESTAMP -c 8 -m 1500 -l 50
```

### Retrieving results

```bash
kubectl --context ${CONTEXT} apply -f scripts/kafka-tcp-k8s.yaml
kubectl --context ${CONTEXT} wait --for=condition=Ready pod/results-reader \
  -n confluent --timeout=60s

# Find latest timestamped run directory (excludes loose files and lost+found)
LATEST=$(kubectl --context ${CONTEXT} \
  exec -i results-reader -n confluent -- \
  sh -c 'ls -1 /results | grep "^[0-9]" | sort -r | head -n1' \
  | tr -d '\r\n')

echo "Latest result: [${LATEST}]"

# Copy via tar — more reliable than kubectl cp on busybox
mkdir -p ./results-${LATEST}
kubectl --context ${CONTEXT} exec -i results-reader -n confluent -- \
  tar cf - -C /results/${LATEST} . \
  | tar xf - -C ./results-${LATEST}
```

| Symptom | Cause | Fix |
|---|---|---|
| `LATEST` contains `\r` | `-it` injects TTY line endings | Use `-i` only; pipe through `tr -d '\r\n'` |
| `LATEST` picks up `window_sweep.csv` | `ls` sorts files before dirs | `grep "^[0-9]"` filters to timestamp dirs |
| `tar: No such file or directory` | `kubectl cp` incompatible with busybox `tar` | Use `tar cf - \| tar xf -` over `exec` |
| `Multi-Attach` error on PVC | RWO PVC still bound to Job node | `kubectl delete job kafka-tcp-measure` first |

---

## Formula Reference

All 12 formulas used in this guide. Full derivations in `docs/kafka-tcp-tuning-guide.md` §2–3.

| ID | Name | Formula | Source |
|---|---|---|---|
| F1 | Window-limited throughput | `T = W / RTT` | Little [1]; RFC 1323 [2] |
| F2 | Bandwidth-Delay Product | `BDP = B × RTT` | RFC 1323 [2] |
| F3 | Throughput, practical | `T_max = (W × 8) / RTT  [bit/s]` | Stevens [11] Ch. 20 |
| F4 | Mathis bound | `T ≤ MSS / (RTT × √p)` | Mathis et al. [3] |
| F5 | Padhye refined model | `T ≈ MSS / [RTT×√(2p/3) + T_RTO×…]` | Padhye et al. [6] |
| F6 | Slow start | `cwnd(t+1) = 2 × cwnd(t)` | Jacobson [7]; RFC 5681 [9] |
| F7 | AIMD | `cwnd += MSS²/cwnd per ACK` | Jacobson [7]; Chiu & Jain [8] |
| F8 | MSS from MTU | `MSS = MTU − 40` (IPv4) | RFC 879 [5]; RFC 1191 [12] |
| F9 | BBR operating point | `Inflight = BtlBw × RTprop` | Cardwell et al. [4] |
| F10 | Kafka effective window | `W_eff = batch.size × max.in.flight` | Derived from F1 |
| F11 | Kafka `linger.ms` (throughput) | `linger_t = (batch.size × 8) / B × 1000` | Derived from F1 |
| F12 | Kafka `linger.ms` (latency) | `linger_l = SLA_ms − RTT_ms − t_broker` | Latency budget |

**Formula chain:** Little (1961) → RFC 1323 (1992) → Jacobson/RFC 5681 (1988–2009) → Mathis (1997) → BBR (2016) → Kafka F10–F12.

> **Mathis unit note:** `T = MSS / (RTT × √p)` yields bytes/second. Divide by 125,000 (= 10⁶/8) to convert to Mbit/s. Dividing by 10⁶ instead — a common error — produces values 8× too small.

---

## Key References

Full citations in `docs/kafka-tcp-tuning-guide.md` §10.

- **[1]** Little, J.D.C. (1961). A proof for the queuing formula: L = λW. *Operations Research*, 9(3).
- **[2]** Jacobson, V., Braden, R., & Borman, D. (1992). TCP Extensions for High Performance. **RFC 1323**.
- **[3]** Mathis, M., Semke, J., Mahdavi, J., & Ott, T. (1997). The macroscopic behavior of the TCP congestion avoidance algorithm. *ACM SIGCOMM CCR*, 27(3).
- **[4]** Cardwell, N. et al. (2016). BBR: Congestion-based congestion control. *ACM Queue*, 14(5).
- **[5]** Postel, J. (1983). TCP Maximum Segment Size. **RFC 879**.
- **[6]** Padhye, J. et al. (1998). Modeling TCP throughput. *ACM SIGCOMM*.
- **[7]** Jacobson, V. (1988). Congestion avoidance and control. *ACM SIGCOMM*.
- **[8]** Chiu, D.-M., & Jain, R. (1989). Analysis of the increase and decrease algorithms for congestion avoidance. *Computer Networks*, 17(1).
- **[9]** Allman, M., Paxson, V., & Blanton, E. (2009). TCP Congestion Control. **RFC 5681**.
- **[11]** Stevens, W.R. (1994). *TCP/IP Illustrated, Volume 1.* Addison-Wesley.
- **[12]** Mogul, J., & Deering, S. (1990). Path MTU Discovery. **RFC 1191**.
- **[14]** Borman, D. et al. (2014). TCP Extensions for High Performance. **RFC 7323**.

---

## License

| Component | License |
|---|---|
| Scripts (`scripts/`) | [MIT](https://opensource.org/licenses/MIT) |
| Document content (`docs/`) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) |
| Dashboards (`dashboards/`) | [MIT](https://opensource.org/licenses/MIT) |
| Docker build files (`docker/`) | [MIT](https://opensource.org/licenses/MIT) |
