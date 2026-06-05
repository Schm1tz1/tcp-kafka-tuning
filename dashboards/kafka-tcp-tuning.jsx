import { useState, useCallback } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Legend, AreaChart, Area,
  BarChart, Bar, Cell,
} from "recharts";

// ── Palette ──────────────────────────────────────────────────────────────────
const P = {
  bg:"#08090d", panel:"#0f1117", panel2:"#13161f", border:"#1e2333",
  border2:"#2a3045", text:"#dde3f0", muted:"#5a6480", dim:"#3a4060",
  accent:"#4f8ef7", green:"#34c97a", yellow:"#f5c542", red:"#f05a5a",
  purple:"#a78bfa", cyan:"#22d3c8", orange:"#f97316",
};
const COLORS = [P.accent, P.green, P.yellow, P.red, P.purple, P.cyan, P.orange];

// ── Log scale toggle ──────────────────────────────────────────────────────────
function yAxisProps(logScale, minVal, labelText, extra={}) {
  return {
    stroke: P.muted,
    tick: { fontSize: 10 },
    scale: logScale ? "log" : "linear",
    domain: logScale ? [minVal, "auto"] : [0, "auto"],
    tickFormatter: v => v >= 1000 ? `${(v/1000).toFixed(v>=10000?0:1)}G` : `${v}`,
    label: {
      value: `${labelText}${logScale ? " (log)" : ""}`,
      angle: -90,
      position: "insideLeft",
      dx: -8,
      fill: P.muted,
      fontSize: 11,
    },
    ...extra,
  };
}

const LogToggle = ({ value, onChange }) => (
  <button onClick={() => onChange(!value)} style={{
    background: value ? P.accent + "22" : "transparent",
    color: value ? P.accent : P.muted,
    border: `1px solid ${value ? P.accent + "66" : P.border}`,
    borderRadius: 5, padding: "2px 10px",
    fontSize: "0.72em", fontWeight: 700, cursor: "pointer",
    letterSpacing: "0.05em", textTransform: "uppercase",
    transition: "all 0.15s", fontFamily: "monospace",
  }}>log y</button>
);

const ChartHeader = ({ title, logY, setLogY }) => (
  <div style={{ display:"flex", justifyContent:"space-between",
    alignItems:"center", marginBottom:12 }}>
    <Label c={P.muted}>{title}</Label>
    <LogToggle value={logY} onChange={setLogY} />
  </div>
);

// ── Helpers ───────────────────────────────────────────────────────────────────
const fmtBytes = v => {
  if (v >= 1073741824) return `${(v/1073741824).toFixed(1)} GB`;
  if (v >= 1048576)    return `${(v/1048576).toFixed(1)} MB`;
  if (v >= 1024)       return `${(v/1024).toFixed(0)} KB`;
  return `${v} B`;
};
const fmtMbps  = v => v >= 1000 ? `${(v/1000).toFixed(v>=10000?0:1)} Gbps` : `${Math.round(v)} Mbps`;
const nextPow2 = n => { let p=1; while(p<n) p<<=1; return p; };

const BATCH_STEPS = [16384,32768,65536,131072,262144,524288,1048576];
const nearestBatch = n => BATCH_STEPS.find(b=>b>=n) || BATCH_STEPS[BATCH_STEPS.length-1];

// ── Calculation references ───────────────────────────────────────────────────
// F1  T = W/RTT              Little (1961) Op.Res. 9(3); RFC 1323 §1 (Jacobson et al. 1992)
// F2  BDP = B×RTT            RFC 1323 §1
// F4  T ≤ MSS/(RTT×√p)      Mathis, Semke, Mahdavi, Ott (1997) ACM SIGCOMM CCR 27(3)
// F8  MSS = MTU−40           RFC 879 (Postel 1983); PMTUD: RFC 1191 (Mogul & Deering 1990)
// F9  BBR: BtlBw×RTprop=BDP  Cardwell et al. (2016) ACM Queue 14(5)
// F10 W_eff = batch×inflight  Apache Kafka Producer docs (application of F1)
// F11 linger_t = W×8/B       Derived from F1 — batch drain time
// F12 linger_l = SLA−RTT−t_b  End-to-end latency budget decomposition
// F13 Consumer fetch window per partition = max.partition.fetch.bytes
// F14 fetch.max.wait.ms tradeoff: latency vs batching (symmetric to F11)
// F15 Replica lag timeout = RTT×4 + fetch.max.wait.ms + 5000ms margin
// F16 num.replica.fetchers = ceil(partitions / 4) — one fetcher per ~4 partitions
// F17 replica.fetch.max.bytes ≥ producer batch.size for full-batch replication
// F18 Effective bandwidth per broker = bw / (1 + (RF-1) × 2) — accounts for replication I/O
// F19 Per-broker bandwidth budget: producer_in + consumer_out + replication_in + replication_out ≤ BW_limit
function calcFromMeasurements({bwMbps, rttMin, rttAvg, plateauKB, conns, mtu, inflight, latencyBudgetMs, pktLoss, partitions, compressionRatio, replicationFactor=3, brokers=3, producerCount=3, consumerCount=2, expectedThroughputPct=70, expectedThroughputMbps=100, throughputMode='percent'}) {
  const bwBytes   = bwMbps * 1e6 / 8;
  const rttSMin   = rttMin / 1000;
  const rttSAvg   = rttAvg / 1000;
  const empiricalBDP  = plateauKB * 1024;
  const theoreticalBDP = Math.round(bwBytes * rttSMin);
  const mss       = mtu - 40;
  const bufCeil   = nextPow2(empiricalBDP * conns * 2);
  // batchMin = max(BDP÷inflight, 2×MSS) — must cover the per-connection window slice
  // AND be large enough to hold at least 2 full frames (avoids sub-frame batches on
  // jumbo-frame paths where MSS=8960 and BDP is small relative to link speed).
  const batchMin  = Math.max(Math.round(empiricalBDP / inflight), mss * 2);
  const batchSize = nearestBatch(batchMin);
  const lingerThru    = Math.max(1, Math.round((empiricalBDP*8)/(bwMbps*1e6)*1000));
  const lingerLatency = Math.max(0, Math.round(latencyBudgetMs - rttAvg - 2));
  // Mathis et al. (1997) ACM SIGCOMM CCR 27(3) throughput bound:
  //   T = MSS / (RTT × √p)   [bytes/sec]
  // Divide by 125000 (= 1e6/8) to convert bytes/sec → Mbit/s.
  // pktLoss is a percentage (e.g. 1.0 = 1%), so p = pktLoss/100.
  const mathisMbps = pktLoss > 0
    ? (mss / (rttSAvg * Math.sqrt(pktLoss / 100))) / 125000
    : null;

  // ── Kafka throughput estimates ─────────────────────────────────────────────
  // Wire throughput ceiling: window-limited (F1)
  const kafkaWindowMbps = ((batchSize * inflight * 8) / (rttSAvg)) / 1e6;

  // Effective Kafka throughput: min of window limit and link bandwidth
  const kafkaWireMbps = Math.min(bwMbps, kafkaWindowMbps);

  // After compression: logical (application-layer) throughput
  const kafkaLogicalMbps = kafkaWireMbps * compressionRatio;

  // Loss-limited ceiling (Mathis F4) — caps everything if loss > 0
  const effectiveMbps = mathisMbps
    ? Math.min(kafkaWireMbps, mathisMbps)
    : kafkaWireMbps;
  const effectiveLogicalMbps = effectiveMbps * compressionRatio;

  // ── Per-partition estimates ────────────────────────────────────────────────
  // Each partition gets one TCP connection (one producer → one leader).
  // Per-partition window = batch.size × inflight (same formula, one connection).
  // Per-partition wire throughput = min(link_bw / partitions, window / RTT)
  const perPartWireMbps   = effectiveMbps / partitions;
  const perPartLogicalMbps = effectiveLogicalMbps / partitions;

  // Per-partition window utilisation: how much of the BDP does one partition use?
  const perPartWindowBytes = batchSize * inflight;
  const perPartBdpPct = Math.min(100, Math.round((perPartWindowBytes / empiricalBDP) * 100));

  // Throughput vs partition count series (for chart)
  const partitionSeries = [1,2,4,8,16,32,64,128].map(p => ({
    partitions: p,
    wireMbps:    Math.round(effectiveMbps / p * 10) / 10,
    logicalMbps: Math.round(effectiveLogicalMbps / p * 10) / 10,
  }));

  // ── Consumer configuration (F13, F14) ──────────────────────────────────────
  // Consumer fetch window should be >= producer batch size to avoid stalls.
  // Each partition is fetched independently, so max.partition.fetch.bytes sets
  // the per-partition receive window.
  const consumerFetchMaxBytes = Math.max(batchSize, 1048576); // min 1MB

  // fetch.min.bytes controls batching on the consumer side. Higher = more batching,
  // less CPU, but higher latency. Set to half of batch size as a starting point.
  const consumerFetchMinBytes = Math.max(1, Math.round(batchSize / 2));

  // fetch.max.wait.ms (F14): time broker waits to accumulate fetch.min.bytes.
  // For throughput: match linger.ms from producer. For latency: minimize (100-500ms).
  const consumerFetchMaxWaitThru = lingerThru;
  const consumerFetchMaxWaitLat  = Math.min(500, Math.max(100, Math.round(latencyBudgetMs * 0.3)));

  // Consumer receive.buffer.bytes should be >= BDP, same reasoning as producer send buffer
  const consumerReceiveBuffer = bufCeil;

  // ── Broker replication configuration (F15, F16, F17) ───────────────────────
  // replica.fetch.max.bytes should be >= producer batch.size so replicas can fetch
  // complete batches in one request. Larger = fewer fetch requests but more memory.
  const replicaFetchMaxBytes = Math.max(batchSize, 1048576);

  // num.replica.fetchers: one fetcher thread can handle ~4-8 partitions efficiently.
  // More fetchers = higher parallelism but more broker connections and threads.
  // In multi-region clusters (MRC) with high RTT, increase to 2-3 to avoid replication lag.
  const baseReplicaFetchers = Math.max(1, Math.ceil(partitions / 6));
  const numReplicaFetchers = rttAvg > 50 ? Math.max(baseReplicaFetchers, 2) : baseReplicaFetchers;

  // replica.lag.time.max.ms (F15): timeout before a replica is considered out-of-sync.
  // Must account for: fetch round-trip (RTT×2), broker processing, fetch.max.wait.ms,
  // plus safety margin for GC pauses and network variance.
  // Formula: RTT × 4 (2 round-trips with headroom) + fetch.max.wait.ms + 5000ms margin
  const replicaLagTimeoutMs = Math.max(10000,
    Math.round(rttAvg * 4 + consumerFetchMaxWaitThru + 5000));

  // replica.socket.receive.buffer.bytes — followers fetch from leaders, need >= BDP
  const replicaSocketReceiveBuffer = bufCeil;

  // ── Per-broker bandwidth analysis (F18, F19) ───────────────────────────────
  // CRITICAL: bwMbps is "per-broker NIC bandwidth limit", not total cluster
  // Each broker node must handle: producer writes, consumer reads, replication IN+OUT

  // Expected producer throughput (not link capacity)
  // Can be specified as percentage of link capacity OR as absolute Mbps
  const expectedProducerMbps = throughputMode === 'percent'
    ? effectiveMbps * (expectedThroughputPct / 100)
    : expectedThroughputMbps;

  // Partition distribution (assuming even distribution)
  const partitionsPerBroker = Math.ceil(partitions / brokers);
  // Each broker is leader for ~partitionsPerBroker partitions
  const leaderPartitionsPerBroker = partitionsPerBroker;
  // Each broker is follower for remaining partitions
  // With RF replicas, each partition has (RF-1) followers distributed across remaining brokers
  const followerPartitionsPerBroker = Math.ceil(partitions * (replicationFactor - 1) / brokers);

  // Effective replication bandwidth multiplier (F18):
  // At RF=3: each 1 MB write → 1 MB to leader + 2 MB replication = 3× amplification
  // Leader node bandwidth breakdown:
  //   - Producer ingress: expected producer throughput
  //   - Replication OUT (leader → followers): producer_throughput × (RF-1)
  //   - Replication IN (as follower for other partitions): depends on partition distribution
  //   - Consumer egress: consumer read throughput

  // Per-broker producer ingress (writes to this broker's leader partitions)
  const perBrokerProducerIngressMbps = expectedProducerMbps * (leaderPartitionsPerBroker / partitions);

  // Per-broker replication OUT (this broker as leader → followers)
  // Each write is replicated (RF-1) times
  const perBrokerReplicationOutMbps = perBrokerProducerIngressMbps * (replicationFactor - 1);

  // Per-broker replication IN (this broker as follower ← other leaders)
  // Follower fetches data for followerPartitionsPerBroker partitions at producer write rate
  const perBrokerReplicationInMbps = expectedProducerMbps * (followerPartitionsPerBroker / partitions);

  // Per-broker consumer egress (reads from this broker's leader partitions)
  // Assume read rate matches write rate (typical steady-state)
  const perBrokerConsumerEgressMbps = perBrokerProducerIngressMbps;

  // Total per-broker bandwidth usage
  const perBrokerTotalMbps = perBrokerProducerIngressMbps +
                              perBrokerReplicationOutMbps +
                              perBrokerReplicationInMbps +
                              perBrokerConsumerEgressMbps;

  // Per-broker utilization percentage
  const perBrokerUtilization = Math.round((perBrokerTotalMbps / bwMbps) * 100);

  // Per-broker connection count
  // Producer connections: each producer connects to brokers hosting its partitions
  // Assuming producers are evenly distributed and sticky partitioner
  const perBrokerProducerConns = Math.ceil(producerCount * (leaderPartitionsPerBroker / partitions));

  // Consumer connections: each consumer connects to brokers hosting partitions it consumes
  // With partition assignment, each consumer connects to subset of brokers
  const perBrokerConsumerConns = Math.min(consumerCount, Math.ceil(consumerCount * (leaderPartitionsPerBroker / partitions)));

  // Replica fetcher connections: each broker runs num.replica.fetchers threads,
  // and each thread connects to every OTHER broker (brokers - 1).
  // Total outgoing connections = num.replica.fetchers × (brokers - 1)
  const perBrokerReplicaFetcherConnsOut = numReplicaFetchers * Math.max(0, brokers - 1);

  // Incoming: each of the other brokers connects with num.replica.fetchers threads
  const perBrokerReplicaFetcherConnsIn = numReplicaFetchers * Math.max(0, brokers - 1);

  const perBrokerTotalConns = perBrokerProducerConns + perBrokerConsumerConns +
                               perBrokerReplicaFetcherConnsIn + perBrokerReplicaFetcherConnsOut;

  // num.network.threads: handles all I/O (producer, consumer, replication, inter-broker)
  // Rule of thumb: 1 thread per ~50 connections, minimum 8, scale up for large clusters
  // For replication alone: need at least (brokers - 1) × num.replica.fetchers + 1 for clients
  const minNetworkThreadsForReplication = Math.max(0, brokers - 1) * numReplicaFetchers + 1;
  const numNetworkThreads = Math.max(8, Math.ceil(perBrokerTotalConns / 50), minNetworkThreadsForReplication);

  // Total replication bandwidth (cluster-wide, for reference)
  const totalReplicaConnections = partitions * (replicationFactor - 1);
  const replicationWireMbps = expectedProducerMbps * (replicationFactor - 1);

  // Bottleneck analysis
  const bottleneck = perBrokerUtilization > 100 ? "per-broker NIC limit" :
                      mathisMbps ? "packet loss (Mathis)" :
                      kafkaWindowMbps < bwMbps ? "Kafka window" :
                      "link bandwidth";

  // ── MTU/MSS-derived metrics ────────────────────────────────────────────────
  // Number of TCP segments per Kafka batch (used for header overhead calculation)
  const segmentsPerBatch = Math.ceil(batchSize / mss);

  // Kafka protocol overhead per batch: RecordBatch header (61 bytes) + per-Record overhead (14 bytes/record)
  // Simplified: assume one segment ≈ one record for estimation
  const kafkaOverheadPerBatch = segmentsPerBatch * 75; // 61 + 14 bytes

  // Effective payload ratio after Kafka headers (what % of batch is actual application data)
  const effectivePayloadRatio = (batchSize - kafkaOverheadPerBatch) / batchSize;

  // Packets needed to transmit 1 MB at current MSS
  const packetsPerMB = Math.ceil(1048576 / mss);

  // TCP/IP header overhead percentage (40 bytes per packet)
  const headerOverheadPct = (40 / mtu) * 100;

  return {empiricalBDP, theoreticalBDP, mss, bufCeil, batchSize, batchMin,
          lingerThru, lingerLatency, mathisMbps,
          kafkaWireMbps, kafkaLogicalMbps, kafkaWindowMbps,
          effectiveMbps, effectiveLogicalMbps, expectedProducerMbps,
          perPartWireMbps, perPartLogicalMbps, perPartWindowBytes, perPartBdpPct,
          partitionSeries,
          // Consumer settings
          consumerFetchMaxBytes, consumerFetchMinBytes,
          consumerFetchMaxWaitThru, consumerFetchMaxWaitLat, consumerReceiveBuffer,
          // Replication settings
          replicaFetchMaxBytes, numReplicaFetchers, replicaLagTimeoutMs,
          replicaSocketReceiveBuffer, totalReplicaConnections, replicationWireMbps,
          numNetworkThreads,
          // Per-broker analysis (F18, F19)
          brokers, partitionsPerBroker, leaderPartitionsPerBroker, followerPartitionsPerBroker,
          perBrokerProducerIngressMbps, perBrokerReplicationOutMbps,
          perBrokerReplicationInMbps, perBrokerConsumerEgressMbps,
          perBrokerTotalMbps, perBrokerUtilization, bottleneck,
          perBrokerProducerConns, perBrokerConsumerConns,
          perBrokerReplicaFetcherConnsIn, perBrokerReplicaFetcherConnsOut, perBrokerTotalConns,
          // MTU/MSS metrics
          segmentsPerBatch, kafkaOverheadPerBatch, effectivePayloadRatio, packetsPerMB, headerOverheadPct};
}

// ── Window sweep simulation ───────────────────────────────────────────────────
// T = min(B, W×8/RTT) — F1 applied per window size. Plateau = empirical BDP.
// RFC 1323 §1 motivates this sweep as the key diagnostic for LFN paths.
function simWindowSweep(bwMbps, rttMs) {
  const bwBps = bwMbps * 1e6;
  const rttS  = rttMs / 1000;
  return [4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384].map(kb => {
    const win = kb * 1024;
    const tput = Math.min(bwBps, (win * 8) / rttS);
    return { win: kb, tput: Math.round(tput/1e6) };
  });
}

// ── BBR vs CUBIC simulation ───────────────────────────────────────────────────
// CUBIC: Ha, Rhee, Xu (2008) ACM SIGOPS OSR 42(5).
// BBR:   Cardwell, Cheng, Gunn, Yeganeh, Jacobson (2016) ACM Queue 14(5).
// AIMD fairness proof: Chiu & Jain (1989) Comput. Networks ISDN Syst. 17(1).
// Simulates ~60 RTT rounds of a single TCP flow.
// bwMbps: link bandwidth, rttMs: propagation RTT, bufMss: switch buffer in MSS, mss: TCP MSS in bytes
function simBbrVsCubic(bwMbps, rttMs, bufMss = 50, mss = 1460) {
  const bdpMss   = Math.max(1, Math.round((bwMbps * 1e6 / 8) * (rttMs / 1000) / mss));
  const maxCwnd  = bdpMss + bufMss;   // pipe + switch buffer
  const data     = [];

  // ── CUBIC simulation ─────────────────────────────────────────────────────
  let cwnd_c   = 2;
  let ssthresh = bdpMss * 1.5;
  let qDepth_c = 0;

  // ── BBR simulation ────────────────────────────────────────────────────────
  // BBR targets BDP, probes BW every 8 RTTs (+25%), drains every 10s (~probe_rtt)
  let cwnd_b    = bdpMss;
  let btlbw     = bwMbps;       // estimated bottleneck BW (Mbps)
  let rtprop    = rttMs;        // estimated prop delay (ms)
  let bbrPhase  = 0;            // 0=cruise, 1=probe_bw_up, 2=probe_bw_down, 3=probe_rtt

  for (let t = 0; t < 65; t++) {

    // ── CUBIC ──────────────────────────────────────────────────────────────
    const inFlight_c = Math.min(cwnd_c, maxCwnd);
    qDepth_c = Math.max(0, inFlight_c - bdpMss);
    const rtt_c = rttMs + (qDepth_c / bdpMss) * rttMs * 2;  // RTT inflates with queue
    const tput_c = Math.min(bwMbps, (inFlight_c * mss * 8) / (rtt_c / 1000) / 1e6);

    // Loss when queue overflows
    const loss_c = inFlight_c >= maxCwnd;
    if (loss_c) {
      ssthresh = Math.max(2, Math.floor(cwnd_c / 2));
      cwnd_c = ssthresh;
    } else if (cwnd_c < ssthresh) {
      cwnd_c = Math.min(cwnd_c * 2, ssthresh);    // slow start
    } else {
      cwnd_c = Math.min(cwnd_c + 1, maxCwnd + 4); // congestion avoidance
    }

    // ── BBR ────────────────────────────────────────────────────────────────
    // Phase cycle: 8 rounds cruise, 1 round probe up (+25%), 1 round drain, then repeat
    // Every 30 rounds: probe_rtt (drain to 4 MSS for 1 round)
    const phaseCycle = t % 10;
    let gain = 1.0;
    if (t % 30 === 29)         { bbrPhase = 3; }     // probe_rtt
    else if (phaseCycle === 8)  { bbrPhase = 1; }     // probe_bw up
    else if (phaseCycle === 9)  { bbrPhase = 2; }     // probe_bw drain
    else                        { bbrPhase = 0; }     // steady cruise

    if (bbrPhase === 3)       { cwnd_b = 4;              gain = 0.5; }
    else if (bbrPhase === 1)  { cwnd_b = Math.round(bdpMss * 1.25); gain = 1.25; }
    else if (bbrPhase === 2)  { cwnd_b = Math.round(bdpMss * 0.75); gain = 0.75; }
    else                      { cwnd_b = bdpMss * 2;     gain = 1.0; } // cwnd = 2×BDP in cruise

    // BBR queue: only during probe_bw_up (brief burst), zero otherwise
    const qDepth_b = bbrPhase === 1 ? Math.round(bdpMss * 0.25) : 0;
    const rtt_b    = rtprop + (qDepth_b / Math.max(1, bdpMss)) * rtprop * 0.5;
    const tput_b   = Math.min(bwMbps, bwMbps * gain * (bbrPhase === 3 ? 0.1 : 1.0));

    data.push({
      t,
      // CUBIC
      cwnd_cubic:  Math.round(Math.min(inFlight_c, maxCwnd)),
      rtt_cubic:   Math.round(rtt_c * 10) / 10,
      tput_cubic:  Math.round(tput_c * 10) / 10,
      queue_cubic: qDepth_c,
      loss_cubic:  loss_c ? inFlight_c : null,
      // BBR
      cwnd_bbr:   Math.round(cwnd_b),
      rtt_bbr:    Math.round(rtt_b * 10) / 10,
      tput_bbr:   Math.round(tput_b * 10) / 10,
      queue_bbr:  qDepth_b,
      // Reference lines
      bdp:        bdpMss,
      maxBuf:     maxCwnd,
      linkRate:   bwMbps,
      propRtt:    rttMs,
    });
  }
  return { data, bdpMss, maxCwnd };
}

// ── Scenario presets ──────────────────────────────────────────────────────────
const SCENARIOS = [
  { id:"local",           label:"Local DC (Jumbo)", bwMbps:10000, rttMin:0.08, rttAvg:0.12, pktLoss:0,     mtu:9000, conns:8,  latency:5   },
  // Cloud scenarios (generic, typical for AWS/GCP/Azure)
  { id:"cloud_same_az",   label:"Cloud Same AZ/Zone", bwMbps:10000, rttMin:0.3, rttAvg:0.5, pktLoss:0,     mtu:9000, conns:8,  latency:5 },
  { id:"cloud_cross_az",  label:"Cloud Cross-AZ (same region)", bwMbps:5000, rttMin:1.5, rttAvg:2.5, pktLoss:0,   mtu:9000, conns:4,  latency:10 },
  { id:"cloud_cross_region",label:"Cloud Cross-Region", bwMbps:1000, rttMin:35, rttAvg:45, pktLoss:0.005, mtu:9000, conns:2,  latency:80 },
  { id:"cloud_internet",  label:"Cloud → Internet",  bwMbps:500,   rttMin:20,   rttAvg:30,   pktLoss:0.01,  mtu:1500, conns:2,  latency:50  },
  // Generic on-prem / standard MTU scenarios
  { id:"same_az",         label:"On-Prem Same DC",        bwMbps:1000,  rttMin:1,    rttAvg:2,    pktLoss:0,     mtu:1500, conns:8,  latency:10  },
  { id:"cross_az",        label:"On-Prem Cross-DC",       bwMbps:1000,  rttMin:8,    rttAvg:12,   pktLoss:0,     mtu:1500, conns:4,  latency:20  },
  { id:"cross_region",    label:"Cross-Region (WAN)",     bwMbps:500,   rttMin:55,   rttAvg:65,   pktLoss:0.01,  mtu:1500, conns:2,  latency:100 },
  { id:"multi_region",    label:"Multi-Region (Global)",  bwMbps:200,   rttMin:140,  rttAvg:155,  pktLoss:0.02,  mtu:1500, conns:2,  latency:250 },
  { id:"satellite",       label:"Satellite",              bwMbps:50,    rttMin:580,  rttAvg:620,  pktLoss:0.1,   mtu:1500, conns:1,  latency:900 },
  { id:"custom",          label:"Custom / Measured",      bwMbps:1000,  rttMin:5,    rttAvg:7,    pktLoss:0,     mtu:1500, conns:4,  latency:50  },
];

const KAFKA_DEFAULTS = {
  batchSize: 16384, lingerMs: 0, bufferMemory: 33554432,
  rmemMax: 212992, tcpRmem: "4096 87380 6291456",
  inflight: 5, acks: "1", compression: "none",
};

// ── UI primitives ─────────────────────────────────────────────────────────────
const Label = ({c=P.muted, children, ...p}) => (
  <span style={{color:c, fontSize:"0.78em", fontWeight:600,
    letterSpacing:"0.06em", textTransform:"uppercase", ...p}}>{children}</span>
);

const Card = ({children, style={}}) => (
  <div style={{background:P.panel, border:`1px solid ${P.border}`,
    borderRadius:10, padding:"18px 20px", ...style}}>{children}</div>
);

const StatBox = ({label, value, sub, color=P.accent, warn=false}) => (
  <div style={{background:P.panel2, border:`1px solid ${warn?P.yellow+"55":P.border}`,
    borderRadius:8, padding:"12px 14px"}}>
    <Label c={P.muted}>{label}</Label>
    <div style={{color, fontFamily:"'JetBrains Mono',monospace",
      fontWeight:800, fontSize:"1.15em", marginTop:4}}>{value}</div>
    {sub && <div style={{color:P.muted, fontSize:"0.75em", marginTop:2}}>{sub}</div>}
  </div>
);

const HelpIcon = ({text}) => {
  const [show, setShow] = useState(false);
  return (
    <span style={{position:"relative", display:"inline-block", marginLeft:6}}>
      <span
        onMouseEnter={()=>setShow(true)}
        onMouseLeave={()=>setShow(false)}
        style={{
          display:"inline-flex", alignItems:"center", justifyContent:"center",
          width:16, height:16, borderRadius:"50%",
          border:`1px solid ${P.muted}44`, color:P.muted,
          fontSize:"0.7em", cursor:"help", fontWeight:600
        }}>?</span>
      {show && (
        <div style={{
          position:"absolute", bottom:"calc(100% + 6px)", left:"50%",
          transform:"translateX(-50%)", zIndex:1000,
          background:P.panel, border:`1px solid ${P.border}`, borderRadius:6,
          padding:"8px 12px", minWidth:200, maxWidth:320,
          fontSize:"0.82em", lineHeight:1.5, color:P.text,
          boxShadow:"0 4px 12px rgba(0,0,0,0.3)", whiteSpace:"normal"
        }}>
          {text}
          <div style={{
            position:"absolute", top:"100%", left:"50%",
            transform:"translateX(-50%)",
            width:0, height:0,
            borderLeft:"6px solid transparent",
            borderRight:"6px solid transparent",
            borderTop:`6px solid ${P.border}`
          }} />
        </div>
      )}
    </span>
  );
};

const Slider = ({label, value, min, max, step=1, unit="", onChange, color=P.accent, help}) => {
  const [editing, setEditing] = useState(false);
  const [draft,   setDraft]   = useState("");

  const startEdit = () => {
    setDraft(String(value));
    setEditing(true);
  };

  const commitEdit = () => {
    const n = parseFloat(draft);
    if (!isNaN(n)) {
      onChange(Math.min(max, Math.max(min, n)));
    }
    setEditing(false);
  };

  const handleKey = e => {
    if (e.key === "Enter")  commitEdit();
    if (e.key === "Escape") setEditing(false);
  };

  return (
    <div style={{marginBottom:10}}>
      <div style={{display:"flex", justifyContent:"space-between",
        alignItems:"center", marginBottom:4}}>
        <div style={{display:"flex", alignItems:"center"}}>
          <Label c={P.muted}>{label}</Label>
          {help && <HelpIcon text={help} />}
        </div>
        {editing ? (
          <input
            autoFocus
            type="number"
            min={min} max={max} step={step}
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onBlur={commitEdit}
            onKeyDown={handleKey}
            style={{
              width: 90, textAlign:"right",
              background:"#0d1117",
              border:`1px solid ${color}`,
              borderRadius:4,
              color,
              fontFamily:"monospace", fontSize:"0.85em", fontWeight:700,
              padding:"1px 4px",
              outline:"none",
            }}
          />
        ) : (
          <span
            onClick={startEdit}
            title="Click to type a value"
            style={{
              color, fontFamily:"monospace", fontSize:"0.85em", fontWeight:700,
              cursor:"text",
              borderBottom:`1px dashed ${color}55`,
              paddingBottom:1,
              userSelect:"none",
            }}
          >
            {value}{unit}
          </span>
        )}
      </div>
      <input type="range" min={min} max={max} step={step} value={value}
        onChange={e => onChange(+e.target.value)}
        style={{width:"100%", accentColor:color, height:4}} />
    </div>
  );
};

const CodeBlock = ({children, title}) => (
  <div style={{marginTop:12}}>
    {title && <Label c={P.muted} style={{display:"block",marginBottom:6}}>{title}</Label>}
    <pre style={{background:"#06080e", border:`1px solid ${P.border}`,
      borderRadius:8, padding:"14px 16px", margin:0,
      fontFamily:"'JetBrains Mono',monospace", fontSize:"0.78em",
      color:P.cyan, overflowX:"auto", lineHeight:1.7, whiteSpace:"pre"}}>{children}</pre>
  </div>
);

const TabBtn = ({active, onClick, children}) => (
  <button onClick={onClick} style={{
    background: active ? P.accent+"22" : "transparent",
    color: active ? P.accent : P.muted,
    border: `1px solid ${active ? P.accent+"55" : P.border}`,
    borderRadius:6, padding:"5px 14px", fontSize:"0.8em", fontWeight:600,
    cursor:"pointer", transition:"all 0.15s", letterSpacing:"0.04em",
  }}>{children}</button>
);

const DiagBadge = ({type, children}) => {
  const colors = { ok:[P.green,"#0d2016"], warn:[P.yellow,"#1e1800"], err:[P.red,"#200c0c"] };
  const [fg, bg] = colors[type] || colors.ok;
  return (
    <div style={{background:bg, border:`1px solid ${fg}44`, borderRadius:7,
      padding:"8px 12px", marginBottom:8, fontSize:"0.82em",
      color:fg, display:"flex", gap:8, alignItems:"flex-start"}}>
      <span style={{flexShrink:0}}>{type==="ok"?"✓":type==="warn"?"⚠":"✗"}</span>
      <span>{children}</span>
    </div>
  );
};

// ── Scenario comparison table ─────────────────────────────────────────────────
const SCENARIO_TABLE = [
  { label:"Kafka default",      bw:"-",       rtt:"-",     bdp:"-",       batch:"16 KB",  rmem:"256 KB",  linger:"0",   cc:"cubic", acks:"1",  comp:"none"  },
  { label:"Local DC (<1ms)",    bw:"10 Gbps", rtt:"0.2ms", bdp:"~250 KB", batch:"128 KB", rmem:"32 MB",   linger:"5",   cc:"bbr",   acks:"all",comp:"lz4"   },
  { label:"Same-AZ (1-5ms)",    bw:"1 Gbps",  rtt:"5ms",   bdp:"~625 KB", batch:"128 KB", rmem:"128 MB",  linger:"10",  cc:"bbr",   acks:"all",comp:"lz4"   },
  { label:"Cross-AZ (5-20ms)",  bw:"1 Gbps",  rtt:"20ms",  bdp:"~2.5 MB", batch:"256 KB", rmem:"256 MB",  linger:"20",  cc:"bbr",   acks:"all",comp:"lz4"   },
  { label:"Cross-Region (60ms)","bw":"500 Mbps",rtt:"60ms", bdp:"~3.75 MB",batch:"512 KB", rmem:"512 MB",  linger:"50",  cc:"bbr",   acks:"all",comp:"lz4"   },
  { label:"Multi-Region (150ms)",bw:"200 Mbps",rtt:"150ms", bdp:"~3.75 MB",batch:"1 MB",   rmem:"1 GB",    linger:"100", cc:"bbr",   acks:"all",comp:"zstd"  },
  { label:"Satellite (600ms)",  bw:"50 Mbps", rtt:"600ms", bdp:"~3.75 MB",batch:"1 MB",   rmem:"2 GB",    linger:"500", cc:"bbr",   acks:"1",  comp:"zstd"  },
];
const TABLE_COLS = ["label","bw","rtt","bdp","batch","rmem","linger","cc","acks","comp"];
const TABLE_HEADS = ["Scenario","Bandwidth","RTT","BDP","batch.size","tcp_rmem_max","linger.ms (thru)","CC","acks","compression"];

// ── Main component ────────────────────────────────────────────────────────────
export default function App() {
  const [scenarioId, setScenarioId] = useState("local");
  // Per-chart log-Y toggle state
  const [logWindow,    setLogWindow]    = useState(false);  // window sweep
  const [logPartChart, setLogPartChart] = useState(false);  // partition chart
  const [logBbrCwnd,   setLogBbrCwnd]  = useState(false);  // BBR cwnd
  const [logBbrRtt,    setLogBbrRtt]   = useState(false);  // BBR RTT
  const [logBbrQueue,  setLogBbrQueue] = useState(false);  // BBR queue
  const [logBbrTput,   setLogBbrTput]  = useState(false);  // BBR throughput
  const [tab, setTab] = useState("overview");
  const [inflight, setInflight] = useState(5);
  const [partitions, setPartitions] = useState(12);
  const [brokers, setBrokers] = useState(3);
  const [compressionRatio, setCompressionRatio] = useState(2.5);
  const [replicationFactor, setReplicationFactor] = useState(3);
  const [producerCount, setProducerCount] = useState(3);
  const [consumerCount, setConsumerCount] = useState(2);
  const [expectedThroughputPct, setExpectedThroughputPct] = useState(20);
  const [expectedThroughputMbps, setExpectedThroughputMbps] = useState(2000);
  const [throughputMode, setThroughputMode] = useState('percent'); // 'percent' or 'absolute'

  const scen = SCENARIOS.find(s=>s.id===scenarioId) || SCENARIOS[0];

  const [custom, setCustom] = useState({
    bwMbps: scen.bwMbps, rttMin: scen.rttMin, rttAvg: scen.rttAvg,
    pktLoss: scen.pktLoss, mtu: scen.mtu, conns: scen.conns,
    latencyBudgetMs: scen.latency,
  });

  // Auto-derive plateau from BDP for simulation.
  // Use rttAvg (operating RTT) not rttMin (ping floor) — batch sizing must cover
  // the average in-flight window, not the theoretical minimum propagation delay.
  const simBDP = custom.bwMbps * 1e6 / 8 * custom.rttAvg / 1000;
  const plateauKB = Math.max(4, Math.ceil(simBDP / 1024));

  const calc = calcFromMeasurements({
    ...custom, plateauKB, inflight, partitions, compressionRatio, replicationFactor, brokers,
    producerCount, consumerCount, expectedThroughputPct, expectedThroughputMbps, throughputMode,
  });

  const sweepData = simWindowSweep(custom.bwMbps, custom.rttAvg);

  const applyScenario = useCallback((id) => {
    setScenarioId(id);
    const s = SCENARIOS.find(x=>x.id===id);
    if (s) setCustom({
      bwMbps: s.bwMbps, rttMin: s.rttMin, rttAvg: s.rttAvg,
      pktLoss: s.pktLoss, mtu: s.mtu, conns: s.conns,
      latencyBudgetMs: s.latency,
    });
  }, []);

  // Diagnoses
  const diag = [];
  if (calc.perBrokerUtilization > 100)
    diag.push({type:"err", msg:`Per-broker NIC limit exceeded (${calc.perBrokerUtilization}%)! At RF=${replicationFactor}, each broker handles ${calc.partitionsPerBroker} partitions with ${replicationFactor}× write amplification. Solution: add brokers (scale to ${Math.ceil(partitions * replicationFactor / custom.bwMbps * calc.effectiveMbps / partitions)}+), reduce RF, or reduce partition count.`});
  if (calc.perBrokerUtilization > 80 && calc.perBrokerUtilization <= 100)
    diag.push({type:"warn", msg:`Per-broker bandwidth utilization is high (${calc.perBrokerUtilization}%). Replication (RF=${replicationFactor}) consumes ${fmtMbps(calc.perBrokerReplicationOutMbps + calc.perBrokerReplicationInMbps)} per broker. Limited headroom for bursts.`});
  if (custom.pktLoss > 0.1)
    diag.push({type:"err", msg:`Packet loss ${custom.pktLoss}% → Mathis bound: ${calc.mathisMbps?calc.mathisMbps.toFixed(0):"N/A"} Mbps. Fix network before buffer tuning.`});
  if (custom.pktLoss > 0 && custom.pktLoss <= 0.1)
    diag.push({type:"warn", msg:`Low packet loss ${custom.pktLoss}% detected. BBR + lz4 will help. Monitor retransmits.`});
  if (calc.empiricalBDP > 500000)
    diag.push({type:"warn", msg:`High BDP path (${fmtBytes(calc.empiricalBDP)}). Default Kafka buffers (256 KB) will severely limit throughput.`});
  if (custom.mtu === 1500 && custom.bwMbps >= 10000)
    diag.push({type:"warn", msg:`10+ Gbps with standard MTU 1500 — consider jumbo frames (MTU 9000) on the Kafka VLAN for ~5× reduction in header overhead.`});
  if (custom.mtu > 1500 && custom.mtu !== 9000 && custom.mtu !== 9001 && custom.mtu !== 8896)
    diag.push({type:"warn", msg:`Non-standard MTU ${custom.mtu}. Most cloud jumbo frame paths use 9000 (AWS 9001, GCP 8896). Verify end-to-end path MTU with: tracepath <broker-ip>`});
  if (custom.mtu === 1500 && custom.bwMbps >= 1000) {
    const stdPackets = calc.packetsPerMB;
    const jumboPackets = Math.ceil(1048576 / 8960); // MSS at MTU 9000
    const reduction = Math.round((stdPackets / jumboPackets) * 10) / 10;
    diag.push({type:"info", msg:`At ${fmtMbps(custom.bwMbps)} with MTU 1500: ${stdPackets.toLocaleString()} packets/MB. Jumbo frames (MTU 9000) would reduce to ~${jumboPackets} packets/MB (~${reduction}× fewer interrupts).`});
  }
  if (calc.batchSize < calc.mss * 2)
    diag.push({type:"warn", msg:`batch.size (${fmtBytes(calc.batchSize)}) < 2×MSS (${fmtBytes(calc.mss * 2)}). Sub-frame batches waste MTU capacity — increase batch.size or reduce MTU.`});
  if (custom.rttAvg > 100)
    diag.push({type:"warn", msg:`High RTT (${custom.rttAvg}ms) — linger.ms should be tuned carefully. Batch accumulation time must exceed BDP drain time (${calc.lingerThru}ms).`});
  if (calc.lingerLatency === 0)
    diag.push({type:"warn", msg:`Latency budget (${custom.latencyBudgetMs}ms) ≤ RTT (${custom.rttAvg}ms) + broker overhead (~2ms). linger.ms set to 0 in latency profile — batching disabled, throughput will be reduced.`});
  if (diag.length === 0)
    diag.push({type:"ok", msg:"Path looks healthy. Buffer and batch tuning will have direct impact."});

  // Config generation
  const sysctlConf = `# Generated from measurements
# BDP: ${fmtBytes(calc.empiricalBDP)}  Connections: ${custom.conns}

net.core.rmem_max            = ${calc.bufCeil}
net.core.wmem_max            = ${calc.bufCeil}
net.ipv4.tcp_rmem            = 4096 1048576 ${calc.bufCeil}
net.ipv4.tcp_wmem            = 4096 1048576 ${calc.bufCeil}
net.ipv4.tcp_moderate_rcvbuf = 1
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc          = fq
net.ipv4.tcp_keepalive_time     = 30
net.ipv4.tcp_keepalive_intvl    = 5
net.ipv4.tcp_keepalive_probes   = 3`;

  const kafkaThruConf = `# THROUGHPUT profile
# BDP: ${fmtBytes(calc.empiricalBDP)}  BW: ${fmtMbps(custom.bwMbps)}  RTT: ${custom.rttAvg}ms

batch.size                            = ${calc.batchSize}
linger.ms                             = ${calc.lingerThru}
buffer.memory                         = ${calc.bufCeil * 2}${calc.bufCeil * 2 > 1073741824 ? `  # ⚠ >1 GB — verify producer -Xmx heap` : ""}
compression.type                      = lz4
max.in.flight.requests.per.connection = ${inflight}
acks                                  = all
enable.idempotence                    = true
send.buffer.bytes                     = ${calc.batchSize * 2}  # Fallback if OS tcp_wmem cannot be set`;

  const kafkaLatConf = `# LATENCY profile (budget: ${custom.latencyBudgetMs}ms)
# RTT: ${custom.rttAvg}ms → linger headroom: ${calc.lingerLatency}ms
# WARNING: acks=1 + enable.idempotence=false trades durability/ordering for latency.
# Use acks=all + enable.idempotence=true if message ordering or durability is required.

linger.ms                             = ${calc.lingerLatency}
compression.type                      = lz4
max.in.flight.requests.per.connection = 1
acks                                  = 1
enable.idempotence                    = false
request.timeout.ms                    = 5000
delivery.timeout.ms                   = 10000`;

  const brokerConf = `# Broker server.properties additions
# ── Network I/O buffers ────────────────────────────────────────────────────
# PRIMARY: Use OS-level TCP buffer tuning (see sysctl tab) — allows auto-tuning
# Fallback if OS-level tuning is not possible (uncomment):
#socket.send.buffer.bytes              = ${calc.bufCeil}
#socket.receive.buffer.bytes           = ${calc.bufCeil}

# Network threads: handles producer, consumer, replication, and inter-broker I/O
# Calculated: ${calc.perBrokerTotalConns} total connections / 50 per thread
# Minimum for replication + clients: (${brokers}-1) × ${calc.numReplicaFetchers} + 1 = ${Math.max(0, brokers - 1) * calc.numReplicaFetchers + 1}
num.network.threads                   = ${calc.numNetworkThreads}

# ── Replication (RF=${replicationFactor}, ${partitions} partitions, ${brokers} brokers) ───────
replica.fetch.max.bytes               = ${calc.replicaFetchMaxBytes}
# Fallback if OS-level tuning not possible (uncomment):
#replica.socket.receive.buffer.bytes   = ${calc.replicaSocketReceiveBuffer}

# Fetcher threads: ${calc.numReplicaFetchers} threads × ${Math.max(0, brokers - 1)} other brokers = ${calc.perBrokerReplicaFetcherConnsOut} outgoing connections
# Scaled up for high-RTT (${custom.rttAvg}ms) to avoid replication lag in MRC
num.replica.fetchers                  = ${calc.numReplicaFetchers}

replica.lag.time.max.ms               = ${calc.replicaLagTimeoutMs}
# Timeout = RTT×4 (${custom.rttAvg*4}ms) + fetch.max.wait (${calc.consumerFetchMaxWaitThru}ms) + 5000ms margin`;

  const measureScript = `#!/bin/bash
# 1. Start iperf3 server on broker:
#    iperf3 -s -D -p 5201

# 2. Run measurements (from producer host):
./kafka-tcp-measure.sh \\
  -t <broker-ip> \\
  -p 5201 \\
  -s ${custom.conns} \\
  -o ./results

# 3. Analyze and generate configs:
./kafka-tcp-analyze.sh \\
  -d ./results \\
  -c ${custom.conns} \\
  -m ${custom.mtu} \\
  -l ${custom.latencyBudgetMs}

# 4. Apply sysctl (broker + producer hosts):
sudo sysctl -p ./results/99-kafka-tcp.conf

# 5. Add to /etc/sysctl.d/ for persistence:
sudo cp ./results/99-kafka-tcp.conf /etc/sysctl.d/
sudo sysctl --system`;

  return (
    <div style={{background:P.bg, color:P.text, minHeight:"100vh",
      fontFamily:"'Inter','Segoe UI',sans-serif", maxWidth:960,
      margin:"0 auto", padding:"28px 18px 80px"}}>

      {/* Header */}
      <div style={{borderBottom:`1px solid ${P.border}`, paddingBottom:22, marginBottom:24}}>
        <div style={{display:"flex", gap:8, flexWrap:"wrap", marginBottom:10}}>
          {["Kafka","TCP","IP","Performance"].map(t => (
            <span key={t} style={{background:P.accent+"18", color:P.accent,
              border:`1px solid ${P.accent}33`, borderRadius:4,
              padding:"2px 9px", fontSize:"0.73em", fontWeight:700,
              letterSpacing:"0.05em", textTransform:"uppercase"}}>{t}</span>
          ))}
        </div>
        <h1 style={{margin:"0 0 8px", fontSize:"clamp(1.4em,3.5vw,2em)",
          fontWeight:800, letterSpacing:"-0.02em"}}>
          TCP Kafka Tuning<span style={{color:P.accent}}> Dashboard</span>
        </h1>
        <p style={{margin:0, color:P.muted, fontSize:"0.88em", maxWidth:620, lineHeight:1.6}}>
          Measurement-driven recommendations for TCP socket buffers, congestion control,
          and Kafka producer/broker settings. Select a scenario or enter your own measurements.
        </p>
      </div>

      {/* Scenario selector */}
      <div style={{marginBottom:20}}>
        <Label c={P.muted} style={{display:"block", marginBottom:10}}>Scenario preset</Label>
        <div style={{display:"flex", gap:8, flexWrap:"wrap"}}>
          {SCENARIOS.map(s => (
            <button key={s.id} onClick={()=>applyScenario(s.id)} style={{
              background: scenarioId===s.id ? P.accent+"22" : P.panel,
              color: scenarioId===s.id ? P.accent : P.muted,
              border:`1px solid ${scenarioId===s.id ? P.accent+"66":P.border}`,
              borderRadius:7, padding:"6px 14px", fontSize:"0.82em",
              fontWeight:600, cursor:"pointer", transition:"all 0.15s",
            }}>{s.label}</button>
          ))}
        </div>
      </div>

      {/* Two-column layout: sliders + stats */}
      <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:16, marginBottom:20}}>
        <Card>
          <Label c={P.muted} style={{display:"block", marginBottom:14}}>Path & Cluster Parameters</Label>
          <Slider label="Per-broker bandwidth limit" value={custom.bwMbps} min={10} max={50000} step={10}
            unit={custom.bwMbps>=1000?` (${(custom.bwMbps/1000).toFixed(custom.bwMbps%1000===0?0:1)} Gbps)`:" Mbps"}
            color={P.accent} onChange={v=>setCustom(c=>({...c,bwMbps:v}))}
            help="NIC bandwidth cap per broker node. In cloud (AWS/GCP/Azure), this is the per-VM network limit, not total cluster bandwidth. Each broker's NIC handles producer ingress, consumer egress, and replication traffic." />
          <Slider label="RTT min (ms)" value={custom.rttMin} min={0.05} max={700} step={0.05}
            unit=" ms" color={P.green} onChange={v=>setCustom(c=>({...c,rttMin:v}))}
            help="Minimum round-trip time observed on the path (from ping or iperf3). Used to calculate theoretical BDP (bandwidth × RTT). Lower = less latency, smaller buffers needed." />
          <Slider label="RTT avg (ms)" value={custom.rttAvg} min={0.1} max={700} step={0.1}
            unit=" ms" color={P.cyan} onChange={v=>setCustom(c=>({...c,rttAvg:v}))}
            help="Average round-trip time under load. Used for batch timing (linger.ms) and timeout calculations. Higher RTT requires larger buffers and longer batch accumulation time." />
          <Slider label="Packet loss" value={custom.pktLoss} min={0} max={5} step={0.01}
            unit="%" color={P.red} onChange={v=>setCustom(c=>({...c,pktLoss:v}))}
            help="Packet loss rate on the path. Limits throughput via Mathis equation: T ≤ MSS / (RTT × √loss). Even 0.1% loss can significantly reduce throughput. Fix network issues before tuning buffers." />
          <Slider label="MTU" value={custom.mtu} min={576} max={9000} step={1}
            unit=" bytes" color={P.yellow} onChange={v=>setCustom(c=>({...c,mtu:v}))}
            help="Maximum Transmission Unit. Standard Ethernet = 1500, Jumbo frames = 9000 (AWS 9001, GCP 8896). Larger MTU = less header overhead, fewer packets/MB. Only works on intra-VPC paths; internet egress uses 1500." />
          <Slider label="Parallel connections" value={custom.conns} min={1} max={32} step={1}
            unit="" color={P.purple} onChange={v=>setCustom(c=>({...c,conns:v}))}
            help="Number of concurrent TCP connections used during iperf3 measurement. Each connection has its own window. Total window = batch.size × inflight × connections. Used to calculate buffer ceiling." />
          <Slider label="Latency budget" value={custom.latencyBudgetMs} min={1} max={1000} step={1}
            unit=" ms" color={P.orange} onChange={v=>setCustom(c=>({...c,latencyBudgetMs:v}))}
            help="Maximum acceptable end-to-end latency for a message (producer → broker → consumer). Controls linger.ms ceiling: linger = budget - RTT - broker_overhead. Low budget = less batching = lower throughput." />
          <Slider label="Max in-flight requests" value={inflight} min={1} max={10} step={1}
            unit="" color={P.cyan} onChange={setInflight}
            help="max.in.flight.requests.per.connection — number of unacknowledged batches allowed per connection. Effective window = batch.size × inflight. Higher = better throughput but more memory, harder to maintain ordering." />
          <Slider label="Brokers in cluster" value={brokers} min={1} max={24} step={1}
            unit="" color={P.orange} onChange={setBrokers}
            help="Total number of broker nodes in the Kafka cluster. Partitions are distributed evenly across brokers. More brokers = less partitions/broker = less per-broker bandwidth usage." />
          <Slider label="Partitions (topic total)" value={partitions} min={1} max={256} step={1}
            unit={` (${calc.partitionsPerBroker}/broker)`} color={P.green} onChange={setPartitions}
            help="Total number of partitions in the topic. Distributed across brokers. Each partition = 1 TCP connection from producer. More partitions = higher parallelism but more connections and memory overhead." />
          <Slider label="Replication factor" value={replicationFactor} min={1} max={10} step={1}
            unit={` (RF=${replicationFactor})`} color={P.red} onChange={setReplicationFactor}
            help="Number of replicas per partition (including leader). RF=3 means 1 leader + 2 followers. Higher RF = more durability but multiplies write bandwidth: RF=3 → 3× write amplification on broker NICs." />
          <Slider label="Producer instances" value={producerCount} min={1} max={50} step={1}
            unit="" color={P.cyan} onChange={setProducerCount}
            help="Number of producer application instances. Each connects to brokers hosting its assigned partitions. Used to calculate per-broker connection count, not throughput (see Expected throughput)." />
          <Slider label="Consumer instances" value={consumerCount} min={1} max={50} step={1}
            unit="" color={P.orange} onChange={setConsumerCount}
            help="Number of consumer application instances. Each connects to brokers hosting partitions it consumes. Used to calculate per-broker connection count. Max useful = partition count (beyond that, extra consumers are idle)." />
          {/* Expected throughput with mode toggle */}
          <div>
            <div style={{display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:6}}>
              <Label c={P.muted}>Expected throughput</Label>
              <button
                onClick={() => {
                  if (throughputMode === 'percent') {
                    // Switching to absolute: sync absolute value from percentage
                    setExpectedThroughputMbps(Math.round(calc.expectedProducerMbps));
                    setThroughputMode('absolute');
                  } else {
                    // Switching to percent: sync percentage from absolute value
                    setExpectedThroughputPct(Math.round(expectedThroughputMbps / calc.effectiveMbps * 100));
                    setThroughputMode('percent');
                  }
                }}
                style={{
                  background: P.panel2, border: `1px solid ${P.border}`,
                  borderRadius: 6, padding: "4px 10px", fontSize: "0.75em",
                  color: P.accent, cursor: "pointer", fontWeight: 600,
                  transition: "all 0.15s"
                }}>
                {throughputMode === 'percent' ? '% → Mbps' : 'Mbps → %'}
              </button>
            </div>
            {throughputMode === 'percent' ? (
              <Slider value={expectedThroughputPct} min={1} max={100} step={1}
                unit={`% (≈ ${fmtMbps(calc.expectedProducerMbps)})`} color={P.yellow} onChange={setExpectedThroughputPct}
                help="TOTAL expected producer throughput across all partitions, as % of link capacity. Real workloads rarely saturate the link (20-30% is typical with RF=3, min.isr=2). Used to calculate per-broker bandwidth budget. 100% assumes producers will fully saturate the NIC (unrealistic)." />
            ) : (
              <Slider value={expectedThroughputMbps} min={1} max={custom.bwMbps}
                step={custom.bwMbps >= 10000 ? 100 : custom.bwMbps >= 1000 ? 10 : 1}
                unit={`Mbps (${Math.round(expectedThroughputMbps / calc.effectiveMbps * 100)}% of ${fmtMbps(calc.effectiveMbps)})`}
                color={P.yellow} onChange={setExpectedThroughputMbps}
                help="TOTAL expected producer throughput across all partitions, as absolute Mbps value. Specify directly if you know the expected workload rate from monitoring (e.g., 500 Mbps aggregate). Used to calculate per-broker bandwidth budget independently of link capacity." />
            )}
          </div>
          <Slider label="Compression ratio (lz4/zstd)" value={compressionRatio} min={1} max={6} step={0.1}
            unit={`× (${compressionRatio.toFixed(1)}×)`} color={P.purple} onChange={setCompressionRatio}
            help="Compression ratio achieved by codec (lz4/zstd). 2.5× = 1 MB payload compresses to 400 KB on wire. Higher ratio = less network traffic, more CPU. JSON/text compresses well (3-5×), binary/encrypted data compresses poorly (1-1.5×)." />
        </Card>

        <div style={{display:"flex", flexDirection:"column", gap:10}}>
          <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:10}}>
            <StatBox label="Theoretical BDP"  value={fmtBytes(calc.theoreticalBDP)} color={P.accent}
              sub="bandwidth × RTT_min" />
            <StatBox label="Empirical BDP"    value={fmtBytes(calc.empiricalBDP)} color={P.cyan}
              sub="plateau window (simulated)" />
            <StatBox label="TCP buffer ceil"  value={fmtBytes(calc.bufCeil)} color={P.green}
              sub={`BDP × ${custom.conns} conns × 2`} warn={calc.bufCeil > 536870912} />
            <StatBox label="MSS"              value={`${calc.mss} bytes`} color={P.yellow}
              sub={`MTU ${custom.mtu} − 40`} />
            <StatBox label="Kafka batch.size" value={fmtBytes(calc.batchSize)} color={P.purple}
              sub={`min ${fmtBytes(calc.batchMin)} (BDP÷inflight)`} />
            <StatBox label="linger.ms (thru)" value={`${calc.lingerThru} ms`} color={P.orange}
              sub="BDP drain time at measured BW" />
          </div>

          {/* Per-broker bandwidth breakdown */}
          <Card style={{padding:"12px 14px"}}>
            <Label c={P.muted} style={{marginBottom:8}}>Per-broker bandwidth budget (F18, F19)</Label>
            <div style={{display:"grid", gridTemplateColumns:"repeat(2, 1fr)", gap:8, marginBottom:10}}>
              <div style={{fontSize:"0.75em", color:P.muted}}>
                <span style={{color:P.accent}}>▸</span> Producer IN: {fmtMbps(calc.perBrokerProducerIngressMbps)}
              </div>
              <div style={{fontSize:"0.75em", color:P.muted}}>
                <span style={{color:P.green}}>▸</span> Consumer OUT: {fmtMbps(calc.perBrokerConsumerEgressMbps)}
              </div>
              <div style={{fontSize:"0.75em", color:P.muted}}>
                <span style={{color:P.yellow}}>▸</span> Replication OUT (leader→followers): {fmtMbps(calc.perBrokerReplicationOutMbps)}
              </div>
              <div style={{fontSize:"0.75em", color:P.muted}}>
                <span style={{color:P.cyan}}>▸</span> Replication IN (as follower): {fmtMbps(calc.perBrokerReplicationInMbps)}
              </div>
            </div>
            <div style={{borderTop:`1px solid ${P.border}`, paddingTop:8, display:"flex", justifyContent:"space-between", alignItems:"center"}}>
              <span style={{fontSize:"0.8em", color:P.text, fontWeight:600}}>Total per broker:</span>
              <span style={{fontSize:"0.9em", color: calc.perBrokerUtilization > 100 ? P.red : calc.perBrokerUtilization > 80 ? P.yellow : P.green, fontWeight:700, fontFamily:"monospace"}}>
                {fmtMbps(calc.perBrokerTotalMbps)} / {fmtMbps(custom.bwMbps)} ({calc.perBrokerUtilization}%)
              </span>
            </div>
            {calc.perBrokerUtilization > 100 && (
              <div style={{marginTop:8, padding:"6px 8px", background:P.red+"15", border:`1px solid ${P.red}44`, borderRadius:5, fontSize:"0.75em", color:P.red}}>
                ⚠ Per-broker NIC limit exceeded! Reduce partitions/broker, increase broker count, or reduce replication factor.
              </div>
            )}
            {calc.perBrokerUtilization > 80 && calc.perBrokerUtilization <= 100 && (
              <div style={{marginTop:8, padding:"6px 8px", background:P.yellow+"15", border:`1px solid ${P.yellow}44`, borderRadius:5, fontSize:"0.75em", color:P.yellow}}>
                ⚠ High utilization ({calc.perBrokerUtilization}%). Little headroom for traffic bursts.
              </div>
            )}
          </Card>

          <div style={{display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:10}}>
            <StatBox label={`Partitions per broker`}
              value={`${calc.partitionsPerBroker}`} color={P.green}
              sub={`${partitions} total ÷ ${brokers} brokers`} />
            <StatBox label={`Connections per broker`}
              value={`${calc.perBrokerTotalConns}`} color={P.cyan}
              sub={`${calc.perBrokerProducerConns} prod + ${calc.perBrokerConsumerConns} cons + ${calc.perBrokerReplicaFetcherConnsIn} repl`} />
            <StatBox label="Bottleneck"
              value={calc.bottleneck} color={calc.bottleneck.includes("NIC") ? P.red : P.accent}
              warn={calc.bottleneck.includes("NIC")}
              sub={calc.bottleneck.includes("NIC") ? "Scale brokers or reduce RF" : "See diagnostics"} />
          </div>

          {/* Diagnosis */}
          <div style={{background:P.panel, border:`1px solid ${P.border}`,
            borderRadius:10, padding:"14px 16px", flex:1}}>
            <Label c={P.muted} style={{display:"block", marginBottom:10}}>Diagnosis</Label>
            {diag.map((d,i) => <DiagBadge key={i} type={d.type}>{d.msg}</DiagBadge>)}
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div style={{display:"flex", gap:8, flexWrap:"wrap", marginBottom:16}}>
        {[
          ["overview","Overview"],["throughput","Throughput"],["bbr","BBR vs CUBIC"],
          ["mtu","MTU Impact"],["sysctl","sysctl"],
          ["kafka","Producer"],["consumer","Consumer"],["broker","Broker"],
          ["table","Scenarios"],["scripts","Scripts"],
        ].map(([id,lbl])=>(
          <TabBtn key={id} active={tab===id} onClick={()=>setTab(id)}>{lbl}</TabBtn>
        ))}
      </div>

      {/* Tab: Throughput estimates */}
      {tab === "throughput" && (
        <div style={{display:"grid", gap:16}}>

          {/* Summary row */}
          <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:10}}>
            {[
              {label:"Link ceiling",          value:fmtMbps(custom.bwMbps),               color:P.muted,  sub:"raw bandwidth"},
              {label:"Window-limited",         value:fmtMbps(calc.kafkaWindowMbps),         color:P.yellow, sub:"batch×inflight÷RTT (F1)"},
              {label:"Total wire throughput",  value:fmtMbps(calc.effectiveMbps),           color:P.accent, sub: calc.mathisMbps ? "Mathis-limited (loss)" : "all partitions combined"},
              {label:"Total app data rate",    value:fmtMbps(calc.effectiveLogicalMbps),    color:P.green,  sub:`wire × ${compressionRatio}× — partition-independent`},
              {label:`Per partition (${partitions}p)`, value:fmtMbps(calc.perPartWireMbps), color:P.cyan,   sub:`${fmtMbps(calc.perPartLogicalMbps)} app data`},
            ].map(({label,value,color,sub}) => (
              <div key={label} style={{background:P.panel, border:`1px solid ${color}33`,
                borderRadius:8, padding:"12px 14px"}}>
                <Label c={P.muted}>{label}</Label>
                <div style={{color, fontFamily:"monospace", fontWeight:800,
                  fontSize:"1.2em", marginTop:4}}>{value}</div>
                <div style={{color:P.muted, fontSize:"0.75em", marginTop:2}}>{sub}</div>
              </div>
            ))}
          </div>

          {/* Per-broker bandwidth model explanation */}
          <div style={{background:P.panel2, border:`1px solid ${P.border}`, borderRadius:8,
            padding:"10px 14px", fontSize:"0.8em", color:P.muted, lineHeight:1.6}}>
            <span style={{color:P.text, fontWeight:600}}>Critical: Per-broker bandwidth model (F18, F19)</span>
            <div style={{marginTop:6}}>
              The bandwidth slider represents the <strong style={{color:P.text}}>per-broker NIC limit</strong> (e.g., EC2 instance network cap),
              NOT total cluster bandwidth. Each broker's NIC handles ALL traffic: producer writes, consumer reads,
              AND replication in both directions.
            </div>
            <div style={{marginTop:6}}>
              <span style={{color:P.text, fontWeight:600}}>Replication amplification:</span> At RF={replicationFactor},
              each 1 MB producer write → 1 MB to leader + {replicationFactor-1} MB replication OUT (leader→followers) + replication IN (as follower for other partitions).
              Total = {replicationFactor}× write amplification on the leader broker's NIC.
            </div>
            <div style={{marginTop:6}}>
              With {brokers} brokers, {partitions} partitions: each broker handles ~{calc.partitionsPerBroker} leader partitions.
              Per-broker bandwidth = {fmtMbps(calc.perBrokerProducerIngressMbps)} (producer) + {fmtMbps(calc.perBrokerReplicationOutMbps)} (repl OUT)
              + {fmtMbps(calc.perBrokerReplicationInMbps)} (repl IN) + {fmtMbps(calc.perBrokerConsumerEgressMbps)} (consumer)
              = <strong style={{color: calc.perBrokerUtilization > 100 ? P.red : P.accent}}>{fmtMbps(calc.perBrokerTotalMbps)} ({calc.perBrokerUtilization}%)</strong>.
            </div>
            {calc.perBrokerUtilization > 100 && (
              <div style={{marginTop:6, color:P.red, fontWeight:600}}>
                ⚠ Bottleneck: per-broker NIC saturated! Scale to {Math.ceil(calc.perBrokerTotalMbps / custom.bwMbps * brokers)}+ brokers or reduce RF.
              </div>
            )}
          </div>

          {/* Throughput breakdown explanation */}
          <Card>
            <Label c={P.muted} style={{display:"block", marginBottom:12}}>How the estimate is built</Label>
            <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:10, marginBottom:12}}>
              {[
                {step:"1  Window limit (F1)",
                 formula:`batch.size × inflight × 8 / RTT`,
                 value:`${fmtMbps(calc.kafkaWindowMbps)}`,
                 color:P.yellow,
                 note:`${fmtBytes(calc.batchSize)} × ${inflight} in-flight ÷ ${custom.rttAvg} ms RTT`},
                {step:"2  Link ceiling",
                 formula:`measured bandwidth`,
                 value:fmtMbps(custom.bwMbps),
                 color:P.accent,
                 note:"Physical upper bound — neither layer can exceed this"},
                {step:"3  Wire throughput",
                 formula:`min(window limit, link ceiling)${calc.mathisMbps ? " capped by Mathis" : ""}`,
                 value:fmtMbps(calc.effectiveMbps),
                 color: calc.mathisMbps ? P.red : P.accent,
                 note: calc.mathisMbps
                   ? `Loss ${custom.pktLoss}% → Mathis bound ${fmtMbps(calc.mathisMbps)} (F4)`
                   : calc.kafkaWindowMbps < custom.bwMbps
                     ? "Window-limited — increase batch.size or inflight"
                     : "Link-limited — window fills the pipe"},
                {step:"4  Logical throughput",
                 formula:`wire × compression ratio`,
                 value:fmtMbps(calc.effectiveLogicalMbps),
                 color:P.green,
                 note:`${fmtMbps(calc.effectiveMbps)} × ${compressionRatio}× = application-layer data rate`},
              ].map(({step,formula,value,color,note}) => (
                <div key={step} style={{background:P.panel2, border:`1px solid ${color}33`,
                  borderRadius:8, padding:"10px 12px"}}>
                  <div style={{color, fontWeight:700, fontSize:"0.82em", marginBottom:4}}>{step}</div>
                  <div style={{fontFamily:"monospace", color:P.cyan, fontSize:"0.78em",
                    marginBottom:4}}>{formula}</div>
                  <div style={{color, fontWeight:800, fontSize:"1.1em",
                    fontFamily:"monospace", marginBottom:4}}>{value}</div>
                  <div style={{color:P.muted, fontSize:"0.75em", lineHeight:1.5}}>{note}</div>
                </div>
              ))}
            </div>

            {/* Bottleneck indicator */}
            {calc.kafkaWindowMbps < custom.bwMbps && !calc.mathisMbps && (
              <div style={{background:P.yellow+"15", border:`1px solid ${P.yellow}44`,
                borderRadius:7, padding:"8px 12px", fontSize:"0.82em", color:P.yellow}}>
                ⚠ Window-limited: Kafka effective window ({fmtBytes(calc.batchSize * inflight)}) is smaller than
                the available bandwidth would support. Increase <code>batch.size</code> to{" "}
                {fmtBytes(Math.ceil(custom.bwMbps * 1e6 / 8 * custom.rttAvg / 1000 / inflight))} or
                increase <code>max.in.flight.requests.per.connection</code>.
              </div>
            )}
            {calc.mathisMbps && (
              <div style={{background:P.red+"15", border:`1px solid ${P.red}44`,
                borderRadius:7, padding:"8px 12px", fontSize:"0.82em", color:P.red}}>
                ✗ Loss-limited (Mathis F4): {custom.pktLoss}% packet loss caps throughput
                at {fmtMbps(calc.mathisMbps)} regardless of window size.
                Fix packet loss before tuning buffers.
              </div>
            )}
          </Card>

          {/* Per-partition chart */}
          <Card>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
              <Label c={P.muted}>Throughput per partition vs partition count</Label>
              <LogToggle value={logPartChart} onChange={setLogPartChart} />
            </div>
            <div style={{color:P.muted, fontSize:"0.78em", marginBottom:12}}>
              Wire throughput per partition (solid) and app data rate per partition (dashed, higher due to compression).
              Both decrease as partitions increase — total capacity is fixed, partitions divide it.
              Reference line marks your current setting ({partitions} partitions).
            </div>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={calc.partitionSeries}
                margin={{top:4, right:20, bottom:20, left:10}}>
                <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                <XAxis dataKey="partitions" stroke={P.muted} tick={{fontSize:10}}
                  label={{value:"Partitions", position:"insideBottom", dy:14,
                    fill:P.muted, fontSize:11}} />
                <YAxis {...yAxisProps(logPartChart, 0.01, "Mbps per partition")} />
                <ReferenceLine x={partitions} stroke={P.accent} strokeDasharray="5 3"
                  label={{value:`${partitions}p`, fill:P.accent, fontSize:10, position:"top"}} />
                <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`,
                  borderRadius:8, fontSize:"0.8em"}}
                  formatter={(v,n) => [`${v} Mbps`, n]} />
                <Legend wrapperStyle={{fontSize:"0.8em", paddingTop:8}} />
                <Line type="monotone" dataKey="wireMbps" name="Wire / partition"
                  dot={false} strokeWidth={2.5} stroke={P.accent} />
                <Line type="monotone" dataKey="logicalMbps" name="App data / partition (after compression)"
                  dot={false} strokeWidth={1.5} stroke={P.green} strokeDasharray="5 3" />
              </LineChart>
            </ResponsiveContainer>
          </Card>

          {/* Per-partition table */}
          <Card style={{padding:0, overflow:"hidden"}}>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%", borderCollapse:"collapse", fontSize:"0.8em"}}>
                <thead>
                  <tr style={{background:P.panel2, borderBottom:`2px solid ${P.border}`}}>
                    {["Partitions","Wire / partition","App data / partition","BDP util / partition","Bottleneck"].map(h => (
                      <th key={h} style={{padding:"8px 12px", textAlign:"left",
                        color:P.muted, fontWeight:600, fontSize:"0.85em",
                        whiteSpace:"nowrap"}}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {calc.partitionSeries.filter(r => [1,2,4,8,12,16,24,32,64,128].includes(r.partitions)).map((row, i) => {
                    const bdpPct = Math.min(100, Math.round((calc.batchSize * inflight / calc.empiricalBDP) * 100));
                    const wirePerPart = row.wireMbps;
                    const bottleneck = calc.mathisMbps
                      ? "packet loss"
                      : calc.kafkaWindowMbps < custom.bwMbps
                        ? "window"
                        : "bandwidth";
                    const isCurrentPartitions = row.partitions === partitions;
                    return (
                      <tr key={row.partitions}
                        style={{
                          borderBottom:`1px solid ${P.border}`,
                          background: isCurrentPartitions ? P.accent+"18" : i%2===0 ? "transparent" : P.panel2,
                        }}>
                        <td style={{padding:"8px 12px", color: isCurrentPartitions ? P.accent : P.text,
                          fontWeight: isCurrentPartitions ? 700 : 400}}>
                          {row.partitions}{isCurrentPartitions ? " ◀ current" : ""}
                        </td>
                        <td style={{padding:"8px 12px", fontFamily:"monospace", color:P.accent}}>
                          {fmtMbps(wirePerPart)}
                        </td>
                        <td style={{padding:"8px 12px", fontFamily:"monospace", color:P.green}}>
                          {fmtMbps(row.logicalMbps)}
                        </td>
                        <td style={{padding:"8px 12px", fontFamily:"monospace",
                          color: bdpPct < 20 ? P.red : bdpPct < 60 ? P.yellow : P.green}}>
                          {bdpPct}%
                        </td>
                        <td style={{padding:"8px 12px", color:P.muted, fontSize:"0.9em"}}>
                          {bottleneck}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {/* Tab: BBR vs CUBIC */}
      {tab === "bbr" && (() => {
        const bufMss = 50;
        const { data, bdpMss, maxCwnd } = simBbrVsCubic(custom.bwMbps, custom.rttAvg, bufMss, calc.mss);

        const chartProps = {
          margin:{top:4, right:20, bottom:20, left:10},
        };
        const xAxis = <XAxis dataKey="t" stroke={P.muted} tick={{fontSize:10}}
          label={{value:"Round trips (RTT)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />;
        const grid  = <CartesianGrid strokeDasharray="3 3" stroke={P.border} />;
        const tip   = <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`,
          borderRadius:8, fontSize:"0.8em"}} />;

        // Summary stats
        const cubicAvgTput  = Math.round(data.reduce((s,d)=>s+d.tput_cubic,0)/data.length);
        const bbrAvgTput    = Math.round(data.reduce((s,d)=>s+d.tput_bbr,0)/data.length);
        const cubicAvgRtt   = (data.reduce((s,d)=>s+d.rtt_cubic,0)/data.length).toFixed(1);
        const bbrAvgRtt     = (data.reduce((s,d)=>s+d.rtt_bbr,0)/data.length).toFixed(1);
        const cubicAvgQueue = (data.reduce((s,d)=>s+d.queue_cubic,0)/data.length).toFixed(1);
        const bbrAvgQueue   = (data.reduce((s,d)=>s+d.queue_bbr,0)/data.length).toFixed(1);
        const lossEvents    = data.filter(d=>d.loss_cubic!==null).length;

        return (
          <div style={{display:"grid", gap:16}}>

            {/* Comparison summary */}
            <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:12}}>
              {[
                {algo:"CUBIC (default)", color:P.red,
                 stats:[
                   {k:"Avg throughput", v:`${cubicAvgTput} Mbps`},
                   {k:"Avg RTT",        v:`${cubicAvgRtt} ms`},
                   {k:"Avg queue depth",v:`${cubicAvgQueue} MSS`},
                   {k:"Loss events",    v:`${lossEvents} (required for signal)`},
                   {k:"Signal",         v:"Packet loss — must overflow buffer"},
                   {k:"fq qdisc needed",v:"No"},
                 ]},
                {algo:"BBR (recommended)", color:P.green,
                 stats:[
                   {k:"Avg throughput", v:`${bbrAvgTput} Mbps`},
                   {k:"Avg RTT",        v:`${bbrAvgRtt} ms`},
                   {k:"Avg queue depth",v:`${bbrAvgQueue} MSS`},
                   {k:"Loss events",    v:"0 (avoids loss)"},
                   {k:"Signal",         v:"BtlBw + RTprop model"},
                   {k:"fq qdisc needed",v:"Yes — mandatory for pacing"},
                 ]},
              ].map(({algo,color,stats}) => (
                <div key={algo} style={{background:P.panel, border:`1px solid ${color}44`,
                  borderRadius:10, padding:"16px 18px"}}>
                  <div style={{color, fontWeight:700, fontSize:"0.95em",
                    marginBottom:12}}>{algo}</div>
                  {stats.map(({k,v}) => (
                    <div key={k} style={{display:"flex", justifyContent:"space-between",
                      borderBottom:`1px solid ${P.border}`, padding:"5px 0",
                      fontSize:"0.82em"}}>
                      <span style={{color:P.muted}}>{k}</span>
                      <span style={{color:P.text, fontFamily:"monospace"}}>{v}</span>
                    </div>
                  ))}
                </div>
              ))}
            </div>

            {/* cwnd chart */}
            <Card>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                <Label c={P.muted}>Congestion window (cwnd) — MSS</Label>
                <LogToggle value={logBbrCwnd} onChange={setLogBbrCwnd} />
              </div>
              <div style={{color:P.muted, fontSize:"0.78em", marginBottom:10}}>
                CUBIC climbs exponentially, hits the buffer limit, drops by half — the sawtooth.
                BBR holds steady at 2×BDP in cruise, briefly probes at 1.25× every 8 RTTs.
                Red dots on CUBIC = loss event (required signal). BDP reference line shown.
              </div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={data} {...chartProps}>
                  {grid}{xAxis}{tip}
                  <YAxis stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"cwnd (MSS)", angle:-90, position:"insideLeft", dx:-6, fill:P.muted, fontSize:11}} />
                  <ReferenceLine y={bdpMss}  stroke={P.accent} strokeDasharray="4 3"
                    label={{value:"BDP", fill:P.accent, fontSize:10, position:"right"}} />
                  <ReferenceLine y={maxCwnd} stroke={P.red} strokeDasharray="2 4"
                    label={{value:"buffer limit", fill:P.red, fontSize:10, position:"right"}} />
                  <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
                  <Line type="monotone" dataKey="cwnd_cubic" name="CUBIC cwnd"
                    dot={false} strokeWidth={2} stroke={P.red} />
                  <Line type="monotone" dataKey="cwnd_bbr" name="BBR cwnd"
                    dot={false} strokeWidth={2} stroke={P.green} />
                  {/* Loss event markers */}
                  <Line type="monotone" dataKey="loss_cubic" name="CUBIC loss"
                    dot={{r:4, fill:P.red, stroke:P.red}}
                    activeDot={false} stroke="none" legendType="circle" />
                </LineChart>
              </ResponsiveContainer>
            </Card>

            {/* RTT chart */}
            <Card>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                <Label c={P.muted}>RTT observed by sender — ms</Label>
                <LogToggle value={logBbrRtt} onChange={setLogBbrRtt} />
              </div>
              <div style={{color:P.muted, fontSize:"0.78em", marginBottom:10}}>
                CUBIC fills the switch buffer before backing off — RTT inflates by
                queuing delay on top of propagation delay. BBR tracks RTprop (minimum RTT)
                and actively avoids adding queue, keeping RTT near the propagation baseline.
              </div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={data} {...chartProps}>
                  {grid}{xAxis}{tip}
                  <YAxis stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"RTT (ms)", angle:-90, position:"insideLeft", dx:-6, fill:P.muted, fontSize:11}} />
                  <ReferenceLine y={custom.rttAvg} stroke={P.accent} strokeDasharray="4 3"
                    label={{value:"RTprop", fill:P.accent, fontSize:10, position:"right"}} />
                  <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
                  <Line type="monotone" dataKey="rtt_cubic" name="CUBIC RTT"
                    dot={false} strokeWidth={2} stroke={P.red} />
                  <Line type="monotone" dataKey="rtt_bbr" name="BBR RTT"
                    dot={false} strokeWidth={2} stroke={P.green} />
                </LineChart>
              </ResponsiveContainer>
            </Card>

            {/* Queue depth chart */}
            <Card>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                <Label c={P.muted}>Switch buffer queue depth — MSS</Label>
                <LogToggle value={logBbrQueue} onChange={setLogBbrQueue} />
              </div>
              <div style={{color:P.muted, fontSize:"0.78em", marginBottom:10}}>
                CUBIC persistently fills the buffer — queue depth oscillates from 0 to the
                buffer limit. BBR targets zero queue in steady state; a brief queue spike
                appears only during the BW probe phase (every 8 RTTs, 1 RTT duration).
                Buffer bloat affects every flow sharing the switch, not just Kafka.
              </div>
              <ResponsiveContainer width="100%" height={180}>
                <AreaChart data={data} {...chartProps}>
                  {grid}{xAxis}{tip}
                  <YAxis stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"Queue (MSS)", angle:-90, position:"insideLeft", dx:-6, fill:P.muted, fontSize:11}} />
                  <defs>
                    <linearGradient id="qCubic" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%"  stopColor={P.red}   stopOpacity={0.3}/>
                      <stop offset="95%" stopColor={P.red}   stopOpacity={0}/>
                    </linearGradient>
                    <linearGradient id="qBbr" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%"  stopColor={P.green} stopOpacity={0.3}/>
                      <stop offset="95%" stopColor={P.green} stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
                  <Area type="monotone" dataKey="queue_cubic" name="CUBIC queue"
                    stroke={P.red}   fill="url(#qCubic)" strokeWidth={1.5} />
                  <Area type="monotone" dataKey="queue_bbr" name="BBR queue"
                    stroke={P.green} fill="url(#qBbr)"   strokeWidth={1.5} />
                </AreaChart>
              </ResponsiveContainer>
            </Card>

            {/* Throughput chart */}
            <Card>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                <Label c={P.muted}>Throughput — Mbps</Label>
                <LogToggle value={logBbrTput} onChange={setLogBbrTput} />
              </div>
              <div style={{color:P.muted, fontSize:"0.78em", marginBottom:10}}>
                CUBIC throughput oscillates with the sawtooth — it is always either growing
                toward the link rate or recovering from a loss event. BBR stays near the
                link rate continuously, with a brief dip during the RTT probe (~every 30 RTTs).
              </div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={data} {...chartProps}>
                  {grid}{xAxis}{tip}
                  <YAxis stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"Mbps", angle:-90, position:"insideLeft", dx:-6, fill:P.muted, fontSize:11}} />
                  <ReferenceLine y={custom.bwMbps} stroke={P.accent} strokeDasharray="4 3"
                    label={{value:"link rate", fill:P.accent, fontSize:10, position:"right"}} />
                  <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
                  <Line type="monotone" dataKey="tput_cubic" name="CUBIC"
                    dot={false} strokeWidth={2} stroke={P.red} />
                  <Line type="monotone" dataKey="tput_bbr" name="BBR"
                    dot={false} strokeWidth={2} stroke={P.green} />
                </LineChart>
              </ResponsiveContainer>
            </Card>

            {/* Behaviour table */}
            <Card>
              <Label c={P.muted} style={{display:"block", marginBottom:12}}>
                Behavioural comparison
              </Label>
              <div style={{overflowX:"auto"}}>
                <table style={{width:"100%", borderCollapse:"collapse", fontSize:"0.82em"}}>
                  <thead>
                    <tr style={{borderBottom:`2px solid ${P.border}`}}>
                      {["Property","CUBIC (default)","BBR","Impact on Kafka"].map(h => (
                        <th key={h} style={{padding:"7px 10px", textAlign:"left",
                          color:P.muted, fontWeight:600}}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      ["Congestion signal",   "Packet loss",                 "BtlBw + RTprop model",          "BBR never waits for a drop"],
                      ["Window behaviour",    "Sawtooth — grow, drop, repeat","Steady at BDP; brief probes",   "BBR throughput more stable"],
                      ["Queue depth",         "Fills buffer (bufferbloat)",   "Near zero in steady state",     "Lower p99 latency with BBR"],
                      ["RTT inflation",       "Up to 2–3× propagation RTT",  "Tracks propagation delay",       "linger.ms budget more reliable"],
                      ["After broker restart","Slow convergence (sawtooth)",  "Fast re-lock to BDP",           "BBR recovers partition leaders faster"],
                      ["Packet loss path",    "Cut window in half",           "Continues at BtlBw estimate",   "BBR tolerates random Wi-Fi loss better"],
                      ["fq qdisc required",   "No",                           "Yes — pacing needs fq",         "Must set default_qdisc=fq"],
                      ["High BDP paths",      "Under-utilises (window lag)",  "Self-calibrates to BDP",        "No manual rmem tuning needed with BBR"],
                      ["Many flows sharing",  "Fair via AIMD",                "Probe phases may cause bursts", "BBRv2 preferred at 50+ producers"],
                    ].map(([prop,cubic,bbr,impact], i) => (
                      <tr key={prop} style={{borderBottom:`1px solid ${P.border}`,
                        background: i%2===0 ? "transparent" : P.panel2}}>
                        <td style={{padding:"7px 10px", color:P.text, fontWeight:600}}>{prop}</td>
                        <td style={{padding:"7px 10px", color:P.red}}>{cubic}</td>
                        <td style={{padding:"7px 10px", color:P.green}}>{bbr}</td>
                        <td style={{padding:"7px 10px", color:P.muted, fontSize:"0.9em"}}>{impact}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>

          </div>
        );
      })()}

      {/* Tab: Overview */}
      {tab === "overview" && (
        <div>
          <Card style={{marginBottom:16}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
              <Label c={P.muted}>Throughput vs Window Size — simulated for your path</Label>
              <LogToggle value={logWindow} onChange={setLogWindow} />
            </div>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={sweepData} margin={{top:4,right:20,bottom:20,left:10}}>
                <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                <XAxis dataKey="win" stroke={P.muted} tick={{fontSize:10}}
                  tickFormatter={v=>v>=1024?`${v/1024}MB`:`${v}KB`}
                  label={{value:"Window (KB)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />
                <YAxis {...yAxisProps(logWindow, 0.1, "Throughput (Mbps)")} />
                <ReferenceLine x={plateauKB} stroke={P.green} strokeDasharray="5 3"
                  label={{value:"BDP plateau",fill:P.green,fontSize:10,position:"insideTopRight"}} />
                <ReferenceLine x={64} stroke={P.red} strokeDasharray="4 3"
                  label={{value:"64KB default",fill:P.red,fontSize:9,position:"insideTopLeft"}} />
                <Tooltip contentStyle={{background:P.panel,border:`1px solid ${P.border}`,
                  borderRadius:8,fontSize:"0.8em"}}
                  labelFormatter={v=>`Window: ${v>=1024?v/1024+"MB":v+"KB"}`}
                  formatter={v=>[`${v} Mbps`,"Throughput"]} />
                <Line type="monotone" dataKey="tput" dot={false} strokeWidth={2.5}
                  stroke={P.accent} name="Throughput" />
              </LineChart>
            </ResponsiveContainer>
          </Card>

          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:12}}>
              The tuning stack — how layers interact
            </Label>
            <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))", gap:10}}>
              {[
                {layer:"IP / MTU", color:P.yellow,
                 items:[`MTU: ${custom.mtu}B`,`MSS: ${calc.mss}B`,
                   custom.mtu<9000?"Jumbo frames available":"Jumbo frames active ✓"]},
                {layer:"TCP Window", color:P.accent,
                 items:[`BDP: ${fmtBytes(calc.empiricalBDP)}`,
                   `Buffer ceil: ${fmtBytes(calc.bufCeil)}`,
                   `Default rmem_max: 256 KB ← too small`]},
                {layer:"Congestion Ctrl", color:P.purple,
                 items:["CUBIC (default): loss-driven",
                   "BBR (recommended): model-driven",
                   "FQ qdisc: required with BBR"]},
                {layer:"Kafka Batching", color:P.green,
                 items:[`batch.size: ${fmtBytes(calc.batchSize)} (rec)`,
                   `Default 16 KB ← fills < 1 frame`,
                   `linger.ms: ${calc.lingerThru}ms (thru) / ${calc.lingerLatency}ms (lat)`]},
                {layer:"Compression", color:P.cyan,
                 items:["Default: none","lz4: 2-4× ratio, ~0.1ms latency",
                   "zstd: 3-5× ratio, +0.5ms, high-RTT"]},
              ].map(({layer,color,items}) => (
                <div key={layer} style={{background:P.panel2,
                  border:`1px solid ${color}33`, borderRadius:8, padding:"12px 14px"}}>
                  <div style={{color, fontWeight:700, fontSize:"0.85em", marginBottom:8}}>{layer}</div>
                  {items.map((item,i) => (
                    <div key={i} style={{color: item.includes("←")?P.yellow:P.muted,
                      fontSize:"0.78em", lineHeight:1.6}}>{item}</div>
                  ))}
                </div>
              ))}
            </div>
          </Card>
        </div>
      )}

      {/* Tab: MTU Impact Analysis */}
      {tab === "mtu" && (() => {
        // Data generator: packet efficiency at different MTUs
        const mtuData = [576, 1500, 9000].map(mtuVal => {
          const mssVal = mtuVal - 40;
          const headerOverhead = (40 / mtuVal * 100);
          const packetsPerMB = Math.ceil(1048576 / mssVal);
          return { mtu: mtuVal, headerOverhead, packetsPerMB };
        });

        // Data generator: throughput vs MTU for different RTT scenarios
        const throughputVsMtu = [];
        const rtts = [10, 50, 100]; // ms
        for (let mtuVal = 576; mtuVal <= 9000; mtuVal += (mtuVal < 1500 ? 100 : 500)) {
          const mssVal = mtuVal - 40;
          const point = { mtu: mtuVal };
          rtts.forEach(rtt => {
            // Throughput for single packet: T = (MSS × 8) / RTT
            point[`rtt${rtt}`] = Math.round((mssVal * 8) / (rtt / 1000) / 1e6 * 10) / 10;
          });
          throughputVsMtu.push(point);
        }

        return (
          <div style={{display:"grid", gap:16}}>

            {/* Packet Efficiency Chart */}
            <Card>
              <div style={{marginBottom:12}}>
                <Label c={P.muted}>Packet Efficiency vs MTU</Label>
                <div style={{color:P.muted, fontSize:"0.78em", marginTop:6}}>
                  Larger MTU → less header overhead → fewer packets → fewer interrupts.
                  Standard Ethernet (1500) vs Jumbo Frames (9000) vs Minimum (576).
                </div>
              </div>
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={mtuData}>
                  <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                  <XAxis dataKey="mtu" stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"MTU (bytes)", position:"insideBottom", dy:10, fill:P.muted, fontSize:11}} />
                  <YAxis yAxisId="left" stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"Header Overhead %", angle:-90, position:"insideLeft", dx:-8, fill:P.muted, fontSize:11}} />
                  <YAxis yAxisId="right" orientation="right" stroke={P.green} tick={{fontSize:10}}
                    label={{value:"Packets per MB", angle:90, position:"insideRight", dx:8, fill:P.green, fontSize:11}} />
                  <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:6, fontSize:"0.85em"}} />
                  <Bar yAxisId="left" dataKey="headerOverhead" fill={P.red} name="Header Overhead %" />
                  <Bar yAxisId="right" dataKey="packetsPerMB" fill={P.green} name="Packets/MB" />
                </BarChart>
              </ResponsiveContainer>
              <div style={{marginTop:12, padding:12, background:P.panel2, borderRadius:6, fontSize:"0.85em"}}>
                <strong style={{color:P.accent}}>Current MTU {custom.mtu}:</strong>
                <div style={{marginTop:6, color:P.muted}}>
                  • MSS: {calc.mss} bytes (MTU - 40)<br/>
                  • Header overhead: {calc.headerOverheadPct.toFixed(2)}%<br/>
                  • Packets per MB: {calc.packetsPerMB.toLocaleString()}<br/>
                  • Segments per Kafka batch ({fmtBytes(calc.batchSize)}): {calc.segmentsPerBatch}<br/>
                  • Kafka protocol overhead per batch: ~{fmtBytes(calc.kafkaOverheadPerBatch)} ({(100-calc.effectivePayloadRatio*100).toFixed(1)}% of batch)
                </div>
              </div>
            </Card>

            {/* Throughput vs MTU Chart */}
            <Card>
              <div style={{marginBottom:12}}>
                <Label c={P.muted}>Single-Packet Throughput vs MTU</Label>
                <div style={{color:P.muted, fontSize:"0.78em", marginTop:6}}>
                  For 1 Gbps link at different RTTs. Shows throughput ceiling imposed by MTU.
                  Formula: T = (MSS × 8) / RTT where MSS = MTU - 40
                </div>
              </div>
              <ResponsiveContainer width="100%" height={240}>
                <LineChart data={throughputVsMtu}>
                  <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                  <XAxis dataKey="mtu" stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"MTU (bytes)", position:"insideBottom", dy:10, fill:P.muted, fontSize:11}} />
                  <YAxis stroke={P.muted} tick={{fontSize:10}}
                    label={{value:"Throughput (Mbps)", angle:-90, position:"insideLeft", dx:-8, fill:P.muted, fontSize:11}} />
                  <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:6, fontSize:"0.85em"}} />
                  <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
                  <ReferenceLine x={1500} stroke={P.yellow} strokeDasharray="4 3"
                    label={{value:"Standard", fill:P.yellow, fontSize:10, position:"top"}} />
                  <ReferenceLine x={9000} stroke={P.green} strokeDasharray="4 3"
                    label={{value:"Jumbo", fill:P.green, fontSize:10, position:"top"}} />
                  <Line type="monotone" dataKey="rtt10" name="RTT 10ms" stroke={P.green} strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="rtt50" name="RTT 50ms" stroke={P.cyan} strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="rtt100" name="RTT 100ms" stroke={P.red} strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </Card>

            {/* Cloud Provider MTU Table */}
            <Card>
              <Label c={P.muted}>Cloud Provider MTU Limits</Label>
              <div style={{marginTop:12, overflowX:"auto"}}>
                <table style={{width:"100%", fontSize:"0.85em", borderCollapse:"collapse"}}>
                  <thead>
                    <tr style={{borderBottom:`2px solid ${P.border}`}}>
                      <th style={{padding:"8px", textAlign:"left", color:P.muted}}>Provider</th>
                      <th style={{padding:"8px", textAlign:"left", color:P.muted}}>Intra-VPC MTU</th>
                      <th style={{padding:"8px", textAlign:"left", color:P.muted}}>Internet Egress MTU</th>
                      <th style={{padding:"8px", textAlign:"left", color:P.muted}}>Notes</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr style={{borderBottom:`1px solid ${P.border}`}}>
                      <td style={{padding:"8px", color:P.text, fontWeight:600}}>AWS</td>
                      <td style={{padding:"8px", color:P.green, fontFamily:"monospace"}}>9001</td>
                      <td style={{padding:"8px", color:P.yellow, fontFamily:"monospace"}}>1500</td>
                      <td style={{padding:"8px", color:P.muted, fontSize:"0.9em"}}>Enhanced Networking required; check instance type</td>
                    </tr>
                    <tr style={{borderBottom:`1px solid ${P.border}`}}>
                      <td style={{padding:"8px", color:P.text, fontWeight:600}}>GCP</td>
                      <td style={{padding:"8px", color:P.green, fontFamily:"monospace"}}>8896</td>
                      <td style={{padding:"8px", color:P.yellow, fontFamily:"monospace"}}>1460</td>
                      <td style={{padding:"8px", color:P.muted, fontSize:"0.9em"}}>VPC default; external IP uses 1460 (GRE overhead)</td>
                    </tr>
                    <tr>
                      <td style={{padding:"8px", color:P.text, fontWeight:600}}>Azure</td>
                      <td style={{padding:"8px", color:P.green, fontFamily:"monospace"}}>9000</td>
                      <td style={{padding:"8px", color:P.yellow, fontFamily:"monospace"}}>1400</td>
                      <td style={{padding:"8px", color:P.muted, fontSize:"0.9em"}}>VNet default; internet uses 1400 (VXLAN overhead)</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div style={{marginTop:12, padding:10, background:P.bg, borderLeft:`3px solid ${P.yellow}`, fontSize:"0.85em"}}>
                ⚠ <strong>Jumbo frames work only on intra-VPC paths.</strong> Internet-bound traffic reverts to standard MTU.
                Hybrid deployments (VPC + internet consumers) must use min(MTU_vpc, MTU_internet) - 40 as effective MSS.
              </div>
            </Card>

            {/* Fragmentation Diagnostic */}
            <Card>
              <Label c={P.muted}>Path MTU Discovery (PMTUD) & Diagnostics</Label>
              <div style={{marginTop:12, fontSize:"0.85em", lineHeight:1.7, color:P.muted}}>
                <p>TCP relies on ICMP "Fragmentation Needed" (Type 3, Code 4) to discover path MTU.
                If firewalls block ICMP, you get a <strong style={{color:P.red}}>PMTUD black hole</strong>:
                handshakes succeed (small packets) but transfers stall (large segments silently drop).</p>

                <div style={{marginTop:12, padding:10, background:P.bg, borderRadius:6, fontFamily:"monospace", fontSize:"0.9em"}}>
                  # Detect path MTU to broker<br/>
                  tracepath broker.example.com<br/><br/>
                  # Capture negotiated MSS from active connection<br/>
                  ss -tin dst broker.example.com | grep mss<br/><br/>
                  # Force MSS clamping on tunnel interface<br/>
                  iptables -t mangle -A FORWARD -p tcp --tcp-flags SYN,RST SYN \<br/>
                  &nbsp;&nbsp;-j TCPMSS --clamp-mss-to-pmtu
                </div>
              </div>
            </Card>

            {/* Fragmentation Loss Amplification */}
            <Card>
              <Label c={P.muted}>Fragmentation Loss Amplification</Label>
              <div style={{marginTop:12, fontSize:"0.85em", lineHeight:1.7, color:P.muted}}>
                <p>When a packet exceeds path MTU and fragments into <em>N</em> fragments,
                losing <strong style={{color:P.red}}>any single fragment</strong> forces retransmission
                of the <em>entire original packet</em>. This creates multiplicative loss:</p>

                <div style={{marginTop:12, marginBottom:12, padding:12, background:P.panel2, borderRadius:6, fontFamily:"monospace", fontSize:"0.9em", color:P.text}}>
                  P<sub>effective</sub> = 1 − (1 − p)<sup>N</sup>&nbsp;&nbsp;&nbsp;where N = ⌈packet_size / path_MTU⌉
                </div>

                {(() => {
                  // Calculate fragmentation scenario
                  // Assume jumbo frame (9000) might fragment to 1500 on internet egress
                  const pathMtu = custom.mtu > 1500 ? 1500 : custom.mtu; // Assume internet path if jumbo configured
                  const largePacket = custom.mtu; // User's configured MTU
                  const fragmentCount = largePacket > pathMtu ? Math.ceil(largePacket / pathMtu) : 1;
                  const perFragmentLoss = custom.pktLoss;
                  const effectiveLoss = 1 - Math.pow(1 - perFragmentLoss, fragmentCount);
                  const lossAmplification = perFragmentLoss > 0 ? effectiveLoss / perFragmentLoss : 1;

                  // Mathis throughput degradation
                  const mathisBefore = calc.mss / (custom.rttAvg / 1000 * Math.sqrt(perFragmentLoss)) / 125000;
                  const mathisAfter = calc.mss / (custom.rttAvg / 1000 * Math.sqrt(effectiveLoss)) / 125000;
                  const throughputRatio = perFragmentLoss > 0 && effectiveLoss > 0 ? mathisBefore / mathisAfter : 1;

                  const hasFragmentation = fragmentCount > 1;

                  return (
                    <div style={{marginTop:12}}>
                      <div style={{display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:12, marginBottom:12}}>
                        <StatBox label="Configured MTU" value={custom.mtu} sub={`MSS: ${calc.mss}`} color={P.accent} />
                        <StatBox label="Assumed Path MTU" value={pathMtu}
                          sub={custom.mtu > 1500 ? "Internet egress" : "Same as configured"}
                          color={hasFragmentation ? P.yellow : P.green} />
                        <StatBox label="Fragments / Packet" value={fragmentCount}
                          color={hasFragmentation ? P.red : P.green}
                          warn={hasFragmentation} />
                      </div>

                      {hasFragmentation && perFragmentLoss > 0 && (
                        <div style={{padding:12, background:P.bg, borderLeft:`3px solid ${P.red}`, marginBottom:12}}>
                          <strong style={{color:P.red}}>⚠ Fragmentation detected!</strong>
                          <div style={{marginTop:8, color:P.muted}}>
                            • Per-fragment loss: {(perFragmentLoss * 100).toFixed(2)}%<br/>
                            • Effective packet loss: <strong style={{color:P.red}}>{(effectiveLoss * 100).toFixed(2)}%</strong> (amplification: {lossAmplification.toFixed(1)}×)<br/>
                            • Mathis throughput degradation: {throughputRatio.toFixed(2)}× slower<br/>
                            • Before fragmentation: {mathisBefore.toFixed(1)} Mbps → After: {mathisAfter.toFixed(1)} Mbps
                          </div>
                          <div style={{marginTop:12, color:P.accent}}>
                            <strong>Recommendation:</strong> Reduce MTU to {pathMtu} or use MSS clamping to prevent fragmentation.
                            Run <code style={{background:P.panel, padding:"2px 6px", borderRadius:3}}>tracepath</code> to
                            verify actual path MTU before deploying jumbo frames.
                          </div>
                        </div>
                      )}

                      {!hasFragmentation && (
                        <div style={{padding:12, background:P.bg, borderLeft:`3px solid ${P.green}`, marginBottom:12, color:P.green}}>
                          ✓ No fragmentation expected. MTU ≤ path MTU.
                        </div>
                      )}

                      {/* Loss amplification chart */}
                      <div style={{marginTop:16}}>
                        <div style={{color:P.muted, fontSize:"0.9em", marginBottom:8}}>
                          <strong>Loss Amplification by Fragment Count</strong> (current per-fragment loss: {(perFragmentLoss * 100).toFixed(2)}%)
                        </div>
                        <ResponsiveContainer width="100%" height={200}>
                          <BarChart data={(() => {
                            const fragCounts = [1, 2, 3, 5, 7, 10];
                            return fragCounts.map(n => {
                              const effLoss = 1 - Math.pow(1 - perFragmentLoss, n);
                              const amplification = perFragmentLoss > 0 ? effLoss / perFragmentLoss : 1;
                              return {
                                fragments: `${n} fragment${n > 1 ? 's' : ''}`,
                                effectiveLoss: parseFloat((effLoss * 100).toFixed(2)),
                                amplificationLabel: `${amplification.toFixed(1)}× amplification`,
                                isCurrent: n === fragmentCount
                              };
                            });
                          })()}>
                            <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
                            <XAxis dataKey="fragments" stroke={P.muted} tick={{fontSize:10}}
                              label={{value:"Fragments per Packet", position:"insideBottom", dy:10, fill:P.muted, fontSize:11}} />
                            <YAxis stroke={P.muted} tick={{fontSize:10}}
                              label={{value:"Effective Packet Loss %", angle:-90, position:"insideLeft", dx:-8, fill:P.muted, fontSize:11}} />
                            <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:6, fontSize:"0.85em"}}
                              formatter={(value, name, props) => {
                                if (name === "effectiveLoss") {
                                  return [
                                    <div key="tooltip">
                                      <div><strong>{value}%</strong> effective packet loss</div>
                                      <div style={{fontSize:"0.9em", marginTop:4, opacity:0.8}}>
                                        {props.payload.amplificationLabel}
                                      </div>
                                    </div>,
                                    ""
                                  ];
                                }
                                return [value, name];
                              }} />
                            <Bar dataKey="effectiveLoss" fill={P.red} name="Effective Loss %">
                              {(() => {
                                const data = [1, 2, 3, 5, 7, 10].map(n => ({
                                  fragments: n,
                                  effectiveLoss: (1 - Math.pow(1 - perFragmentLoss, n)) * 100,
                                  isCurrent: n === fragmentCount
                                }));
                                return data.map((entry, index) => (
                                  <Cell key={`cell-${index}`} fill={entry.isCurrent ? P.yellow : P.red} />
                                ));
                              })()}
                            </Bar>
                          </BarChart>
                        </ResponsiveContainer>
                      </div>

                      <div style={{marginTop:16, fontSize:"0.85em", lineHeight:1.7, color:P.muted}}>
                        <strong style={{color:P.text}}>Why fragmentation + loss is catastrophic:</strong>
                        <ul style={{marginTop:6, marginLeft:20}}>
                          <li>Losing 1 of 7 fragments → entire 9KB packet lost and retransmitted</li>
                          <li>Reassembly timeout (30-60s) adds severe latency spikes</li>
                          <li>Out-of-order fragments trigger unnecessary TCP retransmissions</li>
                          <li>Many firewalls drop fragmented packets entirely for security</li>
                          <li>Combines with Mathis equation to create {throughputRatio.toFixed(1)}× throughput reduction</li>
                        </ul>
                      </div>
                    </div>
                  );
                })()}
              </div>
            </Card>

          </div>
        );
      })()}

      {/* Tab: sysctl */}
      {tab === "sysctl" && (
        <Card>
          <div style={{display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:4}}>
            <Label c={P.muted}>Linux TCP — /etc/sysctl.d/99-kafka-tcp.conf</Label>
            <span style={{color:P.muted, fontSize:"0.76em"}}>apply: sudo sysctl -p /etc/sysctl.d/99-kafka-tcp.conf</span>
          </div>
          <CodeBlock>{sysctlConf}</CodeBlock>
          <div style={{marginTop:16, display:"grid", gridTemplateColumns:"1fr 1fr", gap:10}}>
            {[
              {k:"net.core.rmem_max",        dflt:"212992 (208KB)",    rec:fmtBytes(calc.bufCeil), why:"Ceiling for receive socket buffers per connection"},
              {k:"net.ipv4.tcp_rmem (max)",  dflt:"6291456 (6MB)",     rec:fmtBytes(calc.bufCeil), why:"Kernel autotuning upper bound"},
              {k:"tcp_congestion_control",   dflt:"cubic",             rec:"bbr",                  why:"Model-based, no loss required as signal"},
              {k:"default_qdisc",            dflt:"pfifo_fast",        rec:"fq",                   why:"Required pairing for BBR pacing"},
            ].map(({k,dflt,rec,why}) => (
              <div key={k} style={{background:P.panel2,border:`1px solid ${P.border}`,borderRadius:8,padding:"10px 12px"}}>
                <div style={{fontFamily:"monospace",color:P.cyan,fontSize:"0.8em",marginBottom:4}}>{k}</div>
                <div style={{display:"flex",gap:12,marginBottom:4}}>
                  <span style={{color:P.red,fontSize:"0.75em"}}>default: {dflt}</span>
                  <span style={{color:P.green,fontSize:"0.75em"}}>→ {rec}</span>
                </div>
                <div style={{color:P.muted,fontSize:"0.73em"}}>{why}</div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Tab: Kafka producer props */}
      {tab === "kafka" && (
        <div style={{display:"grid", gap:16}}>
          <Card>
            <Label c={P.green} style={{display:"block",marginBottom:4}}>Throughput Profile</Label>
            <div style={{color:P.muted,fontSize:"0.8em",marginBottom:8}}>
              Maximise bytes-per-second. Use when latency budget &gt; {calc.lingerThru + 5}ms.
            </div>
            <CodeBlock>{kafkaThruConf}</CodeBlock>
          </Card>
          <Card>
            <Label c={P.yellow} style={{display:"block",marginBottom:4}}>Latency Profile</Label>
            <div style={{color:P.muted,fontSize:"0.8em",marginBottom:8}}>
              Budget: {custom.latencyBudgetMs}ms. linger.ms = budget − RTT({custom.rttAvg}ms) − broker(~2ms) = {calc.lingerLatency}ms.
            </div>
            <CodeBlock>{kafkaLatConf}</CodeBlock>
          </Card>
          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:10}}>Key parameter interactions</Label>
            <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))", gap:10}}>
              {[
                {param:"batch.size",       dflt:"16 KB",  rec:fmtBytes(calc.batchSize), color:P.purple,
                 note:`Min = BDP÷inflight = ${fmtBytes(calc.batchMin)}. Larger = better compression ratio.`},
                {param:"linger.ms",        dflt:"0",      rec:`${calc.lingerThru}ms (thru) / ${calc.lingerLatency}ms (lat)`, color:P.orange,
                 note:"Time to accumulate batch. BDP drain time at measured BW."},
                {param:"compression.type", dflt:"none",   rec:"lz4 (always)",    color:P.cyan,
                 note:"Compresses entire batch. Larger batch → better ratio."},
                {param:"max.in.flight",    dflt:"5",      rec:`${inflight} (tunable)`, color:P.accent,
                 note:"Pipeline depth. Effective window = batch.size × inflight."},
                {param:"buffer.memory",    dflt:"32 MB",  rec:fmtBytes(calc.bufCeil*2), color:P.green,
                 note:`Total producer buffer. Must cover all pending batches.${calc.bufCeil*2 > 1073741824 ? " ⚠ Exceeds 1 GB — verify producer host has sufficient heap (-Xmx)." : ""}`},
                {param:"acks",             dflt:"1",      rec:"all (idempotent)", color:P.yellow,
                 note:"'all' requires min.insync.replicas=2. Latency +RTT_broker."},
              ].map(({param,dflt,rec,color,note}) => (
                <div key={param} style={{background:P.panel2,border:`1px solid ${color}33`,borderRadius:8,padding:"10px 12px"}}>
                  <div style={{fontFamily:"monospace",color,fontSize:"0.82em",fontWeight:700,marginBottom:4}}>{param}</div>
                  <div style={{display:"flex",gap:10,marginBottom:4,flexWrap:"wrap"}}>
                    <span style={{color:P.red,fontSize:"0.75em"}}>default: {dflt}</span>
                    <span style={{color:P.green,fontSize:"0.75em"}}>→ {rec}</span>
                  </div>
                  <div style={{color:P.muted,fontSize:"0.73em",lineHeight:1.5}}>{note}</div>
                </div>
              ))}
            </div>
          </Card>
        </div>
      )}

      {/* Tab: Consumer */}
      {tab === "consumer" && (
        <div style={{display:"grid", gap:16}}>
          <Card>
            <Label c={P.green} style={{display:"block",marginBottom:4}}>Consumer Configuration</Label>
            <div style={{color:P.muted,fontSize:"0.8em",marginBottom:8}}>
              Consumer fetch settings control the tradeoff between throughput (batching) and latency.
              Similar to producer linger.ms, but on the receive side.
            </div>
            <CodeBlock>{`# Consumer configuration (add to consumer.properties)
# ── Throughput profile ─────────────────────────────────────────────────────
fetch.min.bytes                       = ${calc.consumerFetchMinBytes}
fetch.max.wait.ms                     = ${calc.consumerFetchMaxWaitThru}
max.partition.fetch.bytes             = ${calc.consumerFetchMaxBytes}
receive.buffer.bytes                  = ${calc.consumerReceiveBuffer}

# Reasoning (F13, F14):
# - max.partition.fetch.bytes ≥ producer batch.size (${fmtBytes(calc.batchSize)})
#   ensures consumer can receive full producer batches without fragmenting
# - fetch.min.bytes = batch.size/2 balances batching vs latency
# - fetch.max.wait.ms = ${calc.consumerFetchMaxWaitThru}ms matches producer linger for throughput
# - receive.buffer.bytes = ${fmtBytes(calc.consumerReceiveBuffer)} covers BDP×conns×2

# ── Latency profile (minimize wait time) ───────────────────────────────────
fetch.max.wait.ms                     = ${calc.consumerFetchMaxWaitLat}
max.partition.fetch.bytes             = ${calc.consumerFetchMaxBytes}
receive.buffer.bytes                  = ${calc.consumerReceiveBuffer}
# Latency budget: ${custom.latencyBudgetMs}ms → fetch.max.wait ≤ ${calc.consumerFetchMaxWaitLat}ms`}</CodeBlock>
          </Card>

          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:10}}>Consumer fetch parameters explained</Label>
            <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(240px,1fr))", gap:10}}>
              {[
                {param:"max.partition.fetch.bytes", dflt:"1 MB", rec:fmtBytes(calc.consumerFetchMaxBytes), color:P.purple,
                 note:`Must be ≥ producer batch.size (${fmtBytes(calc.batchSize)}) to avoid fetch fragmentation. Per partition.`},
                {param:"fetch.min.bytes",        dflt:"1 byte",  rec:fmtBytes(calc.consumerFetchMinBytes), color:P.orange,
                 note:"Broker waits until this much data available or fetch.max.wait.ms expires. Higher = more batching."},
                {param:"fetch.max.wait.ms",      dflt:"500 ms",  rec:`${calc.consumerFetchMaxWaitThru}ms (thru) / ${calc.consumerFetchMaxWaitLat}ms (lat)`, color:P.cyan,
                 note:"Max time broker waits to fill fetch.min.bytes. Symmetric to producer linger.ms (F14)."},
                {param:"receive.buffer.bytes",   dflt:"64 KB",   rec:fmtBytes(calc.consumerReceiveBuffer), color:P.green,
                 note:`TCP receive buffer, must be ≥ BDP (${fmtBytes(calc.empiricalBDP)}). Defers to OS if set to -1.`},
              ].map(({param,dflt,rec,color,note}) => (
                <div key={param} style={{background:P.panel2,border:`1px solid ${color}33`,borderRadius:8,padding:"10px 12px"}}>
                  <div style={{fontFamily:"monospace",color,fontSize:"0.82em",fontWeight:700,marginBottom:4}}>{param}</div>
                  <div style={{display:"flex",gap:10,marginBottom:4,flexWrap:"wrap"}}>
                    <span style={{color:P.red,fontSize:"0.75em"}}>default: {dflt}</span>
                    <span style={{color:P.green,fontSize:"0.75em"}}>→ {rec}</span>
                  </div>
                  <div style={{color:P.muted,fontSize:"0.73em",lineHeight:1.5}}>{note}</div>
                </div>
              ))}
            </div>
          </Card>

          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:10}}>Throughput-Latency Tradeoff (F14)</Label>
            <div style={{color:P.muted, fontSize:"0.82em", lineHeight:1.7}}>
              <div style={{marginBottom:8}}>
                <span style={{color:P.text, fontWeight:600}}>Throughput profile:</span> fetch.min.bytes
                = {fmtBytes(calc.consumerFetchMinBytes)}, fetch.max.wait.ms = {calc.consumerFetchMaxWaitThru}ms.
                Broker accumulates data for up to {calc.consumerFetchMaxWaitThru}ms before responding,
                improving batching and reducing CPU overhead. Best when latency budget &gt; {calc.consumerFetchMaxWaitThru + custom.rttAvg}ms.
              </div>
              <div style={{marginBottom:8}}>
                <span style={{color:P.text, fontWeight:600}}>Latency profile:</span> fetch.min.bytes = 1,
                fetch.max.wait.ms = {calc.consumerFetchMaxWaitLat}ms. Broker responds immediately when any data
                is available (min 1 byte), minimizing wait time. Use when end-to-end latency SLA
                is tight ({custom.latencyBudgetMs}ms budget).
              </div>
              <div style={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:6,padding:"8px 10px",marginTop:10}}>
                <span style={{color:P.accent,fontWeight:600}}>Symmetric to producer:</span> Producer has
                linger.ms ({calc.lingerThru}ms thru / {calc.lingerLatency}ms lat). Consumer has fetch.max.wait.ms
                ({calc.consumerFetchMaxWaitThru}ms thru / {calc.consumerFetchMaxWaitLat}ms lat).
                Both control the batch-vs-latency tradeoff at their respective ends of the pipeline.
              </div>
            </div>
          </Card>
        </div>
      )}

      {/* Tab: Broker */}
      {tab === "broker" && (
        <div style={{display:"grid", gap:16}}>
          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:4}}>Broker server.properties additions</Label>
            <div style={{color:P.muted,fontSize:"0.8em",marginBottom:8}}>
              Restart broker after applying. Test replication throughput with kafka-producer-perf-test.sh.
            </div>
            <CodeBlock>{brokerConf}</CodeBlock>
          </Card>

          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:10}}>Broker configuration explained</Label>
            <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(240px,1fr))", gap:10}}>
              {[
                {param:"num.network.threads",     dflt:"3",     rec:`${calc.numNetworkThreads}`, color:P.accent,
                 note:`Handles all I/O: ${calc.perBrokerTotalConns} total connections (${calc.perBrokerProducerConns} prod + ${calc.perBrokerConsumerConns} cons + ${calc.perBrokerReplicaFetcherConnsIn + calc.perBrokerReplicaFetcherConnsOut} repl). Rule: 1 per ~50 conns OR min (${brokers}-1)×${calc.numReplicaFetchers}+1 for repl+clients.`},
                {param:"replica.fetch.max.bytes", dflt:"1 MB", rec:fmtBytes(calc.replicaFetchMaxBytes), color:P.purple,
                 note:`Should be ≥ producer batch.size (${fmtBytes(calc.batchSize)}) so replicas fetch complete batches.`},
                {param:"num.replica.fetchers",    dflt:"1",     rec:`${calc.numReplicaFetchers}`, color:P.cyan,
                 note:`Base: 1 fetcher per ~6 partitions (${partitions} ÷ 6). Scaled to ${calc.numReplicaFetchers} for RTT=${custom.rttAvg}ms. Each broker: ${calc.numReplicaFetchers} threads × ${Math.max(0, brokers - 1)} other brokers = ${calc.perBrokerReplicaFetcherConnsOut} connections.`},
                {param:"replica.lag.time.max.ms", dflt:"10000", rec:`${calc.replicaLagTimeoutMs} ms`, color:P.orange,
                 note:`Timeout before replica considered out-of-sync. Formula (F15): RTT×4 + fetch.max.wait + 5000ms margin.`},
                {param:"replica.socket.receive.buffer.bytes", dflt:"-1 (OS)", rec:fmtBytes(calc.replicaSocketReceiveBuffer), color:P.green,
                 note:`Follower receive buffer when fetching from leader. Must cover BDP (${fmtBytes(calc.empiricalBDP)}).`},
              ].map(({param,dflt,rec,color,note}) => (
                <div key={param} style={{background:P.panel2,border:`1px solid ${color}33`,borderRadius:8,padding:"10px 12px"}}>
                  <div style={{fontFamily:"monospace",color,fontSize:"0.82em",fontWeight:700,marginBottom:4}}>{param}</div>
                  <div style={{display:"flex",gap:10,marginBottom:4,flexWrap:"wrap"}}>
                    <span style={{color:P.red,fontSize:"0.75em"}}>default: {dflt}</span>
                    <span style={{color:P.green,fontSize:"0.75em"}}>→ {rec}</span>
                  </div>
                  <div style={{color:P.muted,fontSize:"0.73em",lineHeight:1.5}}>{note}</div>
                </div>
              ))}
            </div>
          </Card>

          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:10}}>Replication bandwidth analysis (per-broker NIC model)</Label>
            <div style={{display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:10, marginBottom:12}}>
              <StatBox label="Replication factor" value={`RF=${replicationFactor}`} color={P.red}
                sub={`${replicationFactor}× write amplification`} />
              <StatBox label="Per-broker utilization" value={`${calc.perBrokerUtilization}%`}
                color={calc.perBrokerUtilization > 100 ? P.red : calc.perBrokerUtilization > 80 ? P.yellow : P.green}
                warn={calc.perBrokerUtilization > 80}
                sub={`${fmtMbps(calc.perBrokerTotalMbps)} / ${fmtMbps(custom.bwMbps)} limit`} />
              <StatBox label="Cluster-wide repl BW" value={fmtMbps(calc.replicationWireMbps)} color={P.cyan}
                sub={`${calc.totalReplicaConnections} follower connections`} />
            </div>
            <div style={{color:P.muted, fontSize:"0.82em", lineHeight:1.7}}>
              <div style={{marginBottom:8}}>
                <span style={{color:P.text, fontWeight:600}}>Per-broker bandwidth breakdown (F18, F19):</span>
                Each broker node's NIC must handle producer writes, consumer reads, AND replication in BOTH directions.
                At {brokers} brokers with {partitions} partitions evenly distributed:
              </div>
              <ul style={{marginLeft:16, marginBottom:8, marginTop:4}}>
                <li>Leader partitions/broker: {calc.leaderPartitionsPerBroker}</li>
                <li>Follower partitions/broker: {calc.followerPartitionsPerBroker}</li>
                <li>Producer IN: {fmtMbps(calc.perBrokerProducerIngressMbps)} (writes to this broker's leaders)</li>
                <li>Replication OUT: {fmtMbps(calc.perBrokerReplicationOutMbps)} (leader → {replicationFactor-1} followers, {replicationFactor-1}× amplification)</li>
                <li>Replication IN: {fmtMbps(calc.perBrokerReplicationInMbps)} (as follower ← other leaders)</li>
                <li>Consumer OUT: {fmtMbps(calc.perBrokerConsumerEgressMbps)} (reads from this broker's leaders)</li>
                <li><strong>Total: {fmtMbps(calc.perBrokerTotalMbps)} ({calc.perBrokerUtilization}% of {fmtMbps(custom.bwMbps)} NIC limit)</strong></li>
              </ul>
              <div style={{marginBottom:8}}>
                <span style={{color:P.text, fontWeight:600}}>Effective replication factor impact:</span>
                RF={replicationFactor} means each producer write is replicated {replicationFactor-1} times.
                Leader brokers experience {replicationFactor}× write amplification: 1× producer ingress + {replicationFactor-1}× replication egress.
                This is why per-broker bandwidth grows with RF even if producer throughput stays constant.
              </div>
              <div style={{marginBottom:8}}>
                <span style={{color:P.text, fontWeight:600}}>Timeout calculation (F15):</span> replica.lag.time.max.ms
                = {calc.replicaLagTimeoutMs}ms accounts for worst-case fetch cycle: RTT to leader ({custom.rttAvg}ms),
                broker waits up to fetch.max.wait.ms ({calc.consumerFetchMaxWaitThru}ms) to accumulate data,
                RTT back to follower ({custom.rttAvg}ms), plus processing overhead and a 5000ms safety margin
                for GC pauses. Total = {custom.rttAvg}×2 + {calc.consumerFetchMaxWaitThru} + 5000 + headroom ≈ {calc.replicaLagTimeoutMs}ms.
              </div>
              <div style={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:6,padding:"8px 10px"}}>
                <span style={{color:P.accent,fontWeight:600}}>Fetcher parallelism (F16):</span> num.replica.fetchers
                = {calc.numReplicaFetchers} (one per ~6 partitions). Each fetcher thread handles multiple partitions
                sequentially. More fetchers improve replication throughput but increase broker thread count and
                memory. Tune based on partition count and observed replication lag.
              </div>
            </div>
          </Card>

          <Card>
            <div style={{color:P.muted, fontSize:"0.8em", lineHeight:1.7}}>
              <strong style={{color:P.text}}>Note on socket buffers in broker:</strong> Kafka's broker
              uses <code style={{color:P.cyan}}>socket.send.buffer.bytes = -1</code> by default, which
              defers to the OS. Explicitly setting it prevents the OS ceiling from being the silent limit
              when rmem_max is large but the Kafka config hasn't been updated to match.
            </div>
          </Card>
        </div>
      )}

      {/* Tab: Scenario table */}
      {tab === "table" && (
        <Card style={{padding:0, overflow:"hidden"}}>
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%", borderCollapse:"collapse", fontSize:"0.8em"}}>
              <thead>
                <tr style={{background:P.panel2, borderBottom:`2px solid ${P.border}`}}>
                  {TABLE_HEADS.map(h => (
                    <th key={h} style={{padding:"10px 12px", textAlign:"left",
                      color:P.muted, fontWeight:600, letterSpacing:"0.04em",
                      whiteSpace:"nowrap", fontSize:"0.85em"}}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {SCENARIO_TABLE.map((row, i) => (
                  <tr key={i} style={{borderBottom:`1px solid ${P.border}`,
                    background: i===0 ? "#100a0a" : i%2===0 ? "transparent" : P.panel2}}>
                    {TABLE_COLS.map(col => (
                      <td key={col} style={{padding:"9px 12px", whiteSpace:"nowrap",
                        color: col==="label" ? P.text :
                               col==="batch"||col==="rmem" ? P.cyan :
                               col==="linger" ? P.orange :
                               col==="cc" ? P.purple :
                               col==="comp" ? P.green :
                               col==="rtt" ? P.yellow : P.muted,
                        fontFamily: col!=="label" ? "monospace" : "inherit",
                        fontWeight: col==="label" ? 600 : 400,
                        opacity: i===0 ? 0.7 : 1,
                      }}>{row[col]}</td>
                    ))}
                  </tr>
                ))}
                {/* YOUR PATH row */}
                <tr style={{borderTop:`2px solid ${P.accent}55`, background:"#0a1020"}}>
                  <td style={{padding:"10px 12px",color:P.accent,fontWeight:700}}>
                    ▶ Your Path
                  </td>
                  <td style={{padding:"10px 12px",color:P.cyan,fontFamily:"monospace"}}>{fmtMbps(custom.bwMbps)}</td>
                  <td style={{padding:"10px 12px",color:P.yellow,fontFamily:"monospace"}}>{custom.rttAvg}ms</td>
                  <td style={{padding:"10px 12px",color:P.cyan,fontFamily:"monospace"}}>{fmtBytes(calc.empiricalBDP)}</td>
                  <td style={{padding:"10px 12px",color:P.cyan,fontFamily:"monospace"}}>{fmtBytes(calc.batchSize)}</td>
                  <td style={{padding:"10px 12px",color:P.cyan,fontFamily:"monospace"}}>{fmtBytes(calc.bufCeil)}</td>
                  <td style={{padding:"10px 12px",color:P.orange,fontFamily:"monospace"}}>{calc.lingerThru}ms</td>
                  <td style={{padding:"10px 12px",color:P.purple,fontFamily:"monospace"}}>bbr</td>
                  <td style={{padding:"10px 12px",color:P.muted,fontFamily:"monospace"}}>all</td>
                  <td style={{padding:"10px 12px",color:P.green,fontFamily:"monospace"}}>lz4</td>
                </tr>
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* Tab: Scripts */}
      {tab === "scripts" && (
        <div style={{display:"grid", gap:16}}>
          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:4}}>Measurement + Analysis workflow</Label>
            <div style={{color:P.muted,fontSize:"0.8em",marginBottom:8}}>
              Run kafka-tcp-measure.sh from the producer host. Outputs CSV files.
              kafka-tcp-analyze.sh reads them and writes sysctl + Kafka property files.
            </div>
            <CodeBlock title="Quick start">{measureScript}</CodeBlock>
          </Card>
          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:8}}>Script overview</Label>
            <div style={{display:"grid", gridTemplateColumns:"1fr 1fr", gap:10}}>
              {[
                {file:"kafka-tcp-measure.sh", color:P.accent,
                 steps:["Phase 1: ping RTT (200 samples)","Phase 2: iperf3 window sweep (1 stream)","Phase 3: parallel stream sweep","Phase 4: Nagle/TCP_NODELAY comparison"],
                 out:"results/  ping.csv  window_sweep.csv  parallel_sweep.csv  nodelay_comparison.csv  meta.env"},
                {file:"kafka-tcp-analyze.sh", color:P.green,
                 steps:["Loads meta.env from measure run","Computes BDP, buffer ceilings, batch sizes","Detects bottleneck type (window/loss/jitter)","Outputs sysctl + producer + broker configs"],
                 out:"99-kafka-tcp.conf  producer-throughput.properties  producer-latency.properties  broker-additions.properties"},
              ].map(({file,color,steps,out}) => (
                <div key={file} style={{background:P.panel2,border:`1px solid ${color}33`,borderRadius:8,padding:"12px 14px"}}>
                  <div style={{fontFamily:"monospace",color,fontSize:"0.85em",fontWeight:700,marginBottom:8}}>{file}</div>
                  {steps.map((s,i) => (
                    <div key={i} style={{color:P.muted,fontSize:"0.77em",lineHeight:1.7}}>
                      {i+1}. {s}
                    </div>
                  ))}
                  <div style={{marginTop:8,paddingTop:8,borderTop:`1px solid ${P.border}`,
                    color:P.dim,fontSize:"0.72em",fontFamily:"monospace",lineHeight:1.6}}>
                    output: {out}
                  </div>
                </div>
              ))}
            </div>
          </Card>
          <Card>
            <Label c={P.muted} style={{display:"block",marginBottom:8}}>Verify applied settings</Label>
            <CodeBlock>{`# Verify TCP congestion control
sysctl net.ipv4.tcp_congestion_control   # → bbr
tc qdisc show dev eth0                   # → fq

# Verify window on active Kafka connections
ss -tin dst <broker-ip> | grep -E "rtt|cwnd|mss|rcv_space"
# Look for: cwnd × mss ≈ BDP (${fmtBytes(calc.empiricalBDP)})

# Verify TCP_NODELAY (Nagle off)
ss -tinp | grep <kafka-port> | grep nonagle

# Benchmark Kafka producer before/after
kafka-producer-perf-test.sh \\
  --topic test-perf \\
  --num-records 1000000 \\
  --record-size ${calc.batchSize} \\
  --throughput -1 \\
  --producer-props \\
    bootstrap.servers=<broker>:9092 \\
    batch.size=${calc.batchSize} \\
    linger.ms=${calc.lingerThru} \\
    compression.type=lz4`}</CodeBlock>
          </Card>
        </div>
      )}

      {/* Footer */}
      <div style={{marginTop:48, paddingTop:18, borderTop:`1px solid ${P.border}`,
        color:P.dim, fontSize:"0.75em", lineHeight:1.9}}>
        <div style={{color:P.muted, fontWeight:600, fontSize:"0.85em", marginBottom:6}}>References</div>
        <div>F1/F2: Little, J.D.C. (1961) Op.Res. 9(3) — <em>L=λW</em>; Jacobson, Braden, Borman (1992) RFC 1323 §1.</div>
        <div>F4: Mathis, Semke, Mahdavi, Ott (1997) ACM SIGCOMM CCR 27(3) — <em>T = MSS/(RTT×√p)</em> bytes/sec ÷ 125,000 = Mbit/s.</div>
        <div>F5: Padhye, Firoiu, Towsley, Kurose (1998) ACM SIGCOMM — refined model with RTO term.</div>
        <div>F6/F7: Jacobson (1988) ACM SIGCOMM; Allman, Paxson, Blanton (2009) RFC 5681; Chiu & Jain (1989) Comput. Networks ISDN Syst. 17(1).</div>
        <div>F8: Postel (1983) RFC 879; Mogul & Deering (1990) RFC 1191 (PMTUD).</div>
        <div>F9/BBR: Cardwell, Cheng, Gunn, Yeganeh, Jacobson (2016) ACM Queue 14(5).</div>
        <div>CUBIC: Ha, Rhee, Xu (2008) ACM SIGOPS OSR 42(5). RFC 7323 (2014) supersedes RFC 1323.</div>
        <div>F10–F12/Kafka: Apache Kafka Producer Configuration docs. Linux buffer tuning: kernel.org ip-sysctl.txt.</div>
      </div>
    </div>
  );
}