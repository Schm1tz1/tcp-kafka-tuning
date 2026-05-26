import { useState, useCallback } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Legend, AreaChart, Area,
  BarChart, Bar,
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
function calcFromMeasurements({bwMbps, rttMin, rttAvg, plateauKB, conns, mtu, inflight, latencyBudgetMs, pktLoss, partitions, compressionRatio}) {
  const bwBytes   = bwMbps * 1e6 / 8;
  const rttSMin   = rttMin / 1000;
  const rttSAvg   = rttAvg / 1000;
  const empiricalBDP  = plateauKB * 1024;
  const theoreticalBDP = Math.round(bwBytes * rttSMin);
  const mss       = mtu - 40;
  const bufCeil   = nextPow2(empiricalBDP * conns * 2);
  const batchMin  = Math.round(empiricalBDP / inflight);
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

  return {empiricalBDP, theoreticalBDP, mss, bufCeil, batchSize, batchMin,
          lingerThru, lingerLatency, mathisMbps,
          kafkaWireMbps, kafkaLogicalMbps, kafkaWindowMbps,
          effectiveMbps, effectiveLogicalMbps,
          perPartWireMbps, perPartLogicalMbps, perPartWindowBytes, perPartBdpPct,
          partitionSeries};
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
// bwMbps: link bandwidth, rttMs: propagation RTT, bufMss: switch buffer in MSS
function simBbrVsCubic(bwMbps, rttMs, bufMss = 50) {
  const bdpMss   = Math.max(1, Math.round((bwMbps * 1e6 / 8) * (rttMs / 1000) / 1460));
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
    const tput_c = Math.min(bwMbps, (inFlight_c * 1460 * 8) / (rtt_c / 1000) / 1e6);

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
  { id:"local",       label:"Local DC",       bwMbps:10000, rttMin:0.08, rttAvg:0.12, pktLoss:0,   mtu:9000, conns:8,  latency:5   },
  { id:"same_az",     label:"Same AZ",        bwMbps:1000,  rttMin:1,    rttAvg:2,    pktLoss:0,   mtu:1500, conns:8,  latency:10  },
  { id:"cross_az",    label:"Cross-AZ",       bwMbps:1000,  rttMin:8,    rttAvg:12,   pktLoss:0,   mtu:1500, conns:4,  latency:20  },
  { id:"cross_region",label:"Cross-Region",   bwMbps:500,   rttMin:55,   rttAvg:65,   pktLoss:0.01,mtu:1500, conns:2,  latency:100 },
  { id:"multi_region",label:"Multi-Region",   bwMbps:200,   rttMin:140,  rttAvg:155,  pktLoss:0.02,mtu:1500, conns:2,  latency:250 },
  { id:"satellite",   label:"Satellite",      bwMbps:50,    rttMin:580,  rttAvg:620,  pktLoss:0.1, mtu:1500, conns:1,  latency:900 },
  { id:"custom",      label:"Custom / Measured", bwMbps:1000, rttMin:5, rttAvg:7,    pktLoss:0,   mtu:1500, conns:4,  latency:50  },
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

const Slider = ({label, value, min, max, step=1, unit="", onChange, color=P.accent}) => {
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
        <Label c={P.muted}>{label}</Label>
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
const TABLE_HEADS = ["Scenario","Bandwidth","RTT","BDP","batch.size","tcp_rmem_max","linger.ms","CC","acks","compression"];

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
  const [compressionRatio, setCompressionRatio] = useState(2.5);

  const scen = SCENARIOS.find(s=>s.id===scenarioId) || SCENARIOS[0];

  const [custom, setCustom] = useState({
    bwMbps: scen.bwMbps, rttMin: scen.rttMin, rttAvg: scen.rttAvg,
    pktLoss: scen.pktLoss, mtu: scen.mtu, conns: scen.conns,
    latencyBudgetMs: scen.latency,
  });

  // Auto-derive plateau from BDP for simulation
  const simBDP = custom.bwMbps * 1e6 / 8 * custom.rttMin / 1000;
  const plateauKB = Math.max(4, Math.ceil(simBDP / 1024));

  const calc = calcFromMeasurements({
    ...custom, plateauKB, inflight, partitions, compressionRatio,
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
  if (custom.pktLoss > 0.1)
    diag.push({type:"err", msg:`Packet loss ${custom.pktLoss}% → Mathis bound: ${calc.mathisMbps?calc.mathisMbps.toFixed(0):"N/A"} Mbps. Fix network before buffer tuning.`});
  if (custom.pktLoss > 0 && custom.pktLoss <= 0.1)
    diag.push({type:"warn", msg:`Low packet loss ${custom.pktLoss}% detected. BBR + lz4 will help. Monitor retransmits.`});
  if (calc.empiricalBDP > 500000)
    diag.push({type:"warn", msg:`High BDP path (${fmtBytes(calc.empiricalBDP)}). Default Kafka buffers (256 KB) will severely limit throughput.`});
  if (custom.mtu === 1500 && custom.bwMbps >= 10000)
    diag.push({type:"warn", msg:`10+ Gbps with standard MTU 1500 — consider jumbo frames (MTU 9000) on the Kafka VLAN for ~5× reduction in header overhead.`});
  if (custom.rttAvg > 100)
    diag.push({type:"warn", msg:`High RTT (${custom.rttAvg}ms) — linger.ms should be tuned carefully. Batch accumulation time must exceed BDP drain time (${calc.lingerThru}ms).`});
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
buffer.memory                         = ${calc.bufCeil * 2}
compression.type                      = lz4
max.in.flight.requests.per.connection = ${inflight}
acks                                  = all
enable.idempotence                    = true
send.buffer.bytes                     = ${calc.batchSize * 2}
receive.buffer.bytes                  = 65536
retries                               = 10
retry.backoff.ms                      = 100
delivery.timeout.ms                   = 120000`;

  const kafkaLatConf = `# LATENCY profile (budget: ${custom.latencyBudgetMs}ms)
# RTT: ${custom.rttAvg}ms → linger headroom: ${calc.lingerLatency}ms

batch.size                            = 16384
linger.ms                             = ${calc.lingerLatency}
compression.type                      = lz4
max.in.flight.requests.per.connection = 1
acks                                  = 1
enable.idempotence                    = false
request.timeout.ms                    = 5000
delivery.timeout.ms                   = 10000
retries                               = 3
retry.backoff.ms                      = 50`;

  const brokerConf = `# Broker server.properties additions
socket.send.buffer.bytes              = ${calc.bufCeil}
socket.receive.buffer.bytes           = ${calc.bufCeil}
socket.request.max.bytes              = 104857600
num.network.threads                   = 8
num.io.threads                        = 8
replica.fetch.max.bytes               = ${calc.batchSize}
replica.socket.receive.buffer.bytes   = ${calc.bufCeil}`;

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
          <Label c={P.muted} style={{display:"block", marginBottom:14}}>Path Parameters</Label>
          <Slider label="Bandwidth" value={custom.bwMbps} min={10} max={50000} step={10}
            unit={custom.bwMbps>=1000?` (${(custom.bwMbps/1000).toFixed(custom.bwMbps%1000===0?0:1)} Gbps)`:" Mbps"}
            color={P.accent} onChange={v=>setCustom(c=>({...c,bwMbps:v}))} />
          <Slider label="RTT min (ms)" value={custom.rttMin} min={0.05} max={700} step={0.05}
            unit=" ms" color={P.green} onChange={v=>setCustom(c=>({...c,rttMin:v}))} />
          <Slider label="RTT avg (ms)" value={custom.rttAvg} min={0.1} max={700} step={0.1}
            unit=" ms" color={P.cyan} onChange={v=>setCustom(c=>({...c,rttAvg:v}))} />
          <Slider label="Packet loss" value={custom.pktLoss} min={0} max={5} step={0.01}
            unit="%" color={P.red} onChange={v=>setCustom(c=>({...c,pktLoss:v}))} />
          <Slider label="MTU" value={custom.mtu} min={576} max={9000} step={1}
            unit=" bytes" color={P.yellow} onChange={v=>setCustom(c=>({...c,mtu:v}))} />
          <Slider label="Parallel connections" value={custom.conns} min={1} max={32} step={1}
            unit="" color={P.purple} onChange={v=>setCustom(c=>({...c,conns:v}))} />
          <Slider label="Latency budget" value={custom.latencyBudgetMs} min={1} max={1000} step={1}
            unit=" ms" color={P.orange} onChange={v=>setCustom(c=>({...c,latencyBudgetMs:v}))} />
          <Slider label="Max in-flight requests" value={inflight} min={1} max={10} step={1}
            unit="" color={P.cyan} onChange={setInflight} />
          <Slider label="Partitions (topic total)" value={partitions} min={1} max={256} step={1}
            unit="" color={P.green} onChange={setPartitions} />
          <Slider label="Compression ratio (lz4/zstd)" value={compressionRatio} min={1} max={6} step={0.1}
            unit={`× (${compressionRatio.toFixed(1)}×)`} color={P.purple} onChange={setCompressionRatio} />
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
            <StatBox label="Total wire throughput"
              value={`${fmtMbps(calc.effectiveMbps)} wire`} color={P.green}
              warn={calc.effectiveMbps < custom.bwMbps * 0.5}
              sub={`${fmtMbps(calc.effectiveLogicalMbps)} app data · independent of partition count`} />
            <StatBox label={`Per partition (${partitions}p)`}
              value={`${fmtMbps(calc.perPartWireMbps)} wire`} color={P.cyan}
              warn={calc.perPartBdpPct < 20}
              sub={`${fmtMbps(calc.perPartLogicalMbps)} app data · ${calc.perPartBdpPct}% BDP util`} />
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
          ["overview","Overview"],["throughput","Throughput"],["bbr","BBR vs CUBIC"],["sysctl","sysctl"],
          ["kafka","Kafka Props"],["broker","Broker"],
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

          {/* Wire vs app data clarification */}
          <div style={{background:P.panel2, border:`1px solid ${P.border}`, borderRadius:8,
            padding:"10px 14px", fontSize:"0.8em", color:P.muted, lineHeight:1.6}}>
            <span style={{color:P.text, fontWeight:600}}>Total vs per-partition: </span>
            Total wire throughput and app data rate are fixed by bandwidth, RTT, window size, and loss —
            they do not change with partition count. The partition count divides that total capacity
            across partitions: more partitions means less throughput available per partition.
            The <span style={{color:P.cyan, fontFamily:"monospace"}}>Per partition ({partitions}p)</span> box
            and chart respond to the Partitions slider.
            A {compressionRatio}× compression ratio means {fmtMbps(calc.effectiveMbps)} wire
            delivers {fmtMbps(calc.effectiveLogicalMbps)} of application data in total.
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
        const { data, bdpMss, maxCwnd } = simBbrVsCubic(custom.bwMbps, custom.rttAvg, bufMss);

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
                 note:"Total producer buffer. Must cover all pending batches."},
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

      {/* Tab: Broker */}
      {tab === "broker" && (
        <Card>
          <Label c={P.muted} style={{display:"block",marginBottom:4}}>Broker server.properties additions</Label>
          <div style={{color:P.muted,fontSize:"0.8em",marginBottom:8}}>
            Restart broker after applying. Test replication throughput with kafka-producer-perf-test.sh.
          </div>
          <CodeBlock>{brokerConf}</CodeBlock>
          <div style={{marginTop:16, color:P.muted, fontSize:"0.8em", lineHeight:1.7}}>
            <strong style={{color:P.text}}>Note on socket buffers in broker:</strong> Kafka's broker
            uses <code style={{color:P.cyan}}>socket.send.buffer.bytes = -1</code> by default, which
            defers to the OS. Explicitly setting it prevents the OS ceiling from being the silent limit
            when rmem_max is large but the Kafka config hasn't been updated to match.
          </div>
        </Card>
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
