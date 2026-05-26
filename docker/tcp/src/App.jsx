import { useState, useEffect, useRef } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Area, AreaChart,
  ScatterChart, Scatter, Legend, Label, BarChart, Bar,
} from "recharts";

// ─── Data generators ──────────────────────────────────────────────────────────

function bdpData() {
  const rows = [];
  const bandwidths = [100, 1000, 10000, 50000]; // Mbps
  for (let rtt = 1; rtt <= 200; rtt += 2) {
    const row = { rtt };
    bandwidths.forEach(bw => {
      // BDP (bytes) = B (bytes/s) × RTT (s)  — RFC 1323 §1, Jacobson et al. 1992
      row[`${bw}Mbps`] = ((bw * 1e6 / 8) * (rtt / 1000)) / 1024; // KB
    });
    rows.push(row);
  }
  return rows;
}

function throughputVsWindow() {
  const rows = [];
  const rtts = [5, 20, 80, 200]; // ms
  for (let win = 4; win <= 65536; win += win < 512 ? 4 : win < 4096 ? 64 : 512) {
    const row = { win };
    rtts.forEach(rtt => {
      // T = W / RTT  (Little 1961; TCP application: RFC 1323 §1)
      // win is in KB; convert to bytes (*1024), to bits (*8), divide by RTT in seconds
      row[`${rtt}ms`] = Math.min(50000, ((win * 1024 * 8) / (rtt / 1000)) / 1e6); // Mbps
    });
    rows.push(row);
  }
  return rows;
}

function cwndData() {
  // Models TCP Reno per RFC 5681 (Allman, Paxson, Blanton 2009).
  // Slow start: cwnd doubles each RTT until ssthresh (RFC 5681 §3.1).
  // Congestion avoidance: +1 MSS/RTT — AIMD (Jacobson 1988 ACM SIGCOMM).
  // Timeout: ssthresh = cwnd/2; cwnd → 1 (RFC 5681 §3.1).
  // Fast recovery (3 dup-ACKs): ssthresh = cwnd/2; cwnd = ssthresh (RFC 5681 §3.2).
  const data = [];
  let cwnd = 1, ssthresh = 32, time = 0;
  for (let i = 0; i < 60; i++) {
    data.push({ time, cwnd: Math.round(cwnd * 10) / 10, ssthresh });
    if (cwnd < ssthresh) {
      cwnd = Math.min(cwnd * 2, ssthresh + 0.1);
    } else {
      cwnd += 1;
    }
    time += 1;
    if (i === 28) {
      ssthresh = Math.ceil(cwnd / 2);
      cwnd = 1;
      data.push({ time, cwnd: Math.round(cwnd * 10) / 10, ssthresh, event: "loss" });
    }
    if (i === 48) {
      ssthresh = Math.ceil(cwnd / 2);
      cwnd = ssthresh;
      data.push({ time, cwnd: Math.round(cwnd * 10) / 10, ssthresh, event: "3dup-ack" });
    }
  }
  return data;
}

function mathisData() {
  const rows = [];
  const mss = 1460;   // bytes — standard Ethernet MSS (RFC 879)
  const rtt = 0.05;   // seconds — 50 ms reference path
  for (let pExp = -1; pExp >= -5; pExp -= 0.05) {
    const p = Math.pow(10, pExp);
    // Mathis et al. (1997) ACM SIGCOMM CCR 27(3):
    //   T = MSS / (RTT × √p)
    // T is in bytes/sec; divide by 125000 (= 1e6/8) to convert to Mbit/s.
    // Common error: dividing by 1e6 treats the result as bits/sec and gives
    // values 8× too small. Verified: p=0.01, RTT=50ms → T = 2.34 Mbit/s.
    const throughput = (mss / (rtt * Math.sqrt(p))) / 125000; // Mbit/s
    rows.push({ p: pExp, throughput: Math.min(50000, throughput) });
  }
  return rows;
}

function throughputVsRtt() {
  // T = W / RTT  — Little (1961) Op. Res. 9(3); TCP application: RFC 1323 §1.
  // Window sizes in KB. Result capped at 50 Gbps (hardware ceiling).
  const rows = [];
  const windows = [64, 256, 1024, 4096, 16384]; // KB
  for (let rtt = 1; rtt <= 500; rtt += 5) {
    const row = { rtt };
    windows.forEach(w => {
      row[`${w}KB`] = Math.min(50000, ((w * 1024 * 8) / (rtt / 1000)) / 1e6);
    });
    rows.push(row);
  }
  return rows;
}

// ─── BBR vs CUBIC simulation ──────────────────────────────────────────────────
// CUBIC source: Ha, Rhee, Xu (2008) ACM SIGOPS OSR 42(5).
// BBR source:   Cardwell, Cheng, Gunn, Yeganeh, Jacobson (2016) ACM Queue 14(5).
// Simulation models key behaviours qualitatively; not a full ns-3/ns-2 trace.
// CUBIC: loss-triggered AIMD (RFC 5681). RTT inflation via Little's Law queuing.
// BBR:   BtlBw×RTprop = BDP target; pacing_gain cycle per Cardwell 2016 §4.
// bwMbps: link bandwidth, rttMs: propagation RTT, bufMss: switch buffer depth
function simBbrVsCubic(bwMbps, rttMs, bufMss) {
  const bdpMss  = Math.max(1, Math.round((bwMbps * 1e6 / 8) * (rttMs / 1000) / 1460));
  const maxCwnd = bdpMss + bufMss;
  const rows    = [];

  let cwnd_c   = 2;
  let ssthresh = Math.round(bdpMss * 1.5);

  let cwnd_b   = bdpMss;
  let bbrPhase = 0;

  for (let t = 0; t < 70; t++) {
    // ── CUBIC ─────────────────────────────────────────────────────────────
    const inFlight_c = Math.min(cwnd_c, maxCwnd);
    const qDepth_c   = Math.max(0, inFlight_c - bdpMss);
    const rtt_c      = rttMs + (qDepth_c / Math.max(1, bdpMss)) * rttMs * 2;
    const tput_c     = Math.min(bwMbps,
      (inFlight_c * 1460 * 8) / (rtt_c / 1000) / 1e6);
    const loss_c = inFlight_c >= maxCwnd;
    if (loss_c) {
      ssthresh = Math.max(2, Math.floor(cwnd_c / 2));
      cwnd_c   = ssthresh;
    } else if (cwnd_c < ssthresh) {
      cwnd_c = Math.min(cwnd_c * 2, ssthresh);
    } else {
      cwnd_c = Math.min(cwnd_c + 1, maxCwnd + 4);
    }

    // ── BBR ───────────────────────────────────────────────────────────────
    // 8-round cycle: 7 cruise + 1 probe_up + 1 probe_drain; probe_rtt every 30
    const cycle = t % 10;
    if      (t % 30 === 29) bbrPhase = 3;          // probe_rtt — drain to 4 MSS
    else if (cycle === 8)   bbrPhase = 1;           // probe_bw up  (+25%)
    else if (cycle === 9)   bbrPhase = 2;           // probe_bw down (drain)
    else                    bbrPhase = 0;           // steady cruise

    let cwndTarget_b, gainFactor;
    if      (bbrPhase === 3) { cwndTarget_b = 4;                       gainFactor = 0.1;  }
    else if (bbrPhase === 1) { cwndTarget_b = Math.round(bdpMss*1.25); gainFactor = 1.25; }
    else if (bbrPhase === 2) { cwndTarget_b = Math.round(bdpMss*0.75); gainFactor = 0.75; }
    else                     { cwndTarget_b = bdpMss * 2;              gainFactor = 1.0;  }

    const qDepth_b = bbrPhase === 1 ? Math.round(bdpMss * 0.25) : 0;
    const rtt_b    = rttMs + (qDepth_b / Math.max(1, bdpMss)) * rttMs * 0.5;
    const tput_b   = Math.min(bwMbps, bwMbps * gainFactor * (bbrPhase === 3 ? 0.1 : 1.0));

    rows.push({
      t,
      cwnd_cubic:  Math.round(Math.min(inFlight_c, maxCwnd)),
      rtt_cubic:   +rtt_c.toFixed(1),
      tput_cubic:  +tput_c.toFixed(1),
      queue_cubic: qDepth_c,
      loss_cubic:  loss_c ? Math.round(inFlight_c) : null,
      cwnd_bbr:    cwndTarget_b,
      rtt_bbr:     +rtt_b.toFixed(1),
      tput_bbr:    +tput_b.toFixed(1),
      queue_bbr:   qDepth_b,
      bdp:         bdpMss,
      bufLimit:    maxCwnd,
      linkRate:    bwMbps,
      propRtt:     rttMs,
    });
  }
  return { rows, bdpMss, maxCwnd };
}

// ─── BBR Interactive Component ────────────────────────────────────────────────
function BbrComparison() {
  const [bw,  setBw]  = useState(1000);   // Mbps
  const [rtt, setRtt] = useState(20);     // ms
  const [buf, setBuf] = useState(50);     // switch buffer in MSS

  // Per-chart log scale toggles
  const [logCwnd,  setLogCwnd]  = useState(false);
  const [logRtt2,  setLogRtt2]  = useState(false);
  const [logQueue, setLogQueue] = useState(false);
  const [logTput,  setLogTput]  = useState(false);

  const { rows, bdpMss, maxCwnd } = simBbrVsCubic(bw, rtt, buf);

  const avgCubicTput  = +(rows.reduce((s,r)=>s+r.tput_cubic,0)/rows.length).toFixed(1);
  const avgBbrTput    = +(rows.reduce((s,r)=>s+r.tput_bbr,0)/rows.length).toFixed(1);
  const avgCubicRtt   = +(rows.reduce((s,r)=>s+r.rtt_cubic,0)/rows.length).toFixed(1);
  const avgBbrRtt     = +(rows.reduce((s,r)=>s+r.rtt_bbr,0)/rows.length).toFixed(1);
  const avgCubicQueue = +(rows.reduce((s,r)=>s+r.queue_cubic,0)/rows.length).toFixed(1);
  const lossEvents    = rows.filter(r=>r.loss_cubic!==null).length;
  const tputGain      = avgCubicTput > 0
    ? +(((avgBbrTput - avgCubicTput) / avgCubicTput) * 100).toFixed(0) : 0;
  const rttReduction  = avgCubicRtt  > 0
    ? +(((avgCubicRtt  - avgBbrRtt)  / avgCubicRtt)  * 100).toFixed(0) : 0;

  const grid = <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />;
  const xAx  = <XAxis dataKey="t" stroke={P.muted} tick={{fontSize:10}}
    label={{value:"Round trips", position:"insideBottom", dy:13, fill:P.muted, fontSize:10}} />;
  const tipStyle = {background:P.panel, border:`1px solid ${P.border}`,
    borderRadius:8, fontSize:"0.8em"};

  return (
    <div>
      {/* Sliders */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 20px", margin:"16px 0"}}>
        <div style={{color:P.muted, fontSize:"0.78em", fontWeight:600,
          letterSpacing:"0.08em", textTransform:"uppercase", marginBottom:14}}>
          Simulation Parameters
        </div>
        <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))", gap:20}}>
          {[
            {label:"Bandwidth", val:bw,  set:setBw,  min:10,  max:50000, step:10,
              fmt: v => v>=1000?`${(v/1000).toFixed(v%1000===0?0:1)} Gbps`:`${v} Mbps`, color:P.accent},
            {label:"RTT (propagation)", val:rtt, set:setRtt, min:1, max:600, step:1,
              fmt: v=>`${v} ms`, color:P.green},
            {label:"Switch buffer depth", val:buf, set:setBuf, min:5, max:200, step:5,
              fmt: v=>`${v} MSS`, color:P.yellow},
          ].map(({label,val,set,min,max,step,fmt,color}) => (
            <SliderField key={label} label={label} val={val} set={set}
              min={min} max={max} step={step} fmt={fmt} color={color} />
          ))}
        </div>
        {/* Derived info */}
        <div style={{display:"flex", gap:16, marginTop:14, flexWrap:"wrap"}}>
          {[
            {label:"BDP", value:`${bdpMss} MSS`, color:P.accent},
            {label:"Buffer limit", value:`${maxCwnd} MSS (BDP + ${buf} MSS)`, color:P.yellow},
            {label:"BBR throughput gain", value:`+${tputGain}%`, color:P.green},
            {label:"RTT reduction", value:`${rttReduction}%`, color:P.cyan},
            {label:"CUBIC loss events", value:`${lossEvents}`, color:P.red},
          ].map(({label,value,color}) => (
            <div key={label} style={{background:"#0d1117", borderRadius:7,
              padding:"6px 12px", border:`1px solid ${color}33`}}>
              <div style={{color:P.muted, fontSize:"0.7em", textTransform:"uppercase",
                letterSpacing:"0.06em"}}>{label}</div>
              <div style={{color, fontFamily:"monospace", fontWeight:700,
                fontSize:"0.9em"}}>{value}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Chart 1: cwnd */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Congestion window (cwnd) — MSS" logY={logCwnd} setLogY={setLogCwnd} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          CUBIC climbs exponentially until it overflows the buffer, then halves — the sawtooth.
          BBR holds at 2×BDP in cruise; briefly probes at 1.25× every 8 RTTs to test for
          more bandwidth. Red dots = CUBIC loss events (required signal).
        </div>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logCwnd, 0.5, "cwnd (MSS)", {tick:{fontSize:10}, label:{...yAxisProps(logCwnd,0.5,"cwnd (MSS)").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle}
              formatter={(v,n)=>[typeof v==="number"?`${v} MSS`:v, n]} />
            <ReferenceLine y={bdpMss}  stroke={P.accent} strokeDasharray="4 3"
              label={{value:"BDP", fill:P.accent, fontSize:10, position:"insideTopRight"}} />
            <ReferenceLine y={maxCwnd} stroke={P.red}    strokeDasharray="2 4"
              label={{value:"buffer limit", fill:P.red, fontSize:10, position:"insideTopRight"}} />
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Line type="monotone" dataKey="cwnd_cubic" name="CUBIC"
              dot={false} strokeWidth={2} stroke={P.red} />
            <Line type="monotone" dataKey="cwnd_bbr"   name="BBR"
              dot={false} strokeWidth={2} stroke={P.green} />
            <Line type="monotone" dataKey="loss_cubic" name="CUBIC loss"
              dot={{r:4, fill:P.red, stroke:"#fff", strokeWidth:1}}
              activeDot={false} stroke="none" legendType="circle" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: RTT */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Observed RTT — ms" logY={logRtt2} setLogY={setLogRtt2} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          CUBIC fills the switch buffer before backing off — queuing delay inflates RTT well
          above the propagation baseline. BBR continuously tracks RTprop (minimum RTT) and
          avoids building a queue, keeping latency near the wire delay.
        </div>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logRtt2, 0.1, "RTT (ms)", {tick:{fontSize:10}, label:{...yAxisProps(logRtt2,0.1,"RTT (ms)").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle} formatter={(v,n)=>[`${v} ms`, n]} />
            <ReferenceLine y={rtt} stroke={P.accent} strokeDasharray="4 3"
              label={{value:"RTprop", fill:P.accent, fontSize:10, position:"insideTopRight"}} />
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Line type="monotone" dataKey="rtt_cubic" name="CUBIC RTT"
              dot={false} strokeWidth={2} stroke={P.red} />
            <Line type="monotone" dataKey="rtt_bbr"   name="BBR RTT"
              dot={false} strokeWidth={2} stroke={P.green} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: Queue depth */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Switch buffer queue depth — MSS" logY={logQueue} setLogY={setLogQueue} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          CUBIC persistently fills the buffer — this is bufferbloat and raises latency for
          every flow sharing that port. BBR targets zero queue in steady state; a brief spike
          appears only during the bandwidth probe phase (1 RTT every 8 RTTs).
        </div>
        <ResponsiveContainer width="100%" height={190}>
          <AreaChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logQueue, 0.1, "Queue (MSS)", {tick:{fontSize:10}, label:{...yAxisProps(logQueue,0.1,"Queue (MSS)").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle} formatter={(v,n)=>[`${v} MSS`, n]} />
            <defs>
              <linearGradient id="qc" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={P.red}   stopOpacity={0.3}/>
                <stop offset="95%" stopColor={P.red}   stopOpacity={0}/>
              </linearGradient>
              <linearGradient id="qb" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={P.green} stopOpacity={0.3}/>
                <stop offset="95%" stopColor={P.green} stopOpacity={0}/>
              </linearGradient>
            </defs>
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Area type="monotone" dataKey="queue_cubic" name="CUBIC queue"
              stroke={P.red}   fill="url(#qc)" strokeWidth={1.5} />
            <Area type="monotone" dataKey="queue_bbr"   name="BBR queue"
              stroke={P.green} fill="url(#qb)" strokeWidth={1.5} />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Throughput */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Throughput — Mbps" logY={logTput} setLogY={setLogTput} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          CUBIC throughput oscillates continuously — always either climbing toward the link
          rate or recovering from loss. BBR maintains steady throughput near the link ceiling,
          with only a brief dip during the RTT probe (~every 30 RTTs, 1 RTT duration).
        </div>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logTput, 0.1, "Mbps", {tick:{fontSize:10}, label:{...yAxisProps(logTput,0.1,"Mbps").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle} formatter={(v,n)=>[`${v} Mbps`, n]} />
            <ReferenceLine y={bw} stroke={P.accent} strokeDasharray="4 3"
              label={{value:"link rate", fill:P.accent, fontSize:10, position:"insideTopRight"}} />
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Line type="monotone" dataKey="tput_cubic" name="CUBIC"
              dot={false} strokeWidth={2} stroke={P.red} />
            <Line type="monotone" dataKey="tput_bbr"   name="BBR"
              dot={false} strokeWidth={2} stroke={P.green} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Behaviour table */}
      <div style={{overflowX:"auto", margin:"8px 0 24px"}}>
        <table style={{width:"100%", borderCollapse:"collapse", fontSize:"0.83em"}}>
          <thead>
            <tr style={{borderBottom:`2px solid ${P.border}`}}>
              {["Property","CUBIC (default)","BBR","Why it matters"].map(h=>(
                <th key={h} style={{padding:"8px 10px", textAlign:"left", color:P.muted,
                  fontWeight:600, letterSpacing:"0.03em"}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[
              ["Congestion signal",   "Packet loss — must overflow buffer",   "BtlBw + RTprop model",          "BBR never waits for a drop"],
              ["Window behaviour",    "Sawtooth — grow, drop, repeat",        "Steady at BDP; brief probes",   "More predictable throughput"],
              ["Queue depth",         "Fills buffer (bufferbloat)",            "Near zero in steady state",     "Lower latency for all flows on switch"],
              ["RTT inflation",       "Up to 2–3× propagation delay",         "Tracks propagation delay",      "More accurate timeout calculation"],
              ["Loss event required", `Yes — ${lossEvents} in simulation`,    "No",                            "BBR works well on lossy links"],
              ["fq qdisc",           "Not required",                          "Mandatory (pacing)",            "Must set default_qdisc=fq with BBR"],
              ["High BDP paths",      "Under-utilises (window takes time)",   "Self-calibrates immediately",   "No manual rmem tuning required"],
              ["After restart/idle",  "Slow convergence (sawtooth)",          "Re-locks to BDP quickly",       "Faster post-failure recovery"],
            ].map(([prop,cubic,bbr,why],i)=>(
              <tr key={prop} style={{borderBottom:`1px solid ${P.border}`,
                background: i%2===0 ? "transparent" : "#161b22"}}>
                <td style={{padding:"7px 10px", color:P.text,   fontWeight:600}}>{prop}</td>
                <td style={{padding:"7px 10px", color:P.red                   }}>{cubic}</td>
                <td style={{padding:"7px 10px", color:P.green                 }}>{bbr}</td>
                <td style={{padding:"7px 10px", color:P.muted,  fontSize:"0.9em"}}>{why}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Colour palette ───────────────────────────────────────────────────────────
const P = {
  bg:    "#0d1117",
  panel: "#161b22",
  border:"#30363d",
  text:  "#e6edf3",
  muted: "#7d8590",
  accent:"#58a6ff",
  green: "#3fb950",
  yellow:"#d29922",
  red:   "#f85149",
  purple:"#bc8cff",
  cyan:  "#39d353",
};

const COLORS = [P.accent, P.green, P.yellow, P.red, P.purple];

// ─── Helpers ──────────────────────────────────────────────────────────────────
const fmtWin  = v => v >= 1024 ? `${(v/1024).toFixed(0)}MB` : `${v}KB`;
const fmtMbps = v => v >= 1000 ? `${(v/1000).toFixed(v>=10000?0:1)} Gbps` : `${v.toFixed(0)} Mbps`;

const Mono = ({children}) => (
  <code style={{fontFamily:"'JetBrains Mono',monospace", background:"#1c2333",
    padding:"1px 6px", borderRadius:4, color:P.cyan, fontSize:"0.85em"}}>{children}</code>
);

const Formula = ({children}) => (
  <div style={{background:"#1a2035", border:`1px solid #2d4263`, borderRadius:8,
    padding:"10px 18px", margin:"12px 0", fontFamily:"'JetBrains Mono',monospace",
    color:"#79c0ff", fontSize:"0.95em", letterSpacing:"0.02em"}}>{children}</div>
);

const SectionHeading = ({num, title}) => (
  <div style={{display:"flex", alignItems:"center", gap:12, margin:"40px 0 16px"}}>
    <div style={{width:28,height:28, borderRadius:"50%", background:P.accent,
      display:"flex",alignItems:"center",justifyContent:"center",
      color:P.bg, fontWeight:800, fontSize:"0.8em", flexShrink:0}}>{num}</div>
    <h2 style={{margin:0, color:P.text, fontSize:"1.15em", fontWeight:700,
      letterSpacing:"0.01em"}}>{title}</h2>
  </div>
);

const Tag = ({children, color=P.accent}) => (
  <span style={{background:color+"22", color, border:`1px solid ${color}44`,
    borderRadius:4, padding:"1px 8px", fontSize:"0.78em", fontWeight:600,
    letterSpacing:"0.04em", textTransform:"uppercase"}}>{children}</span>
);

// ─── Log scale toggle ─────────────────────────────────────────────────────────
// Returns recharts YAxis props for linear or log scale.
// minVal: safe minimum for log scale (must be > 0); label text for the axis.
function yAxisProps(logScale, minVal, labelText, extraProps={}) {
  return {
    stroke: P.muted,
    tick: { fontSize: 11 },
    scale: logScale ? "log" : "linear",
    domain: logScale ? [minVal, "auto"] : [0, "auto"],
    tickFormatter: v => v >= 1000 ? `${(v/1000).toFixed(v>=10000?0:1)}G` : `${v}`,
    label: {
      value: `${labelText}${logScale ? " (log)" : ""}`,
      angle: -90,
      position: "insideLeft",
      dx: labelText.length > 12 ? -14 : -8,
      fill: P.muted,
      fontSize: 11,
    },
    ...extraProps,
  };
}

const LogToggle = ({ value, onChange }) => (
  <button
    onClick={() => onChange(!value)}
    style={{
      background: value ? P.accent + "22" : "transparent",
      color: value ? P.accent : P.muted,
      border: `1px solid ${value ? P.accent + "66" : P.border}`,
      borderRadius: 5,
      padding: "2px 10px",
      fontSize: "0.72em",
      fontWeight: 700,
      cursor: "pointer",
      letterSpacing: "0.05em",
      textTransform: "uppercase",
      transition: "all 0.15s",
      fontFamily: "monospace",
    }}
  >
    log y
  </button>
);

// Chart header row: title on left, log toggle on right
const ChartHeader = ({ title, logY, setLogY }) => (
  <div style={{ display: "flex", justifyContent: "space-between",
    alignItems: "center", marginBottom: 12 }}>
    <div style={{ color: P.muted, fontSize: "0.78em", fontWeight: 600,
      letterSpacing: "0.08em", textTransform: "uppercase" }}>
      {title}
    </div>
    <LogToggle value={logY} onChange={setLogY} />
  </div>
);

// ─── SliderField: range slider + click-to-type value ────────────────────────
function SliderField({label, val, set, min, max, step, fmt, color}) {
  const [editing, setEditing] = useState(false);
  const [draft,   setDraft]   = useState("");

  const startEdit = () => { setDraft(String(val)); setEditing(true); };
  const commit    = () => {
    const n = parseFloat(draft);
    if (!isNaN(n)) set(Math.min(max, Math.max(min, n)));
    setEditing(false);
  };
  const onKey = e => {
    if (e.key === "Enter")  commit();
    if (e.key === "Escape") setEditing(false);
  };

  return (
    <div>
      <div style={{display:"flex", justifyContent:"space-between",
        alignItems:"center", marginBottom:5}}>
        <span style={{color:P.muted, fontSize:"0.8em"}}>{label}</span>
        {editing ? (
          <input autoFocus type="number" min={min} max={max} step={step}
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onBlur={commit} onKeyDown={onKey}
            style={{width:88, textAlign:"right", background:"#0d1117",
              border:`1px solid ${color}`, borderRadius:4, color,
              fontFamily:"monospace", fontSize:"0.85em", fontWeight:700,
              padding:"1px 4px", outline:"none"}} />
        ) : (
          <span onClick={startEdit} title="Click to type a value"
            style={{color, fontFamily:"monospace", fontSize:"0.85em",
              fontWeight:700, cursor:"text",
              borderBottom:`1px dashed ${color}55`, paddingBottom:1,
              userSelect:"none"}}>
            {fmt(val)}
          </span>
        )}
      </div>
      <input type="range" min={min} max={max} step={step} value={val}
        onChange={e => set(+e.target.value)}
        style={{width:"100%", accentColor:color}} />
    </div>
  );
}

function BdpCalc() {
  const [bw, setBw] = useState(1000);
  const [rtt, setRtt] = useState(80);
  const bdp = ((bw * 1e6 / 8) * (rtt / 1000));
  const util64k = Math.min(100, (65536 / bdp * 100)).toFixed(1);

  return (
    <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
      padding:"20px 24px", margin:"16px 0"}}>
      <div style={{color:P.muted, fontSize:"0.78em", fontWeight:600,
        letterSpacing:"0.08em", textTransform:"uppercase", marginBottom:16}}>
        Interactive BDP Calculator
      </div>
      <div style={{display:"flex", gap:24, flexWrap:"wrap", marginBottom:20}}>
        <div style={{flex:1, minWidth:160}}>
          <SliderField label="Bandwidth" val={bw} set={setBw}
            min={10} max={50000} step={10} color={P.accent}
            fmt={v => v>=1000 ? `${(v/1000).toFixed(v%1000===0?0:1)} Gbps` : `${v} Mbps`} />
        </div>
        <div style={{flex:1, minWidth:160}}>
          <SliderField label="RTT" val={rtt} set={setRtt}
            min={1} max={500} step={1} color={P.green}
            fmt={v => `${v} ms`} />
        </div>
      </div>
      <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(160px,1fr))", gap:12}}>
        {[
          {label:"BDP", value: bdp >= 1048576 ? `${(bdp/1048576).toFixed(2)} MB` : `${(bdp/1024).toFixed(1)} KB`, color:P.accent},
          {label:"64KB window utilisation", value:`${util64k}%`, color: +util64k < 50 ? P.red : P.green},
          {label:"Theoretical max", value: fmtMbps(bw), color:P.yellow},
          {label:"Window needed", value: bdp >= 1048576 ? `${(bdp/1048576).toFixed(2)} MB` : `${(bdp/1024).toFixed(0)} KB`, color:P.purple},
        ].map(({label,value,color}) => (
          <div key={label} style={{background:"#0d1117", borderRadius:8, padding:"10px 14px",
            border:`1px solid ${color}33`}}>
            <div style={{color:P.muted, fontSize:"0.72em", textTransform:"uppercase",
              letterSpacing:"0.06em", marginBottom:4}}>{label}</div>
            <div style={{color, fontWeight:800, fontSize:"1.2em",
              fontFamily:"'JetBrains Mono',monospace"}}>{value}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Main app ─────────────────────────────────────────────────────────────────
export default function App() {
  const bdpRows   = bdpData();
  const winRows   = throughputVsWindow();
  const cwndRows  = cwndData();
  const mathRows  = mathisData();
  const rttRows   = throughputVsRtt();

  // Per-chart log-Y toggle state
  const [logBdp,   setLogBdp]   = useState(true);   // BDP chart (already log)
  const [logTputRtt, setLogTputRtt] = useState(true); // new: throughput vs RTT in section 1
  const [logWin,   setLogWin]   = useState(false);  // throughput vs window
  const [logRtt,   setLogRtt]   = useState(true);   // throughput vs RTT (already log)
  const [logCwnd,  setLogCwnd]  = useState(false);  // cwnd sawtooth
  const [logMath,  setLogMath]  = useState(true);   // Mathis (already log)

  return (
    <div style={{background:P.bg, color:P.text, minHeight:"100vh",
      fontFamily:"'Inter','Segoe UI',sans-serif", maxWidth:900, margin:"0 auto",
      padding:"32px 20px 80px"}}>

      {/* Header */}
      <div style={{borderBottom:`1px solid ${P.border}`, paddingBottom:24, marginBottom:8}}>
        <div style={{display:"flex", gap:8, marginBottom:12, flexWrap:"wrap"}}>
          <Tag color={P.accent}>TCP Internals</Tag>
          <Tag color={P.green}>Networking</Tag>
          <Tag color={P.yellow}>Performance</Tag>
        </div>
        <h1 style={{margin:"0 0 10px", fontSize:"clamp(1.5em,4vw,2.2em)",
          fontWeight:800, letterSpacing:"-0.02em", lineHeight:1.2}}>
          TCP Kafka Tuning — Windows, Buffers & RTT
          <span style={{color:P.accent}}> → Throughput</span>
        </h1>
        <p style={{margin:0, color:P.muted, fontSize:"0.95em", maxWidth:650, lineHeight:1.6}}>
          Why a 1 Gbps link can deliver 4 Mbps, why satellite links are painful, and what
          actually limits your transfers. Interactive charts, formulas, and references.
        </p>
      </div>

      {/* ── 1. The Pipe Model ─────────────────────────────────────────────── */}
      <SectionHeading num="1" title="The Pipe Analogy — Bandwidth × Delay = Volume" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        Think of a network path as a physical pipe.{" "}
        <strong style={{color:P.text}}>Bandwidth</strong> is the pipe's diameter — how much data fits per second.{" "}
        <strong style={{color:P.text}}>RTT</strong> (Round-Trip Time) is the pipe's length — the time for a bit to travel to the receiver and back.
        Their product is the <strong style={{color:P.accent}}>Bandwidth-Delay Product (BDP)</strong>: the volume of data that can be simultaneously
        "in flight" in the pipe. To keep the pipe full, the TCP sender window must be at least this large.
      </p>

      <Formula>BDP (bytes) = Bandwidth (bits/s) × RTT (s) ÷ 8</Formula>
      <Formula>Max Throughput = Window Size (bytes) × 8 ÷ RTT (s)</Formula>

      <BdpCalc />

      <div style={{marginTop:8, color:P.muted, fontSize:"0.82em",
        fontStyle:"italic", paddingLeft:4}}>
        Example: 1 Gbps link, 100 ms RTT → BDP = 12.5 MB. Default 64 KB window → utilisation ≈ 0.5%.
      </div>

      {/* Chart: BDP vs RTT */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"20px 16px", margin:"20px 0"}}>
        <ChartHeader title="BDP (KB) vs RTT — by Bandwidth" logY={logBdp} setLogY={setLogBdp} />
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={bdpRows} margin={{top:4,right:20,bottom:20,left:20}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis dataKey="rtt" stroke={P.muted} tick={{fontSize:11}} label={{value:"RTT (ms)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />
            <YAxis {...yAxisProps(logBdp, 1, "BDP (KB)")} />
            <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:8, fontSize:"0.82em"}}
              formatter={(v,n) => [`${v>=1024?(v/1024).toFixed(1)+'MB':v.toFixed(0)+'KB'}`, n]} />
            <Legend wrapperStyle={{fontSize:"0.8em", paddingTop:8}} />
            {["100Mbps","1000Mbps","10000Mbps","50000Mbps"].map((k,i) => (
              <Line key={k} type="monotone" dataKey={k} dot={false} strokeWidth={2} stroke={COLORS[i]} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart: Max Throughput vs RTT — paired with BDP chart above */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"20px 16px", margin:"20px 0"}}>
        <ChartHeader title="Max Throughput (Mbps) vs RTT — by Window Size" logY={logTputRtt} setLogY={setLogTputRtt} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          The inverse view of BDP: for a fixed window size, throughput falls hyperbolically
          as RTT rises — T = W / RTT (F1). Each line is a different window size.
          The 64 KB default (dashed red) becomes the bottleneck on any high-latency path.
        </div>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={rttRows} margin={{top:4,right:20,bottom:20,left:20}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis dataKey="rtt" stroke={P.muted} tick={{fontSize:11}}
              label={{value:"RTT (ms)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />
            <YAxis {...yAxisProps(logTputRtt, 0.1, "Throughput (Mbps)")} />
            <Tooltip contentStyle={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:8,fontSize:"0.82em"}}
              formatter={(v,n)=>[`${v.toFixed(1)} Mbps`, `Win=${n}`]} />
            <Legend wrapperStyle={{fontSize:"0.8em",paddingTop:8}} />
            {["64KB","256KB","1024KB","4096KB","16384KB"].map((k,i) => (
              <Line key={k} type="monotone" dataKey={k} dot={false}
                strokeWidth={k==="64KB" ? 1.5 : 2}
                strokeDasharray={k==="64KB" ? "4 3" : undefined}
                stroke={k==="64KB" ? P.red : COLORS[i+1]} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* ── 2. Window as the Bottleneck ───────────────────────────────────── */}
      <SectionHeading num="2" title="The Receive Window (rwnd) — RFC 793 / RFC 1323" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        The original TCP header uses a 16-bit window field, capping the receiver-advertised window
        (<Mono>rwnd</Mono>) at 65,535 bytes. <strong style={{color:P.text}}>RFC 1323 (1992)</strong> added the
        Window Scale option, allowing shifts up to 14 bits (window × 2¹⁴ = up to 1 GB).
        Modern OSes negotiate this during the SYN/SYN-ACK handshake and auto-tune buffers.
        On Linux, <Mono>net.ipv4.tcp_rmem</Mono> and <Mono>tcp_wmem</Mono> define the per-socket buffer range.
      </p>

      <Formula>Effective Window = min(cwnd, rwnd)</Formula>

      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"20px 16px", margin:"20px 0"}}>
        <ChartHeader title="Max Throughput (Mbps) vs Window Size — by RTT" logY={logWin} setLogY={setLogWin} />
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={winRows} margin={{top:4,right:20,bottom:20,left:20}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis dataKey="win" stroke={P.muted} tick={{fontSize:11}}
              tickFormatter={fmtWin}
              label={{value:"Window Size (KB)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />
            <YAxis {...yAxisProps(logWin, 0.1, "Throughput (Mbps)")} />
            <ReferenceLine x={64} stroke={P.red} strokeDasharray="4 4" label={{value:"64KB limit",fill:P.red,fontSize:10,position:"top"}} />
            <Tooltip contentStyle={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:8,fontSize:"0.82em"}}
              labelFormatter={fmtWin} formatter={(v,n)=>[`${v.toFixed(0)} Mbps`, `RTT=${n}`]} />
            <Legend wrapperStyle={{fontSize:"0.8em",paddingTop:8}} />
            {["5ms","20ms","80ms","200ms"].map((k,i) => (
              <Line key={k} type="monotone" dataKey={k} dot={false} strokeWidth={2} stroke={COLORS[i]} />
            ))}
          </LineChart>
        </ResponsiveContainer>
        <div style={{color:P.muted, fontSize:"0.8em", marginTop:8, paddingLeft:4}}>
          The red dashed line marks the original 64 KB hard limit. Note how the 200 ms RTT line barely
          reaches ~2.5 Mbps even at full 64 KB — the classic "fat dumb pipe" problem.
        </div>
      </div>

      {/* ── 3. Throughput vs RTT ──────────────────────────────────────────── */}
      <SectionHeading num="3" title="Throughput vs RTT — Window as the Ceiling" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        For any fixed window size, throughput degrades hyperbolically with RTT. High-RTT paths (satellite: 600 ms+,
        transcontinental: 150–200 ms) require proportionally larger windows to sustain the same throughput.
        This is why protocols like <strong style={{color:P.text}}>QUIC</strong> and tuned TCP variants
        matter so much for WAN/CDN performance.
      </p>

      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"20px 16px", margin:"20px 0"}}>
        <ChartHeader title="Throughput (Mbps) vs RTT — by Window Size" logY={logRtt} setLogY={setLogRtt} />
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={rttRows} margin={{top:4,right:20,bottom:20,left:20}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis dataKey="rtt" stroke={P.muted} tick={{fontSize:11}}
              label={{value:"RTT (ms)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />
            <YAxis {...yAxisProps(logRtt, 0.1, "Throughput (Mbps)")} />
            <Tooltip contentStyle={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:8,fontSize:"0.82em"}}
              formatter={(v,n)=>[`${v.toFixed(1)} Mbps`, `Win=${n}`]} />
            <Legend wrapperStyle={{fontSize:"0.8em",paddingTop:8}} />
            {["64KB","256KB","1024KB","4096KB","16384KB"].map((k,i) => (
              <Line key={k} type="monotone" dataKey={k} dot={false} strokeWidth={2} stroke={COLORS[i]} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* ── 4. Congestion Window ──────────────────────────────────────────── */}
      <SectionHeading num="4" title="The Congestion Window (cwnd) — Slow Start & AIMD" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        Beyond <Mono>rwnd</Mono>, the <strong style={{color:P.text}}>sender</strong> imposes its own window:
        the congestion window (<Mono>cwnd</Mono>), governed by RFC 5681. Effective throughput is{" "}
        <Mono>min(cwnd, rwnd)</Mono> / RTT. There are four phases:
      </p>

      <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))", gap:10, margin:"14px 0 20px"}}>
        {[
          {color:P.green,  label:"Slow Start",         desc:"cwnd doubles each RTT (exponential). Starts at IW ≈ 4 × MSS per RFC 3390."},
          {color:P.accent, label:"Congestion Avoidance",desc:"cwnd += 1 MSS per RTT (linear). AIMD: Additive Increase, Multiplicative Decrease."},
          {color:P.red,    label:"Fast Retransmit",     desc:"3 duplicate ACKs → retransmit without waiting for timeout."},
          {color:P.yellow, label:"Fast Recovery",       desc:"ssthresh = cwnd/2; cwnd = ssthresh (not back to 1). Avoids slow start."},
        ].map(({color,label,desc}) => (
          <div key={label} style={{background:P.panel, border:`1px solid ${color}44`, borderRadius:8, padding:"12px 14px"}}>
            <div style={{color, fontWeight:700, fontSize:"0.85em", marginBottom:6}}>{label}</div>
            <div style={{color:P.muted, fontSize:"0.8em", lineHeight:1.5}}>{desc}</div>
          </div>
        ))}
      </div>

      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"20px 16px", margin:"20px 0"}}>
        <ChartHeader title="cwnd Evolution — Slow Start + Sawtooth (TCP Reno)" logY={logCwnd} setLogY={setLogCwnd} />
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={cwndRows} margin={{top:4,right:20,bottom:20,left:20}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis dataKey="time" stroke={P.muted} tick={{fontSize:11}}
              label={{value:"RTT (round trips)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />
            <YAxis {...yAxisProps(logCwnd, 0.5, "Window (MSS)")} />
            <Tooltip contentStyle={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:8,fontSize:"0.82em"}}
              formatter={(v,n)=>[`${typeof v==="number"?v.toFixed(1):v} MSS`, n]} />
            <Legend wrapperStyle={{fontSize:"0.8em",paddingTop:8}} />
            <Line type="monotone" dataKey="cwnd" dot={false} strokeWidth={2.5} stroke={P.accent} name="cwnd" />
            <Line type="monotone" dataKey="ssthresh" dot={false} strokeWidth={1.5} stroke={P.yellow} strokeDasharray="6 3" name="ssthresh" />
          </LineChart>
        </ResponsiveContainer>
        <div style={{display:"flex", gap:16, marginTop:8, flexWrap:"wrap"}}>
          {[
            {color:P.green, text:"RTT 0–28: Slow start (exponential)"},
            {color:P.accent, text:"Linear growth (congestion avoidance)"},
            {color:P.red, text:"RTT 28: Timeout — cwnd → 1"},
            {color:P.yellow, text:"RTT 48: 3 dup-ACKs — cwnd → ssthresh"},
          ].map(({color,text}) => (
            <div key={text} style={{display:"flex",alignItems:"center",gap:6,fontSize:"0.78em",color:P.muted}}>
              <div style={{width:12,height:3,background:color,borderRadius:2,flexShrink:0}} />{text}
            </div>
          ))}
        </div>
      </div>

      {/* ── 5. Mathis Equation ────────────────────────────────────────────── */}
      <SectionHeading num="5" title="Packet Loss — The Mathis Equation" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        With packet loss <em>p</em>, the achievable throughput is bounded not by the window, but by loss recovery speed.
        The <strong style={{color:P.text}}>Mathis et al. (1997)</strong> macroscopic model gives:
      </p>

      <Formula>{"Throughput ≤ (MSS / RTT) × (1 / √p)"}</Formula>

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        At 1% loss on a 50 ms path (MSS=1460B): max ≈ 2.3 Mbps — <em>regardless of bandwidth</em>.
        At 0.001% loss that rises to 73 Mbps. This is why random loss on Wi-Fi or a congested WAN
        is so damaging, and why BBR/CUBIC attempt to decouple congestion signalling from loss.
      </p>

      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"20px 16px", margin:"20px 0"}}>
        <ChartHeader title="Mathis Throughput Bound — RTT=50ms, MSS=1460B" logY={logMath} setLogY={setLogMath} />
        <ResponsiveContainer width="100%" height={230}>
          <AreaChart data={mathRows} margin={{top:4,right:20,bottom:22,left:20}}>
            <defs>
              <linearGradient id="mathGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={P.red} stopOpacity={0.3} />
                <stop offset="100%" stopColor={P.red} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis dataKey="p" stroke={P.muted} tick={{fontSize:11}}
              tickFormatter={v=>`1e${v.toFixed(0)}`}
              label={{value:"Packet loss probability (log₁₀)", position:"insideBottom", dy:14, fill:P.muted, fontSize:11}} />
            <YAxis {...yAxisProps(logMath, 0.01, "Max Throughput (Mbps)")} />
            <Tooltip contentStyle={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:8,fontSize:"0.82em"}}
              labelFormatter={v=>`p = 10^${(+v).toFixed(1)}`}
              formatter={v=>[`${v.toFixed(2)} Mbps`,"Throughput bound"]} />
            <Area type="monotone" dataKey="throughput" stroke={P.red} fill="url(#mathGrad)" strokeWidth={2} name="Throughput bound" dot={false} />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* ── 6. Practical Table ────────────────────────────────────────────── */}
      <SectionHeading num="6" title="Quick Reference: Scenarios" />

      <div style={{overflowX:"auto", margin:"8px 0 24px"}}>
        <table style={{width:"100%", borderCollapse:"collapse", fontSize:"0.85em"}}>
          <thead>
            <tr style={{borderBottom:`2px solid ${P.border}`}}>
              {["Scenario","Bandwidth","RTT","BDP","64KB util","Action needed"].map(h => (
                <th key={h} style={{padding:"8px 12px", textAlign:"left", color:P.muted,
                  fontWeight:600, letterSpacing:"0.04em", fontSize:"0.9em"}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[
              ["50GbE DC", "50 Gbps", "2 ms", "12.5 MB", "0.5%", "Huge buffers + BBR + FQ qdisc"],
              ["LAN", "1 Gbps", "0.5 ms", "62 KB", "~100%", "No change needed"],
              ["Office WAN", "100 Mbps", "20 ms", "250 KB", "26%", "Tune buffers"],
              ["Datacenter", "10 Gbps", "5 ms", "6.25 MB", "1%", "Large buffers + CUBIC/BBR"],
              ["Trans-Pacific", "1 Gbps", "180 ms", "22.5 MB", "0.3%", "Large buffers, Window Scale"],
              ["Satellite", "50 Mbps", "600 ms", "3.75 MB", "1.7%", "Performance Enhancing Proxy"],
              ["VPN / WireGuard", "500 Mbps", "15 ms", "937 KB", "7%", "Increase socket buffers"],
            ].map(([s,bw,rtt,bdp,u,a],i) => (
              <tr key={s} style={{background: i%2===0 ? "transparent" : "#161b22",
                borderBottom:`1px solid ${P.border}`}}>
                <td style={{padding:"9px 12px", color:P.text, fontWeight:600}}>{s}</td>
                <td style={{padding:"9px 12px", color:P.cyan, fontFamily:"monospace"}}>{bw}</td>
                <td style={{padding:"9px 12px", color:P.green, fontFamily:"monospace"}}>{rtt}</td>
                <td style={{padding:"9px 12px", color:P.yellow, fontFamily:"monospace"}}>{bdp}</td>
                <td style={{padding:"9px 12px", color: +u.replace("%","") < 10 ? P.red : +u.replace("%","") < 50 ? P.yellow : P.green}}>{u}</td>
                <td style={{padding:"9px 12px", color:P.muted, fontSize:"0.9em"}}>{a}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ── 7. Linux tuning ───────────────────────────────────────────────── */}
      <SectionHeading num="7" title="Linux Tuning Cheatsheet" />

      <div style={{background:"#0d1117", border:`1px solid #30363d`, borderRadius:8,
        padding:"16px 20px", margin:"8px 0", fontFamily:"'JetBrains Mono',monospace",
        fontSize:"0.8em", lineHeight:1.8, overflowX:"auto"}}>
        <div style={{color:P.muted}}># BDP = bandwidth × RTT / 8</div>
        <div style={{color:P.muted}}># e.g. 50 Gbps × 10 ms = 62.5 MB → round up to 128 MB</div>
        <br/>
        <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.core.rmem_max=134217728</span></div>
        <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.core.wmem_max=134217728</span></div>
        <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.ipv4.tcp_rmem=<span style={{color:P.yellow}}>"4096 1048576 134217728"</span></span></div>
        <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.ipv4.tcp_wmem=<span style={{color:P.yellow}}>"4096 1048576 134217728"</span></span></div>
        <br/>
        <div style={{color:P.muted}}># Verify window scaling in use:</div>
        <div><span style={{color:P.green}}>ss</span>{" "}<span style={{color:P.text}}>-timn | grep wscale</span></div>
        <br/>
        <div style={{color:P.muted}}># Modern congestion control (BBR recommended on high-BDP paths)</div>
        <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.ipv4.tcp_congestion_control=<span style={{color:P.yellow}}>bbr</span></span></div>
        <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.core.default_qdisc=<span style={{color:P.yellow}}>fq</span></span></div>
      </div>

      {/* ── 8. BBR vs CUBIC ──────────────────────────────────────────────── */}
      <SectionHeading num="8" title="BBR vs CUBIC — Congestion Control Comparison" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        CUBIC (the Linux default) uses <strong style={{color:P.text}}>packet loss</strong> as
        its congestion signal — it must overflow the switch buffer before it knows it has sent
        too fast. This produces the characteristic sawtooth window pattern and inflates RTT for
        every flow sharing that buffer.{" "}
        <strong style={{color:P.text}}>BBR</strong> (Cardwell et al., 2016) instead continuously
        estimates <Mono>BtlBw</Mono> (bottleneck bandwidth) and <Mono>RTprop</Mono>{" "}
        (propagation delay), targeting exactly BDP bytes in flight — no queue, no loss required.
        Adjust the sliders to see how path parameters change the behaviour.
      </p>

      <BbrComparison />

      {/* ── 9. References ────────────────────────────────────────────────── */}
      <SectionHeading num="9" title="References & Standards" />

      <div style={{display:"grid", gap:8, margin:"8px 0"}}>
        {[
          {tag:"Little 1961",  year:1961, title:"A proof for the queuing formula L = λW — Little, J.D.C., Operations Research 9(3)",
           url:"https://doi.org/10.1287/opre.9.3.383"},
          {tag:"RFC 793",      year:1981, title:"Transmission Control Protocol — Postel",
           url:"https://www.rfc-editor.org/rfc/rfc793"},
          {tag:"RFC 879",      year:1983, title:"TCP Maximum Segment Size and Related Topics — Postel",
           url:"https://www.rfc-editor.org/rfc/rfc879"},
          {tag:"RFC 1191",     year:1990, title:"Path MTU Discovery — Mogul & Deering",
           url:"https://www.rfc-editor.org/rfc/rfc1191"},
          {tag:"RFC 1323",     year:1992, title:"TCP Extensions for High Performance — Jacobson, Braden, Borman",
           url:"https://www.rfc-editor.org/rfc/rfc1323"},
          {tag:"Jacobson 1988",year:1988, title:"Congestion Avoidance and Control — Jacobson, ACM SIGCOMM",
           url:"https://doi.org/10.1145/52324.52356"},
          {tag:"Chiu & Jain 1989",year:1989, title:"Analysis of the Increase and Decrease Algorithms for Congestion Avoidance — Computer Networks & ISDN Systems 17(1)",
           url:"https://doi.org/10.1016/0169-7552(89)90019-6"},
          {tag:"RFC 3390",     year:2002, title:"Increasing TCP's Initial Window — Allman et al.",
           url:"https://www.rfc-editor.org/rfc/rfc3390"},
          {tag:"Mathis 1997",  year:1997, title:"The Macroscopic Behavior of the TCP Congestion Avoidance Algorithm — Mathis, Semke, Mahdavi, Ott; ACM SIGCOMM CCR 27(3)",
           url:"https://doi.org/10.1145/263932.264023"},
          {tag:"Padhye 1998",  year:1998, title:"Modeling TCP Throughput: A Simple Model and its Empirical Validation — Padhye et al., ACM SIGCOMM",
           url:"https://doi.org/10.1145/285237.285291"},
          {tag:"RFC 4898",     year:2007, title:"TCP Extended Statistics MIB",
           url:"https://www.rfc-editor.org/rfc/rfc4898"},
          {tag:"Ha 2008",      year:2008, title:"CUBIC: A New TCP-Friendly High-Speed TCP Variant — Ha, Rhee, Xu; ACM SIGOPS OSR 42(5)",
           url:"https://doi.org/10.1145/1400097.1400105"},
          {tag:"RFC 5681",     year:2009, title:"TCP Congestion Control — Allman, Paxson, Blanton",
           url:"https://www.rfc-editor.org/rfc/rfc5681"},
          {tag:"RFC 7323",     year:2014, title:"TCP Extensions for High Performance (obsoletes RFC 1323) — Borman, Braden, Jacobson, Scheffenegger",
           url:"https://www.rfc-editor.org/rfc/rfc7323"},
          {tag:"BBR 2016",     year:2016, title:"BBR: Congestion-Based Congestion Control — Cardwell, Cheng, Gunn, Yeganeh, Jacobson; ACM Queue 14(5)",
           url:"https://queue.acm.org/detail.cfm?id=3022184"},
        ].map(({tag,year,title,url}) => (
          <a key={tag} href={url} target="_blank" rel="noreferrer"
            style={{display:"flex", alignItems:"center", gap:12, background:P.panel,
              border:`1px solid ${P.border}`, borderRadius:8, padding:"10px 14px",
              textDecoration:"none", transition:"border-color 0.15s"}}
            onMouseOver={e=>e.currentTarget.style.borderColor=P.accent}
            onMouseOut={e=>e.currentTarget.style.borderColor=P.border}>
            <div style={{background:P.accent+"22", color:P.accent, borderRadius:5,
              padding:"3px 8px", fontSize:"0.75em", fontFamily:"monospace",
              fontWeight:700, flexShrink:0, whiteSpace:"nowrap"}}>{tag}</div>
            <div style={{flex:1, color:P.text, fontSize:"0.85em"}}>{title}</div>
            <div style={{color:P.muted, fontSize:"0.75em", flexShrink:0}}>{year}</div>
          </a>
        ))}
      </div>

      <div style={{marginTop:40, paddingTop:20, borderTop:`1px solid ${P.border}`,
        color:P.muted, fontSize:"0.78em", lineHeight:1.6}}>
        All charts computed analytically from the cited formulas. No packet simulation.
        BDP (F2): RFC 1323 §1. Throughput (F1): Little (1961). Mathis bound (F4): Mathis et al. (1997)
        — T = MSS/(RTT×√p), result in bytes/sec, converted to Mbit/s by dividing by 125,000.
        cwnd sawtooth: TCP Reno per RFC 5681. BBR phase cycle: Cardwell et al. (2016) §4.
      </div>
    </div>
  );
}
