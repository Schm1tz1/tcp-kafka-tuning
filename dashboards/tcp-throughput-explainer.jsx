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

// ─── Competing congestion control simulation ─────────────────────────────────
// Reproduces the qualitative findings of Zhao, Peters, Chung & Claypool (2022),
// "Competing TCP Congestion Control Algorithms over a Satellite Network", IEEE
// CCNC. A selected competitor flow (BBR / PCC / Hybla / Cubic) shares ONE
// bottleneck with a default Cubic flow. Window dynamics are modelled
// qualitatively — not a full ns-3 trace — and Jain's fairness is computed per
// phase (start-up / steady state / overall) so the sim reproduces the paper's
// headline result: BBR dominates Cubic, Cubic dominates PCC, Hybla dominates
// start-up but is fair in steady state, and Cubic is fair to another Cubic.
// CUBIC:  loss-based AIMD, proportional-share decrease (Ha, Rhee, Xu 2008).
// BBR:    BtlBw×RTprop model, ignores loss (Cardwell et al. 2016).
// PCC:    Vivace-Latency utility, yields on rising delay (Dong et al. 2018).
// Hybla:  RTT-compensated growth, ρ=RTT/RTT₀ (Caini & Firrincieli 2004).
function simCompetition(competitor, bwMbps, rttMs, bufMss) {
  const ROUNDS = 70, RTT0 = 25, MSS = 1460;
  const bdpMss   = Math.max(1, Math.round((bwMbps * 1e6 / 8) * (rttMs / 1000) / MSS));
  const bufLimit = bdpMss + bufMss;

  let aCwnd = 2, aSsthresh = Math.round(bdpMss * 1.5), aStart = true;
  let bCwnd = 2, bSsthresh = Math.round(bdpMss * 1.5);

  const rows = [];
  for (let t = 0; t < ROUNDS; t++) {
    const total  = aCwnd + bCwnd;
    const queue  = Math.max(0, total - bdpMss);            // shared bottleneck queue
    const over   = total > bufLimit;                       // buffer overflow → loss
    const rtt    = rttMs * (1 + Math.min(queue, bufMss * 2) / Math.max(1, bdpMss));
    const sat    = total >= bdpMss;                        // link saturated → share by window
    const aShare = total > 0 ? aCwnd / total : 0.5;
    const bShare = total > 0 ? bCwnd / total : 0.5;
    const aTput  = sat ? bwMbps * aShare : (aCwnd * MSS * 8) / (rtt / 1000) / 1e6;
    const bTput  = sat ? bwMbps * bShare : (bCwnd * MSS * 8) / (rtt / 1000) / 1e6;

    rows.push({
      t,
      cwnd_comp:  Math.round(aCwnd),
      cwnd_cubic: Math.round(bCwnd),
      rtt:        +rtt.toFixed(1),
      queue:      Math.round(queue),
      tput_comp:  +aTput.toFixed(1),
      tput_cubic: +bTput.toFixed(1),
      loss_cubic: over ? Math.round(bCwnd) : null,
      loss_comp:  (over && competitor !== "bbr") ? Math.round(aCwnd) : null,
    });

    // default CUBIC flow B — proportional-share decrease (AQM drops ∝ throughput)
    if      (over)              bCwnd = Math.max(2, bCwnd * (1 - 0.5 * bShare));
    else if (bCwnd < bSsthresh) bCwnd = Math.min(bCwnd * 2, bSsthresh);
    else                        bCwnd = bCwnd + 1;

    // competitor flow A
    if (competitor === "cubic") {
      if      (over)              aCwnd = Math.max(2, aCwnd * (1 - 0.5 * aShare));
      else if (aCwnd < aSsthresh) aCwnd = Math.min(aCwnd * 2, aSsthresh);
      else                        aCwnd = aCwnd + 1;
    } else if (competitor === "hybla") {
      const rho = Math.max(1, Math.min(rtt / RTT0, 24));   // RTT-compensation factor
      if      (over)              aCwnd = Math.max(2, aCwnd * (1 - 0.5 * aShare));
      else if (aCwnd < aSsthresh) aCwnd = Math.min(aCwnd * Math.pow(2, rho), bufLimit + 4); // SS ×2^ρ
      else                        aCwnd = Math.min(aCwnd + rho * rho,        bufLimit + 4); // CA +ρ²
    } else if (competitor === "bbr") {
      if      (aStart)        { aCwnd *= 2; if (aCwnd >= 2 * bdpMss) aStart = false; }       // startup gain
      else if (t % 30 === 29) aCwnd = Math.max(4, Math.round(bdpMss * 0.5));                 // probe_rtt
      else                    aCwnd = Math.min(aCwnd + Math.max(1, Math.round(bdpMss * 0.1)), 2 * bdpMss);
    } else if (competitor === "pcc") {
      if      (aStart)               { aCwnd *= 2; if (aCwnd >= bdpMss) aStart = false; }
      else if (queue > 0.02 * bdpMss) aCwnd = Math.max(2, aCwnd * 0.88);                     // latency penalty
      else                            aCwnd = aCwnd + Math.max(1, Math.round(bdpMss * 0.02));// probe up
    }
  }

  const jain = (rs) => {
    const a = rs.reduce((s, r) => s + r.tput_comp,  0) / rs.length;
    const b = rs.reduce((s, r) => s + r.tput_cubic, 0) / rs.length;
    const f = (a + b) > 0 ? ((a + b) ** 2) / (2 * (a * a + b * b)) : 1;
    return { f: +f.toFixed(2), comp: +a.toFixed(0), cubic: +b.toFixed(0) };
  };
  const fairness = {
    startup: jain(rows.slice(0, 10)),
    steady:  jain(rows.slice(35)),
    overall: jain(rows),
  };
  return { rows, fairness, bdpMss, bufLimit };
}

// Algorithm metadata + measured results from Zhao et al. (2022) Fig 7.
// fair = Jain's index [start-up, steady, overall]; diff = overall throughput
// difference vs Cubic in Mb/s (+ Cubic gets more, − competitor gets more).
function competitorMeta(P) {
  return {
    bbr:   { name:"BBR",   color:P.green,  tag:"bandwidth-est",
      blurb:"Models BtlBw×RTprop and ignores packet loss. It ramps past Cubic and holds ~2×BDP, so Cubic's loss-driven backoff cedes the link — BBR dominates both start-up and steady state.",
      fair:[0.93,0.53,0.55], diff:-91.61, note:"BBR dominates Cubic in both phases (disregards loss)" },
    pcc:   { name:"PCC",   color:P.purple, tag:"utility-fn",
      blurb:"The Vivace-Latency utility penalises rising delay, so PCC reduces its rate whenever the queue builds. Cubic ignores delay and fills the buffer to loss, so Cubic dominates steady state.",
      fair:[0.98,0.85,0.85], diff:43.97,  note:"Cubic dominates PCC in steady state (PCC cuts rate on delay)" },
    hybla: { name:"Hybla", color:P.yellow, tag:"satellite-opt",
      blurb:"Scales window growth by ρ=RTT/RTT₀ to erase RTT bias. On high-latency paths it fills the pipe almost instantly — dominating start-up — then behaves like loss-based TCP and shares fairly in steady state.",
      fair:[0.51,0.99,0.92], diff:-33.21, note:"Hybla dominates start-up, fair in steady state" },
    cubic: { name:"Cubic", color:P.accent, tag:"loss-based",
      blurb:"A second default Cubic flow — the fairness baseline. Two Cubic flows share the bottleneck equally in every phase (Jain ≈ 1.0).",
      fair:[1.00,0.99,0.99], diff:7.31,   note:"Fair to another Cubic flow in all phases" },
  };
}

// ─── Competing Congestion Control Interactive Component ───────────────────────
const fairColor = f => f >= 0.9 ? P.green : f >= 0.7 ? P.yellow : P.red;

function CongestionCompetition() {
  const [competitor, setCompetitor] = useState("bbr");
  const [bw,  setBw]  = useState(144);    // Mbps — satellite-scale default (paper testbed)
  const [rtt, setRtt] = useState(600);    // ms
  const [buf, setBuf] = useState(80);     // switch buffer in MSS

  // Per-chart log scale toggles
  const [logCwnd,  setLogCwnd]  = useState(false);
  const [logRtt2,  setLogRtt2]  = useState(false);
  const [logQueue, setLogQueue] = useState(false);
  const [logTput,  setLogTput]  = useState(false);

  const ALGOS = competitorMeta(P);
  const algo  = ALGOS[competitor];

  const { rows, fairness, bdpMss, bufLimit } = simCompetition(competitor, bw, rtt, buf);
  const compLossEvents = rows.filter(r => r.loss_comp  !== null).length;
  const cubicLossEvents = rows.filter(r => r.loss_cubic !== null).length;

  const grid = <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />;
  const xAx  = <XAxis dataKey="t" stroke={P.muted} tick={{fontSize:10}}
    label={{value:"Round trips", position:"insideBottom", dy:13, fill:P.muted, fontSize:10}} />;
  const tipStyle = {background:P.panel, border:`1px solid ${P.border}`,
    borderRadius:8, fontSize:"0.8em"};

  return (
    <div>
      {/* Competitor selector */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 20px", margin:"16px 0"}}>
        <div style={{color:P.muted, fontSize:"0.78em", fontWeight:600,
          letterSpacing:"0.08em", textTransform:"uppercase", marginBottom:12}}>
          Algorithm competing against default Cubic
        </div>
        <div style={{display:"flex", gap:8, flexWrap:"wrap", marginBottom:14}}>
          {Object.entries(ALGOS).map(([id, a]) => (
            <button key={id} onClick={() => setCompetitor(id)}
              style={{background: competitor===id ? a.color+"22" : "transparent",
                color: competitor===id ? a.color : P.muted,
                border:`1px solid ${competitor===id ? a.color+"88" : P.border}`,
                borderRadius:7, padding:"7px 16px", cursor:"pointer",
                fontWeight:700, fontSize:"0.86em", transition:"all 0.15s"}}>
              {a.name} <span style={{fontSize:"0.78em", opacity:0.7, fontWeight:500}}>{a.tag}</span>
            </button>
          ))}
        </div>
        <div style={{color:P.text, fontSize:"0.84em", lineHeight:1.65,
          borderLeft:`3px solid ${algo.color}`, paddingLeft:12}}>
          <strong style={{color:algo.color}}>{algo.name}</strong> vs Cubic — {algo.blurb}
        </div>

        {/* Sliders */}
        <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",
          gap:20, marginTop:18}}>
          {[
            {label:"Bottleneck bandwidth", val:bw,  set:setBw,  min:10,  max:50000, step:10,
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
            {label:"Buffer limit", value:`${bufLimit} MSS (BDP + ${buf} MSS)`, color:P.yellow},
            {label:`${algo.name} avg throughput`, value:`${fairness.overall.comp} Mbps`, color:algo.color},
            {label:"Cubic avg throughput", value:`${fairness.overall.cubic} Mbps`, color:P.red},
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

      {/* Jain fairness (computed from simulation) */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"16px 20px", margin:"12px 0"}}>
        <div style={{color:P.muted, fontSize:"0.78em", fontWeight:600,
          letterSpacing:"0.08em", textTransform:"uppercase", marginBottom:4}}>
          Jain&apos;s fairness index — computed from this simulation
        </div>
        <div style={{color:P.muted, fontSize:"0.78em", marginBottom:12, lineHeight:1.5}}>
          f(x₁,x₂) = (x₁+x₂)² / 2(x₁²+x₂²) — ranges ½ (one flow starves the other)
          to 1 (perfectly equal share). Start-up = first 10 RTTs, steady = last half.
        </div>
        <div style={{display:"flex", gap:14, flexWrap:"wrap"}}>
          {[["Start-up",fairness.startup.f],["Steady state",fairness.steady.f],["Overall",fairness.overall.f]]
            .map(([label,f])=>(
            <div key={label} style={{flex:"1 1 120px", background:"#0d1117", borderRadius:8,
              padding:"10px 14px", border:`1px solid ${fairColor(f)}44`}}>
              <div style={{color:P.muted, fontSize:"0.72em", textTransform:"uppercase",
                letterSpacing:"0.05em"}}>{label}</div>
              <div style={{color:fairColor(f), fontFamily:"monospace", fontWeight:800,
                fontSize:"1.4em"}}>{f.toFixed(2)}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Chart 1: cwnd */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Congestion window (cwnd) — MSS" logY={logCwnd} setLogY={setLogCwnd} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          Both flows share one bottleneck. Watch how {algo.name}&apos;s window grows relative
          to the default Cubic flow — divergence here is what drives the throughput split.
          Dots mark loss (buffer-overflow) events.
        </div>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logCwnd, 0.5, "cwnd (MSS)", {tick:{fontSize:10}, label:{...yAxisProps(logCwnd,0.5,"cwnd (MSS)").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle}
              formatter={(v,n)=>[typeof v==="number"?`${v} MSS`:v, n]} />
            <ReferenceLine y={bdpMss}   stroke={P.accent} strokeDasharray="4 3"
              label={{value:"BDP", fill:P.accent, fontSize:10, position:"insideTopRight"}} />
            <ReferenceLine y={bufLimit} stroke={P.red}    strokeDasharray="2 4"
              label={{value:"buffer limit", fill:P.red, fontSize:10, position:"insideTopRight"}} />
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Line type="monotone" dataKey="cwnd_comp"  name={algo.name}
              dot={false} strokeWidth={2} stroke={algo.color} />
            <Line type="monotone" dataKey="cwnd_cubic" name="Cubic (default)"
              dot={false} strokeWidth={2} stroke={P.red} />
            <Line type="monotone" dataKey="loss_cubic" name="Cubic loss"
              dot={{r:3.5, fill:P.red, stroke:"#fff", strokeWidth:1}}
              activeDot={false} stroke="none" legendType="circle" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 2: shared path RTT */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Path RTT — ms (shared bottleneck)" logY={logRtt2} setLogY={setLogRtt2} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          Both flows traverse the same queue, so they observe the same RTT. Loss-based and
          utility flows that fill the buffer inflate this well above RTprop; latency-aware
          flows (PCC) and zero-queue models keep it near the propagation baseline.
        </div>
        <ResponsiveContainer width="100%" height={190}>
          <LineChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logRtt2, 0.1, "RTT (ms)", {tick:{fontSize:10}, label:{...yAxisProps(logRtt2,0.1,"RTT (ms)").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle} formatter={(v,n)=>[`${v} ms`, n]} />
            <ReferenceLine y={rtt} stroke={P.accent} strokeDasharray="4 3"
              label={{value:"RTprop", fill:P.accent, fontSize:10, position:"insideTopRight"}} />
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Line type="monotone" dataKey="rtt" name="Path RTT"
              dot={false} strokeWidth={2} stroke={P.cyan} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 3: shared queue depth */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Bottleneck queue depth — MSS" logY={logQueue} setLogY={setLogQueue} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          The shared queue is the contended resource. A flow that keeps it full claims more
          bandwidth but punishes latency for everyone; a flow that drains it sacrifices
          throughput when competing against a buffer-filling neighbour.
        </div>
        <ResponsiveContainer width="100%" height={180}>
          <AreaChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logQueue, 0.1, "Queue (MSS)", {tick:{fontSize:10}, label:{...yAxisProps(logQueue,0.1,"Queue (MSS)").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle} formatter={(v,n)=>[`${v} MSS`, n]} />
            <defs>
              <linearGradient id="qshared" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={P.yellow} stopOpacity={0.3}/>
                <stop offset="95%" stopColor={P.yellow} stopOpacity={0}/>
              </linearGradient>
            </defs>
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Area type="monotone" dataKey="queue" name="Shared queue"
              stroke={P.yellow} fill="url(#qshared)" strokeWidth={1.5} />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Chart 4: Throughput split */}
      <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 16px", margin:"12px 0"}}>
        <ChartHeader title="Throughput split — Mbps" logY={logTput} setLogY={setLogTput} />
        <div style={{color:P.muted, fontSize:"0.79em", marginBottom:10, lineHeight:1.6}}>
          The headline result: how the bottleneck splits between {algo.name} and the default
          Cubic flow. The gap between the two lines is exactly what Jain&apos;s index above
          quantifies.
        </div>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={rows} margin={{top:4,right:24,bottom:20,left:10}}>
            {grid}{xAx}
            <YAxis {...yAxisProps(logTput, 0.1, "Mbps", {tick:{fontSize:10}, label:{...yAxisProps(logTput,0.1,"Mbps").label, fontSize:10, dx:-6}})} />
            <Tooltip contentStyle={tipStyle} formatter={(v,n)=>[`${v} Mbps`, n]} />
            <ReferenceLine y={bw} stroke={P.accent} strokeDasharray="4 3"
              label={{value:"link rate", fill:P.accent, fontSize:10, position:"insideTopRight"}} />
            <Legend wrapperStyle={{fontSize:"0.78em", paddingTop:4}} />
            <Line type="monotone" dataKey="tput_comp"  name={algo.name}
              dot={false} strokeWidth={2} stroke={algo.color} />
            <Line type="monotone" dataKey="tput_cubic" name="Cubic (default)"
              dot={false} strokeWidth={2} stroke={P.red} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Hybla growth equations */}
      <div style={{color:P.muted, fontSize:"0.82em", lineHeight:1.6, margin:"4px 0 6px"}}>
        Hybla erases TCP&apos;s bias against high-RTT flows by scaling its growth with
        ρ = RTT / RTT₀ (RTT₀ ≈ 25 ms wired reference). Per round trip:
      </div>
      <Formula>SS: cwnd ×= 2<sup>ρ</sup>&nbsp;&nbsp;&nbsp;CA: cwnd += ρ²&nbsp;&nbsp;&nbsp;ρ = RTT / RTT₀</Formula>

      {/* Measured results from the paper */}
      <div style={{margin:"18px 0 6px", color:P.text, fontWeight:700, fontSize:"0.95em"}}>
        Measured results — Zhao, Peters, Chung &amp; Claypool (2022), IEEE CCNC
      </div>
      <div style={{color:P.muted, fontSize:"0.8em", marginBottom:10, lineHeight:1.55}}>
        Ground-truth measurements over a commercial Viasat-2 link (~140 Mb/s, RTT ≈ 600 ms).
        Jain&apos;s index per phase, plus the overall throughput difference vs Cubic
        (+ = Cubic gets more, − = the competitor gets more). The selected row is highlighted.
      </div>
      <div style={{overflowX:"auto", margin:"0 0 24px"}}>
        <table style={{width:"100%", borderCollapse:"collapse", fontSize:"0.83em"}}>
          <thead>
            <tr style={{borderBottom:`2px solid ${P.border}`}}>
              {["Algorithm","Jain start-up","Jain steady","Jain overall","Δ throughput (Mb/s)","Competing with Cubic"].map(h=>(
                <th key={h} style={{padding:"8px 10px", textAlign:"left", color:P.muted,
                  fontWeight:600, letterSpacing:"0.03em"}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Object.entries(ALGOS).map(([id,a])=>{
              const sel = id===competitor;
              return (
                <tr key={id} style={{borderBottom:`1px solid ${P.border}`,
                  background: sel ? a.color+"18" : "transparent"}}>
                  <td style={{padding:"7px 10px", color:a.color, fontWeight:700}}>{a.name}</td>
                  {a.fair.map((f,i)=>(
                    <td key={i} style={{padding:"7px 10px", color:fairColor(f),
                      fontFamily:"monospace"}}>{f.toFixed(2)}</td>
                  ))}
                  <td style={{padding:"7px 10px", fontFamily:"monospace",
                    color: a.diff>=0 ? P.accent : a.color}}>
                    {a.diff>=0?"+":""}{a.diff.toFixed(1)}
                  </td>
                  <td style={{padding:"7px 10px", color:P.muted, fontSize:"0.92em"}}>{a.note}</td>
                </tr>
              );
            })}
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

// ─── Scenarios ────────────────────────────────────────────────────────────────
const SCENARIOS = [
  { id:"local",             label:"Local DC (Jumbo)",       bwMbps:10000, rtt:0.12,  pktLoss:0,     mtu:9000 },
  { id:"cloud_same_az",     label:"Cloud Same AZ/Zone",     bwMbps:10000, rtt:0.5,   pktLoss:0,     mtu:9000 },
  { id:"cloud_cross_az",    label:"Cloud Cross-AZ",         bwMbps:5000,  rtt:2.5,   pktLoss:0,     mtu:9000 },
  { id:"cloud_cross_region",label:"Cloud Cross-Region",     bwMbps:1000,  rtt:45,    pktLoss:0.005, mtu:9000 },
  { id:"cloud_internet",    label:"Cloud → Internet",       bwMbps:500,   rtt:30,    pktLoss:0.01,  mtu:1500 },
  { id:"same_az",           label:"On-Prem Same DC",        bwMbps:1000,  rtt:2,     pktLoss:0,     mtu:1500 },
  { id:"cross_az",          label:"On-Prem Cross-DC",       bwMbps:1000,  rtt:12,    pktLoss:0,     mtu:1500 },
  { id:"cross_region",      label:"Cross-Region WAN",       bwMbps:500,   rtt:65,    pktLoss:0.01,  mtu:1500 },
  { id:"multi_region",      label:"Multi-Region (Global)",  bwMbps:200,   rtt:155,   pktLoss:0.02,  mtu:1500 },
  { id:"satellite",         label:"Satellite",              bwMbps:50,    rtt:620,   pktLoss:0.1,   mtu:1500 },
  { id:"custom",            label:"Custom",                 bwMbps:1000,  rtt:10,    pktLoss:0,     mtu:1500 },
];

// ─── Helpers ──────────────────────────────────────────────────────────────────
const fmtWin  = v => v >= 1024 ? `${(v/1024).toFixed(0)}MB` : `${v}KB`;
const fmtMbps = v => v >= 1000 ? `${(v/1000).toFixed(v>=10000?0:1)} Gbps` : `${v.toFixed(0)} Mbps`;
const fmtBytes = v => {
  if (v >= 1073741824) return `${(v/1073741824).toFixed(1)} GB`;
  if (v >= 1048576) return `${(v/1048576).toFixed(1)} MB`;
  if (v >= 1024) return `${(v/1024).toFixed(0)} KB`;
  return `${v} B`;
};
const nextPow2 = n => Math.pow(2, Math.ceil(Math.log2(n)));

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
  const [logCwnd,  setLogCwnd]  = useState(false);  // cwnd sawtooth
  const [logMath,  setLogMath]  = useState(true);   // Mathis (already log)

  // Interactive tuning calculator state
  const [tuningScenario, setTuningScenario] = useState("cloud_cross_az");
  const [tuningBw, setTuningBw] = useState(5000);
  const [tuningRtt, setTuningRtt] = useState(2.5);
  const [tuningMtu, setTuningMtu] = useState(9000);
  const [tuningLoss, setTuningLoss] = useState(0);

  // Apply scenario preset
  const applyTuningScenario = (id) => {
    setTuningScenario(id);
    const s = SCENARIOS.find(x => x.id === id);
    if (s) {
      setTuningBw(s.bwMbps);
      setTuningRtt(s.rtt);
      setTuningMtu(s.mtu);
      setTuningLoss(s.pktLoss);
    }
  };

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

      {/* Scenario BDP / window utilisation breakdown table */}
      <div style={{overflowX:"auto", margin:"16px 0 24px"}}>
        <table style={{width:"100%", borderCollapse:"collapse", fontSize:"0.84em"}}>
          <thead>
            <tr style={{borderBottom:`2px solid ${P.border}`}}>
              {["Scenario","Bandwidth","RTT","BDP","64KB window util","1MB window util","Rec. window"].map(h => (
                <th key={h} style={{padding:"8px 12px", textAlign:"left", color:P.muted,
                  fontWeight:600, letterSpacing:"0.04em", fontSize:"0.85em", whiteSpace:"nowrap"}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[
              {label:"Local DC",     bwMbps:10000, rttMs:0.2},
              {label:"Same-AZ",      bwMbps:1000,  rttMs:2},
              {label:"Cross-AZ",     bwMbps:1000,  rttMs:12},
              {label:"Cross-Region", bwMbps:500,   rttMs:60},
              {label:"Multi-Region", bwMbps:200,   rttMs:150},
              {label:"Satellite",    bwMbps:50,    rttMs:600},
            ].map(({label, bwMbps, rttMs}, i) => {
              const bdpBytes  = (bwMbps * 1e6 / 8) * (rttMs / 1000);
              const util64k   = Math.min(100, (65536  / bdpBytes * 100));
              const util1m    = Math.min(100, (1048576 / bdpBytes * 100));
              const recWin    = bdpBytes >= 1073741824
                ? `${(bdpBytes/1073741824).toFixed(1)} GB`
                : bdpBytes >= 1048576
                  ? `${(bdpBytes/1048576).toFixed(1)} MB`
                  : `${(bdpBytes/1024).toFixed(0)} KB`;
              const u64color  = util64k  < 5  ? P.red : util64k  < 30 ? P.yellow : P.green;
              const u1mcolor  = util1m   < 10 ? P.red : util1m   < 60 ? P.yellow : P.green;
              return (
                <tr key={label} style={{borderBottom:`1px solid ${P.border}`,
                  background: i%2===0 ? "transparent" : "#161b22"}}>
                  <td style={{padding:"9px 12px", color:P.text, fontWeight:600}}>{label}</td>
                  <td style={{padding:"9px 12px", color:P.cyan,   fontFamily:"monospace"}}>
                    {bwMbps >= 1000 ? `${bwMbps/1000} Gbps` : `${bwMbps} Mbps`}
                  </td>
                  <td style={{padding:"9px 12px", color:P.green,  fontFamily:"monospace"}}>{rttMs} ms</td>
                  <td style={{padding:"9px 12px", color:P.yellow, fontFamily:"monospace"}}>{recWin}</td>
                  <td style={{padding:"9px 12px", color:u64color, fontFamily:"monospace"}}>{util64k.toFixed(1)}%</td>
                  <td style={{padding:"9px 12px", color:u1mcolor, fontFamily:"monospace"}}>{util1m.toFixed(1)}%</td>
                  <td style={{padding:"9px 12px", color:P.purple, fontFamily:"monospace"}}>{recWin}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
        <div style={{color:P.muted, fontSize:"0.78em", marginTop:8, paddingLeft:4}}>
          Window utilisation = window size ÷ BDP × 100. Red = severely under-utilised. Recommended window = BDP (minimum to fill the pipe).
        </div>
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

      {/* ── 4.5. MTU and Maximum Segment Size ───────────────────────────── */}
      <SectionHeading num="4.5" title="MTU, MSS, and Path Fragmentation" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em", maxWidth:760}}>
        The <strong style={{color:P.text}}>Maximum Transmission Unit (MTU)</strong> is the largest IP packet size
        (in bytes) that can traverse a link without fragmentation. The TCP <strong style={{color:P.text}}>Maximum
        Segment Size (MSS)</strong> is derived as:
      </p>

      <Formula style={{margin:"20px 0", maxWidth:760}}>
        MSS = MTU − 40 bytes&nbsp;&nbsp;(20-byte IP header + 20-byte TCP header)
      </Formula>

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em", maxWidth:760}}>
        Standard Ethernet uses MTU 1500, yielding MSS 1460. Cloud providers support
        <strong style={{color:P.accent}}> jumbo frames</strong> (MTU 9000) on intra-VPC paths, reducing header
        overhead from 2.7% to 0.4% and decreasing interrupt rate by 6×.
      </p>

      {/* MTU Impact Chart */}
      <div style={{marginTop:20, marginBottom:20, maxWidth:780}}>
        <div style={{background:P.panel,border:`1px solid ${P.border}`,borderRadius:10,padding:16}}>
          <Label c={P.muted} style={{display:"block", marginBottom:12}}>
            MTU Impact on Packet Efficiency
          </Label>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={(() => {
              const mtus = [576, 1280, 1500, 4352, 9000];
              return mtus.map(mtu => {
                const mss = mtu - 40;
                const headerOverhead = parseFloat((40 / mtu * 100).toFixed(1));
                const packetsPerMB = Math.ceil(1048576 / mss);
                return {
                  mtu: `MTU ${mtu}`,
                  mss,
                  headerOverhead,
                  packetsPerMB
                };
              });
            })()}>
              <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
              <XAxis dataKey="mtu" stroke={P.muted} tick={{fontSize:10}}
                label={{value:"MTU (bytes)", position:"insideBottom", dy:10, fill:P.muted, fontSize:11}} />
              <YAxis yAxisId="left" stroke={P.red} tick={{fontSize:10}}
                label={{value:"Header Overhead %", angle:-90, position:"insideLeft", dx:-8, fill:P.red, fontSize:11}} />
              <YAxis yAxisId="right" orientation="right" stroke={P.green} tick={{fontSize:10}}
                label={{value:"Packets per MB", angle:90, position:"insideRight", dx:8, fill:P.green, fontSize:11}} />
              <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:6, fontSize:"0.85em"}}
                separator=""
                formatter={(value, name, props) => {
                  if (name === "headerOverhead") {
                    return [
                      <div key="overhead">
                        <div style={{color:P.red, fontWeight:600}}>{value}% header overhead</div>
                        <div style={{fontSize:"0.9em", marginTop:4, opacity:0.8}}>
                          MSS {props.payload.mss} bytes
                        </div>
                      </div>,
                      ""
                    ];
                  }
                  if (name === "packetsPerMB") {
                    return [
                      <div key="packets">
                        <div style={{color:P.green, fontWeight:600}}>{value.toLocaleString()} packets/MB</div>
                        <div style={{fontSize:"0.9em", marginTop:4, opacity:0.8}}>
                          MSS {props.payload.mss} bytes
                        </div>
                      </div>,
                      ""
                    ];
                  }
                  return [value, name];
                }} />
              <Bar yAxisId="left" dataKey="headerOverhead" fill={P.red} name="headerOverhead" />
              <Bar yAxisId="right" dataKey="packetsPerMB" fill={P.green} name="packetsPerMB" />
            </BarChart>
          </ResponsiveContainer>

          <div style={{marginTop:16, padding:12, background:P.panel2, borderRadius:6, fontSize:"0.85em"}}>
            <strong style={{color:P.accent}}>Cloud Provider MTU Limits:</strong>
            <ul style={{margin:"8px 0 0 20px", lineHeight:1.7, color:P.muted}}>
              <li><strong>AWS:</strong> MTU 9001 (VPC), 1500 (internet gateway egress)</li>
              <li><strong>GCP:</strong> MTU 8896 (VPC), 1460 (external IP egress)</li>
              <li><strong>Azure:</strong> MTU 9000 (VNet), 1400 (internet egress, VXLAN overhead)</li>
            </ul>
            <p style={{margin:"12px 0 0 0", color:P.yellow}}>
              ⚠ Jumbo frames work <em>only</em> on intra-VPC paths. Internet-bound traffic must use standard MTU.
            </p>
          </div>
        </div>

        {/* PMTUD Section */}
        <div style={{marginTop:16, background:P.panel, border:`1px solid ${P.border}`, borderRadius:10, padding:16}}>
          <Label c={P.muted}>Path MTU Discovery (PMTUD)</Label>
          <p style={{color:P.muted, fontSize:"0.9em", marginTop:8, lineHeight:1.7}}>
            TCP relies on ICMP "Fragmentation Needed" (Type 3, Code 4) messages to discover
            the path MTU. If intermediate firewalls block ICMP, the sender never learns to
            reduce MSS — creating a <strong style={{color:P.red}}>PMTUD black hole</strong>: handshakes succeed
            (small packets pass) but bulk transfers stall (large segments silently drop).
          </p>
          <div style={{marginTop:12, padding:10, background:P.bg, borderLeft:`3px solid ${P.cyan}`, fontFamily:"monospace", fontSize:"0.8em", lineHeight:1.6}}>
            # Diagnose path MTU to broker<br/>
            tracepath broker.example.com<br/><br/>
            # Force MSS clamp on tunnel interface (Linux)<br/>
            iptables -t mangle -A FORWARD -p tcp --tcp-flags SYN,RST SYN \<br/>
            &nbsp;&nbsp;-j TCPMSS --clamp-mss-to-pmtu
          </div>
        </div>

        {/* Fragmentation Loss Amplification */}
        <div style={{marginTop:16, background:P.panel, border:`1px solid ${P.border}`, borderRadius:10, padding:16}}>
          <Label c={P.muted}>Fragmentation Loss Amplification</Label>
          <p style={{color:P.muted, fontSize:"0.9em", marginTop:8, lineHeight:1.7}}>
            When a packet exceeds the path MTU and fragments into <em>N</em> fragments,
            losing <strong style={{color:P.red}}>any single fragment</strong> forces retransmission
            of the <em>entire original packet</em>. This creates a multiplicative loss effect:
          </p>

          <Formula style={{margin:"16px 0", fontSize:"0.9em"}}>
            P<sub>effective</sub> = 1 − (1 − p)<sup>N</sup>
            &nbsp;&nbsp;&nbsp;&nbsp;where N = ⌈packet_size / path_MTU⌉
          </Formula>

          <p style={{color:P.muted, fontSize:"0.9em", lineHeight:1.7, marginBottom:12}}>
            Example: A 9000-byte packet fragments into 7 pieces at MTU 1500. With 1% per-fragment loss,
            effective packet loss becomes <strong style={{color:P.red}}>6.8%</strong> — a 6.8× amplification.
            Combined with the Mathis equation, this can reduce throughput by 2.6×.
          </p>

          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={(() => {
              const losses = [0.001, 0.005, 0.01, 0.02, 0.05];
              const fragmentCounts = [1, 2, 3, 5, 7, 10];
              return losses.map(p => {
                const row = { loss: (p * 100).toFixed(2) + '%' };
                fragmentCounts.forEach(n => {
                  const effLoss = 1 - Math.pow(1 - p, n);
                  row[`n${n}`] = parseFloat((effLoss * 100).toFixed(2));
                });
                return row;
              });
            })()} margin={{top:4,right:20,bottom:22,left:50}}>
              <CartesianGrid strokeDasharray="3 3" stroke={P.border} />
              <XAxis dataKey="loss" stroke={P.muted} tick={{fontSize:10}}
                label={{value:"Per-Fragment Loss Rate", position:"insideBottom", dy:10, fill:P.muted, fontSize:11}} />
              <YAxis stroke={P.muted} tick={{fontSize:10}}
                label={{value:"Effective Packet Loss %", angle:-90, position:"insideLeft", dx:-10, fill:P.muted, fontSize:11}} />
              <Tooltip contentStyle={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:6, fontSize:"0.85em"}}
                formatter={(value) => `${value}%`} />
              <Legend wrapperStyle={{fontSize:"0.8em"}} />
              <Line type="monotone" dataKey="n1" stroke={P.green} name="No fragmentation" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="n2" stroke={P.cyan} name="2 fragments" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="n3" stroke={P.blue} name="3 fragments" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="n7" stroke={P.yellow} name="7 fragments (9KB@1500)" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="n10" stroke={P.red} name="10 fragments" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>

          <div style={{marginTop:12, padding:10, background:P.bg, borderLeft:`3px solid ${P.red}`, fontSize:"0.85em", lineHeight:1.6}}>
            <strong style={{color:P.red}}>Why fragmentation + loss is catastrophic:</strong>
            <ul style={{margin:"8px 0 0 20px", color:P.muted}}>
              <li>With 7 fragments and 1% loss: effective loss 6.8% → Mathis throughput drops 2.6×</li>
              <li>Reassembly timeout adds latency variance (typical timeout: 30-60 seconds)</li>
              <li>Out-of-order fragments trigger TCP retransmit timers unnecessarily</li>
              <li>Firewall stateful tracking often drops fragmented packets entirely</li>
            </ul>
            <p style={{margin:"12px 0 0 0", color:P.accent}}>
              <strong>Solution:</strong> Use PMTUD or MSS clamping to prevent fragmentation.
              In Kafka: verify path MTU with <code style={{background:P.panel, padding:"2px 6px", borderRadius:3}}>tracepath</code> before
              enabling jumbo frames.
            </p>
          </div>
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
              ["Local DC (Jumbo)", "10 Gbps", "0.12 ms", "150 KB", "43%", "Jumbo frames (MTU 9000) + BBR"],
              ["Cloud Same AZ/Zone", "10 Gbps", "0.5 ms", "625 KB", "10%", "Jumbo frames + large buffers"],
              ["Cloud Cross-AZ", "5 Gbps", "2.5 ms", "1.56 MB", "4%", "Jumbo frames + BBR + buffers"],
              ["Cloud Cross-Region", "1 Gbps", "45 ms", "5.63 MB", "1.1%", "Large buffers + BBR + Window Scale"],
              ["Cloud → Internet", "500 Mbps", "30 ms", "1.88 MB", "3.4%", "Standard MTU + buffer tuning"],
              ["On-Prem Same DC", "1 Gbps", "2 ms", "250 KB", "26%", "Tune buffers"],
              ["On-Prem Cross-DC", "1 Gbps", "12 ms", "1.5 MB", "4.3%", "Large buffers + CUBIC/BBR"],
              ["Cross-Region WAN", "500 Mbps", "65 ms", "4.06 MB", "1.6%", "Large buffers + Window Scale"],
              ["Multi-Region (Global)", "200 Mbps", "155 ms", "3.88 MB", "1.7%", "Large buffers + BBR + compression"],
              ["Satellite", "50 Mbps", "620 ms", "3.88 MB", "1.7%", "Performance Enhancing Proxy + buffers"],
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

      {/* ── 7. Interactive Linux Tuning Calculator ─────────────────────────── */}
      <SectionHeading num="7" title="Interactive Linux Tuning Calculator" />

      {(() => {
        // Calculate tuning parameters
        const bdpBytes = (tuningBw * 1e6 / 8) * (tuningRtt / 1000);
        const bufCeil = nextPow2(bdpBytes * 2); // 2× BDP for headroom
        const mss = tuningMtu - 40;
        const mathisMbps = tuningLoss > 0
          ? (mss / ((tuningRtt / 1000) * Math.sqrt(tuningLoss / 100))) / 125000
          : null;
        const effectiveBw = mathisMbps ? Math.min(tuningBw, mathisMbps) : tuningBw;
        const windowSizeKB = bdpBytes / 1024;

        return (
          <div style={{display:"grid", gap:16}}>
            {/* Scenario selector */}
            <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:8, padding:16}}>
              <div style={{color:P.muted, fontSize:"0.78em", fontWeight:600, letterSpacing:"0.08em",
                textTransform:"uppercase", marginBottom:10}}>Scenario Preset</div>
              <select
                value={tuningScenario}
                onChange={(e) => applyTuningScenario(e.target.value)}
                style={{
                  width:"100%", background:P.bg, border:`1px solid ${P.border}`,
                  borderRadius:6, padding:"10px 12px", color:P.text, fontSize:"0.9em",
                  fontWeight:600, cursor:"pointer"
                }}>
                {SCENARIOS.map(s => (
                  <option key={s.id} value={s.id}>{s.label}</option>
                ))}
              </select>
            </div>

            {/* Parameter sliders */}
            <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:8, padding:16,
              display:"grid", gap:12}}>
              <SliderField label="Bandwidth" val={tuningBw} set={setTuningBw}
                min={10} max={50000} step={tuningBw >= 10000 ? 100 : tuningBw >= 1000 ? 10 : 1}
                fmt={v => fmtMbps(v)} color={P.accent} />
              <SliderField label="RTT (Round-Trip Time)" val={tuningRtt} set={setTuningRtt}
                min={0.1} max={1000} step={tuningRtt >= 100 ? 10 : tuningRtt >= 10 ? 1 : 0.1}
                fmt={v => `${v.toFixed(v < 1 ? 2 : v < 10 ? 1 : 0)} ms`} color={P.green} />
              <SliderField label="MTU (Maximum Transmission Unit)" val={tuningMtu} set={setTuningMtu}
                min={576} max={9000} step={100}
                fmt={v => `${v} bytes (MSS: ${v-40})`} color={P.yellow} />
              <SliderField label="Packet Loss" val={tuningLoss} set={setTuningLoss}
                min={0} max={5} step={0.01}
                fmt={v => `${v.toFixed(2)}%`} color={P.red} />
            </div>

            {/* Calculated metrics */}
            <div style={{background:P.panel, border:`1px solid ${P.border}`, borderRadius:8, padding:16}}>
              <div style={{color:P.muted, fontSize:"0.78em", fontWeight:600, letterSpacing:"0.08em",
                textTransform:"uppercase", marginBottom:12}}>Calculated Parameters</div>
              <div style={{display:"grid", gridTemplateColumns:"repeat(auto-fit, minmax(140px, 1fr))", gap:12}}>
                <div style={{background:P.bg, borderRadius:6, padding:"10px 12px"}}>
                  <div style={{color:P.muted, fontSize:"0.7em", marginBottom:4}}>BDP</div>
                  <div style={{color:P.accent, fontFamily:"monospace", fontWeight:700, fontSize:"1.1em"}}>
                    {fmtBytes(bdpBytes)}
                  </div>
                </div>
                <div style={{background:P.bg, borderRadius:6, padding:"10px 12px"}}>
                  <div style={{color:P.muted, fontSize:"0.7em", marginBottom:4}}>Buffer Ceiling</div>
                  <div style={{color:P.green, fontFamily:"monospace", fontWeight:700, fontSize:"1.1em"}}>
                    {fmtBytes(bufCeil)}
                  </div>
                </div>
                <div style={{background:P.bg, borderRadius:6, padding:"10px 12px"}}>
                  <div style={{color:P.muted, fontSize:"0.7em", marginBottom:4}}>MSS</div>
                  <div style={{color:P.yellow, fontFamily:"monospace", fontWeight:700, fontSize:"1.1em"}}>
                    {mss} bytes
                  </div>
                </div>
                {mathisMbps && (
                  <div style={{background:P.bg, borderRadius:6, padding:"10px 12px"}}>
                    <div style={{color:P.muted, fontSize:"0.7em", marginBottom:4}}>Loss Limit (Mathis)</div>
                    <div style={{color:P.red, fontFamily:"monospace", fontWeight:700, fontSize:"1.1em"}}>
                      {fmtMbps(mathisMbps)}
                    </div>
                  </div>
                )}
                <div style={{background:P.bg, borderRadius:6, padding:"10px 12px"}}>
                  <div style={{color:P.muted, fontSize:"0.7em", marginBottom:4}}>Window Size (64KB util)</div>
                  <div style={{color:P.purple, fontFamily:"monospace", fontWeight:700, fontSize:"1.1em"}}>
                    {Math.round(65536 / bdpBytes * 100)}%
                  </div>
                </div>
              </div>
            </div>

            {/* Generated sysctl config */}
            <div style={{background:"#0d1117", border:`1px solid #30363d`, borderRadius:8,
              padding:"16px 20px", fontFamily:"'JetBrains Mono',monospace",
              fontSize:"0.8em", lineHeight:1.8, overflowX:"auto"}}>
              <div style={{color:P.muted}}># BDP = {fmtMbps(tuningBw)} × {tuningRtt} ms / 8 = {fmtBytes(bdpBytes)}</div>
              <div style={{color:P.muted}}># Buffer ceiling (2× BDP, rounded to power of 2): {fmtBytes(bufCeil)}</div>
              <br/>
              <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.core.rmem_max={bufCeil}</span></div>
              <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.core.wmem_max={bufCeil}</span></div>
              <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.ipv4.tcp_rmem=<span style={{color:P.yellow}}>"4096 {Math.round(bufCeil/2)} {bufCeil}"</span></span></div>
              <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.ipv4.tcp_wmem=<span style={{color:P.yellow}}>"4096 {Math.round(bufCeil/2)} {bufCeil}"</span></span></div>
              <br/>
              <div style={{color:P.muted}}># Verify window scaling in use:</div>
              <div><span style={{color:P.green}}>ss</span>{" "}<span style={{color:P.text}}>-timn | grep wscale</span></div>
              <br/>
              <div style={{color:P.muted}}># Congestion control ({bdpBytes > 1e6 ? "BBR recommended for high-BDP" : "CUBIC adequate for low-BDP"})</div>
              <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.ipv4.tcp_congestion_control=<span style={{color:P.yellow}}>{bdpBytes > 1e6 ? "bbr" : "cubic"}</span></span></div>
              {bdpBytes > 1e6 && (
                <div><span style={{color:P.green}}>sysctl</span>{" "}<span style={{color:P.text}}>-w net.core.default_qdisc=<span style={{color:P.yellow}}>fq</span></span></div>
              )}
              {tuningMtu === 9000 && (
                <>
                  <br/>
                  <div style={{color:P.muted}}># Jumbo frames (MTU 9000) - verify path supports it:</div>
                  <div><span style={{color:P.green}}>ip</span>{" "}<span style={{color:P.text}}>link set dev eth0 mtu 9000</span></div>
                  <div><span style={{color:P.green}}>tracepath</span>{" "}<span style={{color:P.text}}>target.example.com</span></div>
                </>
              )}
            </div>

            {/* Diagnostics */}
            {mathisMbps && mathisMbps < tuningBw && (
              <div style={{background:"#2d1a1a", border:`1px solid ${P.red}44`, borderRadius:8,
                padding:12, fontSize:"0.85em", color:P.red}}>
                ⚠ <strong>Packet loss ({tuningLoss}%) limits throughput to {fmtMbps(mathisMbps)}</strong> despite {fmtMbps(tuningBw)} link.
                Fix network issues before tuning buffers.
              </div>
            )}
            {bdpBytes < 65536 && (
              <div style={{background:"#1a2d1a", border:`1px solid ${P.green}44`, borderRadius:8,
                padding:12, fontSize:"0.85em", color:P.green}}>
                ✓ Default buffers (64 KB) sufficient for this path ({Math.round(65536/bdpBytes)}× BDP).
                No tuning needed.
              </div>
            )}
            {bdpBytes >= 65536 && bdpBytes < 1e6 && (
              <div style={{background:"#2d2a1a", border:`1px solid ${P.yellow}44`, borderRadius:8,
                padding:12, fontSize:"0.85em", color:P.yellow}}>
                ⚠ BDP ({fmtBytes(bdpBytes)}) exceeds default 64 KB buffers. Buffer tuning recommended.
              </div>
            )}
            {bdpBytes >= 1e6 && (
              <div style={{background:"#2d2a1a", border:`1px solid ${P.yellow}44`, borderRadius:8,
                padding:12, fontSize:"0.85em", color:P.yellow}}>
                ⚠ High-BDP path ({fmtBytes(bdpBytes)}). Large buffers + BBR + Window Scale (RFC 1323) required.
                Default Linux settings will severely limit throughput.
              </div>
            )}
          </div>
        );
      })()}

      {/* ── 8. Competing Congestion Control ──────────────────────────────── */}
      <SectionHeading num="8" title="Competing Congestion Control Algorithms" />

      <p style={{color:P.muted, lineHeight:1.7, fontSize:"0.93em"}}>
        Real links carry many flows at once, and a new algorithm only matters by how it behaves
        when it shares a bottleneck with the deployed default —{" "}
        <strong style={{color:P.text}}>Cubic</strong>. Following Zhao, Peters, Chung &amp; Claypool
        (2022), this simulation pits one competitor against a default Cubic flow over a single
        shared bottleneck and measures the split with Jain&apos;s fairness index. Four approaches:
        loss-based <strong style={{color:P.text}}>Cubic</strong>, bandwidth-estimation{" "}
        <strong style={{color:P.text}}>BBR</strong> (Cardwell et al., 2016), utility-function{" "}
        <strong style={{color:P.text}}>PCC</strong> (Dong et al., 2018), and satellite-optimised{" "}
        <strong style={{color:P.text}}>Hybla</strong> (Caini &amp; Firrincieli, 2004). Pick a
        competitor and adjust the path — the default values reproduce the paper&apos;s Viasat-2
        satellite testbed.
      </p>

      <CongestionCompetition />

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
          {tag:"Hybla 2004",   year:2004, title:"TCP Hybla: a TCP Enhancement for Heterogeneous Networks — Caini & Firrincieli; Int. J. Satellite Comms & Networking 22(5)",
           url:"https://doi.org/10.1002/sat.799"},
          {tag:"PCC 2015",     year:2015, title:"PCC: Re-architecting Congestion Control for Consistent High Performance — Dong, Li, Zarchy, Godfrey, Schapira; USENIX NSDI",
           url:"https://www.usenix.org/conference/nsdi15/technical-sessions/presentation/dong"},
          {tag:"PCC Vivace 2018",year:2018, title:"PCC Vivace: Online-Learning Congestion Control — Dong, Meng, Zarchy, Arslan, Gilad, Godfrey, Schapira; USENIX NSDI",
           url:"https://www.usenix.org/conference/nsdi18/presentation/dong"},
          {tag:"Zhao 2022",    year:2022, title:"Competing TCP Congestion Control Algorithms over a Satellite Network — Zhao, Peters, Chung, Claypool; IEEE CCNC",
           url:"https://web.cs.wpi.edu/~claypool/papers/tcp-compete-22/"},
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
