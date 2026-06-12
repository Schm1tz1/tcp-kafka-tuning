import { useState, useEffect, useCallback } from "react";
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell, ReferenceLine,
} from "recharts";

// ════════════════════════════════════════════════════════════════════════════
// Interactive slideshow — a guided overview of how buffers, TCP windows, RTT,
// and BDP set the ceiling on Kafka throughput. Concepts and orders of magnitude
// only; the technical detail lives in the two explainer dashboards.
// ════════════════════════════════════════════════════════════════════════════

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

// ─── Helpers ────────────────────────────────────────────────────────────────
const bdpBytes = (bwMbps, rttMs) => (bwMbps * 1e6 / 8) * (rttMs / 1000);
const tputMbps = (winBytes, rttMs) => (winBytes * 8) / (rttMs / 1000) / 1e6;

const fmtBytes = v => {
  if (v >= 1073741824) return `${(v / 1073741824).toFixed(1)} GB`;
  if (v >= 1048576)    return `${(v / 1048576).toFixed(v >= 10485760 ? 0 : 1)} MB`;
  if (v >= 1024)       return `${(v / 1024).toFixed(0)} KB`;
  return `${Math.round(v)} B`;
};
const fmtMbps = v =>
  v >= 1000 ? `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)} Gbps`
            : v >= 1 ? `${v.toFixed(v >= 10 ? 0 : 1)} Mbps`
            : `${(v * 1000).toFixed(0)} kbps`;

// ─── Primitives ─────────────────────────────────────────────────────────────
const Mono = ({ children, c = P.cyan }) => (
  <code style={{ fontFamily:"'JetBrains Mono',monospace", background:"#1c2333",
    padding:"1px 6px", borderRadius:4, color:c, fontSize:"0.86em" }}>{children}</code>
);

const Formula = ({ children }) => (
  <div style={{ background:"#1a2035", border:"1px solid #2d4263", borderRadius:8,
    padding:"12px 20px", margin:"14px 0", fontFamily:"'JetBrains Mono',monospace",
    color:"#79c0ff", fontSize:"1.05em", letterSpacing:"0.02em", textAlign:"center" }}>
    {children}
  </div>
);

const Pill = ({ children, color = P.accent }) => (
  <span style={{ background:color + "1f", color, border:`1px solid ${color}55`,
    borderRadius:20, padding:"3px 12px", fontSize:"0.74em", fontWeight:700,
    letterSpacing:"0.05em", textTransform:"uppercase",
    fontFamily:"'JetBrains Mono',monospace" }}>{children}</span>
);

const BigStat = ({ value, label, color = P.accent, sub }) => (
  <div style={{ background:"#0d1117", border:`1px solid ${color}44`, borderRadius:12,
    padding:"18px 22px", textAlign:"center", minWidth:150, flex:"1 1 150px" }}>
    <div style={{ color, fontFamily:"'JetBrains Mono',monospace", fontWeight:800,
      fontSize:"1.9em", lineHeight:1.1 }}>{value}</div>
    <div style={{ color:P.muted, fontSize:"0.78em", marginTop:6, letterSpacing:"0.04em",
      textTransform:"uppercase" }}>{label}</div>
    {sub && <div style={{ color:P.muted, fontSize:"0.8em", marginTop:6, opacity:0.8 }}>{sub}</div>}
  </div>
);

const Lead = ({ children }) => (
  <p style={{ color:P.text, fontSize:"1.08em", lineHeight:1.7, margin:"0 0 18px",
    maxWidth:780 }}>{children}</p>
);

const Note = ({ children, color = P.yellow }) => (
  <div style={{ background:color + "12", borderLeft:`3px solid ${color}`, borderRadius:6,
    padding:"12px 16px", margin:"16px 0", color:P.text, fontSize:"0.95em",
    lineHeight:1.6, maxWidth:780 }}>{children}</div>
);

function Slider({ label, value, set, min, max, step = 1, fmt, color = P.accent }) {
  return (
    <div style={{ flex:"1 1 220px" }}>
      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"baseline",
        marginBottom:6 }}>
        <span style={{ color:P.muted, fontSize:"0.82em", fontWeight:600 }}>{label}</span>
        <span style={{ color, fontFamily:"'JetBrains Mono',monospace", fontWeight:700,
          fontSize:"0.95em" }}>{fmt(value)}</span>
      </div>
      <input type="range" min={min} max={max} step={step} value={value}
        onChange={e => set(Number(e.target.value))}
        style={{ width:"100%", accentColor:color, cursor:"pointer" }} />
    </div>
  );
}

const tipStyle = { background:P.panel, border:`1px solid ${P.border}`,
  borderRadius:8, fontSize:"0.82em" };

// ─── Pipe diagram ───────────────────────────────────────────────────────────
// A horizontal "pipe" filled to fillFrac. Diameter ≈ bandwidth, length ≈ RTT,
// the shaded volume ≈ data in flight. Used to make T = W / RTT tangible.
function Pipe({ fillFrac = 1, fillColor = P.accent, diameterLabel, lengthLabel, caption }) {
  const W = 560, H = 190;
  const x0 = 70, x1 = W - 30, y0 = 50, y1 = 150;
  const innerW = x1 - x0, innerH = y1 - y0;
  const fillW = Math.max(0, Math.min(1, fillFrac)) * innerW;
  const r = innerH / 2;

  const pkts = [];
  const pw = 24, gap = 9;
  for (let x = x0 + 8; x + pw < x0 + fillW; x += pw + gap) pkts.push(x);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width:"100%", maxWidth:600 }}>
      <defs>
        <clipPath id="pipeclip">
          <rect x={x0} y={y0} width={innerW} height={innerH} rx={r} />
        </clipPath>
      </defs>

      {/* diameter arrow = bandwidth */}
      <line x1={40} y1={y0} x2={40} y2={y1} stroke={P.green} strokeWidth="1.5" />
      <polygon points={`36,${y0 + 6} 40,${y0} 44,${y0 + 6}`} fill={P.green} />
      <polygon points={`36,${y1 - 6} 40,${y1} 44,${y1 - 6}`} fill={P.green} />
      {diameterLabel && (
        <text x={20} y={(y0 + y1) / 2} fill={P.green} fontSize="11"
          fontFamily="monospace" textAnchor="middle"
          transform={`rotate(-90,20,${(y0 + y1) / 2})`}>{diameterLabel}</text>
      )}

      {/* length arrow = RTT */}
      <line x1={x0} y1={172} x2={x1} y2={172} stroke={P.yellow} strokeWidth="1.5" />
      <polygon points={`${x0 + 6},168 ${x0},172 ${x0 + 6},176`} fill={P.yellow} />
      <polygon points={`${x1 - 6},168 ${x1},172 ${x1 - 6},176`} fill={P.yellow} />
      {lengthLabel && (
        <text x={(x0 + x1) / 2} y={186} fill={P.yellow} fontSize="11"
          fontFamily="monospace" textAnchor="middle">{lengthLabel}</text>
      )}

      {/* pipe walls + fill */}
      <rect x={x0} y={y0} width={innerW} height={innerH} rx={r}
        fill="#0d1117" stroke={P.border} strokeWidth="2" />
      <rect x={x0} y={y0} width={fillW} height={innerH} fill={fillColor}
        opacity="0.16" clipPath="url(#pipeclip)" />
      {pkts.map((x, i) => (
        <rect key={i} x={x} y={(y0 + y1) / 2 - 11} width={pw} height={22} rx={3}
          fill={fillColor} opacity={Math.max(0.35, 0.9 - i * 0.045)} />
      ))}

      {caption && (
        <text x={(x0 + x1) / 2} y={y0 - 16} fill={P.muted} fontSize="11.5"
          fontFamily="monospace" textAnchor="middle">{caption}</text>
      )}
    </svg>
  );
}

// ════════════════════════════════════════════════════════════════════════════
// SLIDES
// ════════════════════════════════════════════════════════════════════════════

// 1 ── Title ──────────────────────────────────────────────────────────────────
function TitleSlide() {
  return (
    <div style={{ textAlign:"center", maxWidth:760, margin:"0 auto" }}>
      <div style={{ display:"flex", gap:10, justifyContent:"center", marginBottom:24,
        flexWrap:"wrap" }}>
        <Pill color={P.yellow}>RTT</Pill>
        <Pill color={P.green}>Bandwidth</Pill>
        <Pill color={P.accent}>BDP</Pill>
        <Pill color={P.purple}>TCP window</Pill>
        <Pill color={P.cyan}>Buffers</Pill>
      </div>
      <h1 style={{ fontSize:"2.6em", fontWeight:800, color:P.text, margin:"0 0 16px",
        letterSpacing:"-0.02em", lineHeight:1.1 }}>
        Filling the Pipe
      </h1>
      <p style={{ color:P.accent, fontSize:"1.25em", fontWeight:600, margin:"0 0 24px" }}>
        Why TCP tuning decides your Kafka throughput
      </p>
      <p style={{ color:P.muted, fontSize:"1.05em", lineHeight:1.7, maxWidth:620,
        margin:"0 auto" }}>
        A plain-language tour of the four numbers that cap how fast data moves —
        and how they ripple all the way up to producer batches and partition
        counts. Concepts and orders of magnitude here; the math lives in the
        explainer dashboards.
      </p>
      <div style={{ marginTop:40, color:P.muted, fontSize:"0.85em" }}>
        Use <Kbd>←</Kbd> <Kbd>→</Kbd> or the buttons below to move through the deck.
      </div>
    </div>
  );
}

const Kbd = ({ children }) => (
  <span style={{ display:"inline-block", border:`1px solid ${P.border}`,
    borderBottomWidth:2, borderRadius:5, padding:"1px 8px", margin:"0 2px",
    fontFamily:"'JetBrains Mono',monospace", fontSize:"0.85em", color:P.text,
    background:"#0d1117" }}>{children}</span>
);

// 2 ── Pipe model ────────────────────────────────────────────────────────────
function PipeModelSlide() {
  return (
    <div>
      <Lead>
        Think of a network connection as a <strong>pipe</strong>. Its
        <span style={{ color:P.green }}> width</span> is the bandwidth — how fast
        bits can flow. Its <span style={{ color:P.yellow }}> length</span> is the
        round-trip time — how long a packet takes to go there and back.
      </Lead>
      <div style={{ display:"flex", justifyContent:"center", margin:"10px 0 8px" }}>
        <Pipe fillFrac={1} fillColor={P.accent}
          diameterLabel="bandwidth →"
          lengthLabel="round-trip time (RTT) →"
          caption="data in flight = bandwidth × RTT" />
      </div>
      <Note color={P.accent}>
        To run the pipe at full speed you must keep it <strong>full</strong> —
        always have enough unacknowledged data travelling through it. The volume
        the full pipe holds is the <strong>Bandwidth-Delay Product (BDP)</strong>.
        Everything in this deck is about hitting that volume.
      </Note>
    </div>
  );
}

// 3 ── RTT ───────────────────────────────────────────────────────────────────
const RTT_ROWS = [
  { name:"Same rack",        rtt:0.1,  color:P.green },
  { name:"Same AZ / zone",   rtt:0.5,  color:P.green },
  { name:"Cross-AZ",         rtt:2.5,  color:P.cyan },
  { name:"Cross-region",     rtt:45,   color:P.yellow },
  { name:"Intercontinental", rtt:150,  color:P.red },
  { name:"Geostationary sat",rtt:620,  color:P.purple },
];
function RttSlide() {
  return (
    <div>
      <Lead>
        <strong>RTT</strong> — round-trip time — is the latency for a packet to
        reach the other side and an acknowledgement to come back. It is set by
        distance and physics, not by how much you pay for bandwidth.
      </Lead>
      <div style={{ background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"16px 14px" }}>
        <ResponsiveContainer width="100%" height={250}>
          <BarChart data={RTT_ROWS} layout="vertical"
            margin={{ top:6, right:60, bottom:6, left:20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" horizontal={false} />
            <XAxis type="number" scale="log" domain={[0.08, 800]}
              stroke={P.muted} tick={{ fontSize:11 }}
              ticks={[0.1, 1, 10, 100, 600]} tickFormatter={v => `${v} ms`} />
            <YAxis type="category" dataKey="name" stroke={P.muted}
              tick={{ fontSize:11 }} width={120} />
            <Tooltip contentStyle={tipStyle} formatter={v => [`${v} ms`, "RTT"]} />
            <Bar dataKey="rtt" radius={[0, 4, 4, 0]} label={{ position:"right",
              fill:P.text, fontSize:11, formatter:v => `${v} ms` }}>
              {RTT_ROWS.map((r, i) => <Cell key={i} fill={r.color} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <Note>
        Note the <strong>log scale</strong>: a satellite hop is roughly
        <Mono c={P.purple}>6000×</Mono> the latency of a same-rack connection.
        RTT appears in the denominator of every throughput formula, so longer
        pipes are dramatically harder to fill.
      </Note>
    </div>
  );
}

// 4 ── Bandwidth ─────────────────────────────────────────────────────────────
const BW_ROWS = [
  { name:"100 Mbps office",  bw:100,    color:P.muted },
  { name:"1 Gbps NIC",       bw:1000,   color:P.cyan },
  { name:"10 Gbps cloud VM", bw:10000,  color:P.green },
  { name:"25 Gbps NIC",      bw:25000,  color:P.accent },
  { name:"100 Gbps backbone",bw:100000, color:P.purple },
];
function BandwidthSlide() {
  return (
    <div>
      <Lead>
        <strong>Bandwidth</strong> is the pipe's capacity — the maximum bits per
        second the link can carry. It is a <em>ceiling</em>, not a promise: you
        only reach it if everything else lets you keep the pipe full.
      </Lead>
      <div style={{ background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"16px 14px" }}>
        <ResponsiveContainer width="100%" height={230}>
          <BarChart data={BW_ROWS} layout="vertical"
            margin={{ top:6, right:70, bottom:6, left:20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" horizontal={false} />
            <XAxis type="number" scale="log" domain={[80, 130000]}
              stroke={P.muted} tick={{ fontSize:11 }}
              ticks={[100, 1000, 10000, 100000]} tickFormatter={fmtMbps} />
            <YAxis type="category" dataKey="name" stroke={P.muted}
              tick={{ fontSize:11 }} width={130} />
            <Tooltip contentStyle={tipStyle} formatter={v => [fmtMbps(v), "link"]} />
            <Bar dataKey="bw" radius={[0, 4, 4, 0]} label={{ position:"right",
              fill:P.text, fontSize:11, formatter:fmtMbps }}>
              {BW_ROWS.map((r, i) => <Cell key={i} fill={r.color} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <Note color={P.green}>
        Buying a fatter pipe does nothing if your windows and buffers can't keep
        it full. That is the single most common cause of "we have a 10 Gbps link
        but only see 100 Mbps" — and the rest of this deck explains why.
      </Note>
    </div>
  );
}

// 5 ── BDP (interactive) ─────────────────────────────────────────────────────
function BdpSlide() {
  const [bw, setBw]   = useState(10000); // Mbps
  const [rtt, setRtt] = useState(45);    // ms
  const bytes = bdpBytes(bw, rtt);
  const pkts  = Math.round(bytes / 1460);
  const defWinPct = Math.min(100, 65536 / bytes * 100);

  return (
    <div>
      <Lead>
        The <strong>Bandwidth-Delay Product</strong> is how much data must be
        "in flight" to keep the pipe full. It is simply the pipe's volume:
      </Lead>
      <Formula>BDP = bandwidth × RTT</Formula>

      <div style={{ background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 20px", margin:"10px 0" }}>
        <div style={{ display:"flex", gap:24, flexWrap:"wrap", marginBottom:18 }}>
          <Slider label="Bandwidth" value={bw} set={setBw} min={100} max={100000}
            step={100} fmt={fmtMbps} color={P.green} />
          <Slider label="RTT" value={rtt} set={setRtt} min={1} max={620} step={1}
            fmt={v => `${v} ms`} color={P.yellow} />
        </div>
        <div style={{ display:"flex", gap:14, flexWrap:"wrap" }}>
          <BigStat value={fmtBytes(bytes)} label="data in flight (BDP)" color={P.accent} />
          <BigStat value={pkts.toLocaleString()} label="packets in flight" color={P.cyan}
            sub="at 1460 B each" />
          <BigStat value={`${defWinPct < 1 ? defWinPct.toFixed(2) : defWinPct.toFixed(0)}%`}
            label="filled by a 64 KB window" color={defWinPct < 25 ? P.red : P.yellow}
            sub="the classic untuned default" />
        </div>
      </div>
      <Note>
        Drag the sliders to a cross-region link (10 Gbps, 45 ms) and the BDP is
        tens of megabytes — yet an untuned connection only allows tens of
        kilobytes in flight. That gap is the throughput you're leaving on the table.
      </Note>
    </div>
  );
}

// 6 ── TCP window (interactive) ──────────────────────────────────────────────
function WindowSlide() {
  const [winKB, setWinKB] = useState(64);
  const [rtt, setRtt]     = useState(45);
  const winBytes = winKB * 1024;
  const t = tputMbps(winBytes, rtt);

  // T vs window for the chosen RTT
  const data = [];
  for (let kb = 16; kb <= 65536; kb *= 2) {
    data.push({ win: kb, mbps: +tputMbps(kb * 1024, rtt).toFixed(2) });
  }

  return (
    <div>
      <Lead>
        The <strong>TCP receive window</strong> is the sender's permission slip:
        the most unacknowledged data the receiver will accept at once. It caps
        what's in flight — so it caps throughput:
      </Lead>
      <Formula>throughput = window ÷ RTT</Formula>

      <div style={{ background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"18px 20px", margin:"10px 0" }}>
        <div style={{ display:"flex", gap:24, flexWrap:"wrap", marginBottom:14 }}>
          <Slider label="Receive window" value={winKB} set={setWinKB} min={16}
            max={65536} step={16} fmt={v => fmtBytes(v * 1024)} color={P.purple} />
          <Slider label="RTT" value={rtt} set={setRtt} min={1} max={620} step={1}
            fmt={v => `${v} ms`} color={P.yellow} />
        </div>
        <div style={{ display:"flex", gap:14, flexWrap:"wrap", marginBottom:8 }}>
          <BigStat value={fmtMbps(t)} label="achievable throughput" color={P.accent} />
        </div>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={data} margin={{ top:8, right:24, bottom:24, left:8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" />
            <XAxis dataKey="win" scale="log" domain={[16, 65536]} type="number"
              stroke={P.muted} tick={{ fontSize:10 }}
              ticks={[16, 256, 4096, 65536]} tickFormatter={v => fmtBytes(v * 1024)}
              label={{ value:"window", position:"insideBottom", dy:14, fill:P.muted, fontSize:11 }} />
            <YAxis scale="log" domain={[1, "auto"]} stroke={P.muted} tick={{ fontSize:10 }}
              tickFormatter={v => v >= 1000 ? `${v / 1000}G` : v} />
            <Tooltip contentStyle={tipStyle}
              formatter={v => [fmtMbps(v), "throughput"]}
              labelFormatter={v => fmtBytes(v * 1024)} />
            <ReferenceLine x={winKB} stroke={P.purple} strokeDasharray="4 3" />
            <Line type="monotone" dataKey="mbps" stroke={P.accent} strokeWidth={2.5}
              dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <Note color={P.purple}>
        Same window, longer RTT → less throughput. To go faster on a long pipe
        you must grow the window. Before RFC 1323 (1992) the window field maxed
        out at 64 KB; <strong>window scaling</strong> is what lets it reach the
        megabytes a modern BDP needs.
      </Note>
    </div>
  );
}

// 7 ── Buffers ───────────────────────────────────────────────────────────────
function BuffersSlide() {
  return (
    <div>
      <Lead>
        Where does the window's ceiling come from? The OS <strong>socket
        buffers</strong> — <Mono>net.ipv4.tcp_rmem</Mono> (receive) and
        <Mono>tcp_wmem</Mono> (send). The window can never exceed the buffer that
        backs it, because there'd be nowhere to put the data.
      </Lead>

      <div style={{ display:"flex", justifyContent:"center", margin:"8px 0" }}>
        <svg viewBox="0 0 600 150" style={{ width:"100%", maxWidth:620 }}>
          {[
            { x:20,  c:P.cyan,   t:"Socket buffer", s:"rmem / wmem", w:170 },
            { x:225, c:P.purple, t:"TCP window",     s:"≤ buffer",    w:150 },
            { x:410, c:P.accent, t:"In-flight (BDP)",s:"≤ window",    w:170 },
          ].map((b, i) => (
            <g key={i}>
              <rect x={b.x} y={35} width={b.w} height={70} rx={8}
                fill={b.c + "14"} stroke={b.c} strokeWidth="1.8" />
              <text x={b.x + b.w / 2} y={66} fill={b.c} fontSize="14"
                fontFamily="monospace" fontWeight="bold" textAnchor="middle">{b.t}</text>
              <text x={b.x + b.w / 2} y={88} fill={P.muted} fontSize="11.5"
                fontFamily="monospace" textAnchor="middle">{b.s}</text>
              {i < 2 && (
                <>
                  <line x1={b.x + b.w + 4} y1={70} x2={b.x + b.w + 27} y2={70}
                    stroke={P.muted} strokeWidth="1.5" />
                  <polygon points={`${b.x + b.w + 27},65 ${b.x + b.w + 35},70 ${b.x + b.w + 27},75`}
                    fill={P.muted} />
                </>
              )}
            </g>
          ))}
          <text x={300} y={130} fill={P.muted} fontSize="12" fontFamily="monospace"
            textAnchor="middle">each box is bounded by the one on its left</text>
        </svg>
      </div>

      <Note color={P.red}>
        Many systems still ship with a few hundred KB of auto-tuned buffer. On a
        high-BDP path that quietly caps the window — and therefore throughput — far
        below the link rate. Raising <Mono>tcp_rmem</Mono>/<Mono>tcp_wmem</Mono> to
        ~2× BDP is the first lever in any tuning effort.
      </Note>
    </div>
  );
}

// 8 ── Bottleneck chain ──────────────────────────────────────────────────────
function ChainSlide() {
  return (
    <div>
      <Lead>
        Throughput is a chain of ceilings. Each link can only be as fast as the
        weakest one above it:
      </Lead>
      <Formula>buffer ≥ window ≥ BDP → full line rate</Formula>
      <div style={{ display:"flex", gap:14, flexWrap:"wrap", margin:"18px 0" }}>
        <BigStat value="≥ 2× BDP" label="socket buffer" color={P.cyan} />
        <BigStat value="= grows to BDP" label="TCP window" color={P.purple} />
        <BigStat value="= bandwidth × RTT" label="data in flight" color={P.accent} />
        <BigStat value="= line rate" label="throughput" color={P.green} />
      </div>
      <Note color={P.green}>
        Get all three lined up and TCP fills the pipe. Starve any one of them and
        the whole connection drops to that link's limit — no matter how much
        bandwidth you bought. Now we follow this chain up into Kafka.
      </Note>
    </div>
  );
}

// 9 ── Kafka producer mapping ────────────────────────────────────────────────
function KafkaSlide() {
  return (
    <div>
      <Lead>
        Kafka rides on TCP, so the same rules apply — just renamed. The producer
        builds its own "window" out of two settings:
      </Lead>
      <Formula>effective window = batch.size × max.in.flight.requests</Formula>

      <div style={{ display:"flex", justifyContent:"center", margin:"6px 0 12px" }}>
        <svg viewBox="0 0 600 150" style={{ width:"100%", maxWidth:620 }}>
          <rect x={20} y={40} width={150} height={70} rx={8}
            fill={P.green + "14"} stroke={P.green} strokeWidth="1.8" />
          <text x={95} y={66} fill={P.green} fontSize="14" fontFamily="monospace"
            fontWeight="bold" textAnchor="middle">Producer</text>
          <text x={95} y={86} fill={P.muted} fontSize="11" fontFamily="monospace"
            textAnchor="middle">batch.size · linger.ms</text>

          <line x1={172} y1={75} x2={228} y2={75} stroke={P.yellow} strokeWidth="2" />
          <polygon points="228,69 238,75 228,81" fill={P.yellow} />

          <rect x={240} y={40} width={150} height={70} rx={8}
            fill={P.yellow + "12"} stroke={P.yellow} strokeWidth="1.8" />
          <text x={315} y={66} fill={P.yellow} fontSize="13" fontFamily="monospace"
            fontWeight="bold" textAnchor="middle">TCP pipe</text>
          <text x={315} y={86} fill={P.muted} fontSize="11" fontFamily="monospace"
            textAnchor="middle">BDP, window, buffers</text>

          <line x1={392} y1={75} x2={448} y2={75} stroke={P.yellow} strokeWidth="2" />
          <polygon points="448,69 458,75 448,81" fill={P.yellow} />

          <rect x={460} y={40} width={120} height={70} rx={8}
            fill={P.purple + "14"} stroke={P.purple} strokeWidth="1.8" />
          <text x={520} y={66} fill={P.purple} fontSize="14" fontFamily="monospace"
            fontWeight="bold" textAnchor="middle">Broker</text>
          <text x={520} y={86} fill={P.muted} fontSize="11" fontFamily="monospace"
            textAnchor="middle">partitions</text>
        </svg>
      </div>

      <div style={{ display:"flex", gap:18, flexWrap:"wrap", color:P.text,
        fontSize:"0.95em", lineHeight:1.6, maxWidth:820 }}>
        <div style={{ flex:"1 1 240px" }}>
          <Pill color={P.green}>batch.size</Pill>
          <p style={{ marginTop:8, color:P.muted }}>
            How much data the producer bundles per request — the unit it puts
            "in flight". Too small and the window can't reach the BDP.
          </p>
        </div>
        <div style={{ flex:"1 1 240px" }}>
          <Pill color={P.yellow}>linger.ms</Pill>
          <p style={{ marginTop:8, color:P.muted }}>
            How long it waits to fill a batch before sending. The classic
            throughput-vs-latency knob — a little patience packs fuller pipes.
          </p>
        </div>
      </div>
    </div>
  );
}

// 10 ── Partitions = parallel pipes ──────────────────────────────────────────
function PartitionsSlide() {
  return (
    <div>
      <Lead>
        A single TCP connection has one window and one RTT, so it has one speed
        limit. Kafka scales past it with <strong>partitions</strong> — each is an
        independent pipe, and they run in parallel.
      </Lead>

      <div style={{ display:"flex", justifyContent:"center", margin:"4px 0 10px" }}>
        <svg viewBox="0 0 600 170" style={{ width:"100%", maxWidth:600 }}>
          {[0, 1, 2, 3].map(i => {
            const y = 18 + i * 36;
            return (
              <g key={i} opacity={i === 3 ? 0.45 : 1}>
                <rect x={60} y={y} width={480} height={22} rx={11}
                  fill={P.accent + "12"} stroke={P.accent} strokeWidth="1.4" />
                {[0, 1, 2, 3, 4, 5].map(k => (
                  <rect key={k} x={72 + k * 40} y={y + 5} width={28} height={12} rx={2}
                    fill={P.accent} opacity={0.85 - k * 0.08} />
                ))}
                <text x={30} y={y + 16} fill={P.muted} fontSize="11"
                  fontFamily="monospace" textAnchor="middle">P{i}</text>
              </g>
            );
          })}
          <text x={300} y={162} fill={P.muted} fontSize="12" fontFamily="monospace"
            textAnchor="middle">N partitions ≈ N parallel pipes → N× aggregate throughput</text>
        </svg>
      </div>

      <Note color={P.accent}>
        More partitions multiply aggregate throughput — until they collide with a
        real ceiling: the broker's <strong>NIC bandwidth</strong>, plus
        replication traffic (RF×) sharing the same wire. Parallelism helps right
        up to the point the physical link saturates.
      </Note>
    </div>
  );
}

// 11 ── Concrete example ─────────────────────────────────────────────────────
function ExampleSlide() {
  const rtt = 45, bw = 10000;
  const bdp = bdpBytes(bw, rtt);
  const untuned = tputMbps(64 * 1024, rtt);
  const tuned = Math.min(bw, tputMbps(bdp, rtt));
  const data = [
    { name:"Untuned (64 KB)", mbps:+untuned.toFixed(1), color:P.red },
    { name:"Tuned (≈ BDP)",   mbps:+tuned.toFixed(0),    color:P.green },
  ];
  return (
    <div>
      <Lead>
        Put numbers on it. A <strong>10 Gbps cross-region link</strong> at
        <Mono c={P.yellow}> 45 ms</Mono> RTT. The BDP is
        <Mono c={P.accent}> {fmtBytes(bdp)}</Mono> — that's how much must be in
        flight to fill it.
      </Lead>
      <div style={{ display:"flex", gap:14, flexWrap:"wrap", margin:"16px 0" }}>
        <BigStat value={fmtMbps(untuned)} label="64 KB window" color={P.red}
          sub={`${(untuned / bw * 100).toFixed(2)}% of the link`} />
        <BigStat value={fmtMbps(tuned)} label="window sized to BDP" color={P.green}
          sub="full line rate" />
        <BigStat value={`${Math.round(tuned / untuned)}×`} label="throughput gained"
          color={P.accent} sub="from buffer tuning alone" />
      </div>
      <div style={{ background:P.panel, border:`1px solid ${P.border}`, borderRadius:10,
        padding:"14px 16px" }}>
        <ResponsiveContainer width="100%" height={180}>
          <BarChart data={data} layout="vertical"
            margin={{ top:6, right:80, bottom:6, left:20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" horizontal={false} />
            <XAxis type="number" scale="log" domain={[5, 15000]} stroke={P.muted}
              tick={{ fontSize:11 }} ticks={[10, 100, 1000, 10000]} tickFormatter={fmtMbps} />
            <YAxis type="category" dataKey="name" stroke={P.muted}
              tick={{ fontSize:11 }} width={130} />
            <Tooltip contentStyle={tipStyle} formatter={v => [fmtMbps(v), "throughput"]} />
            <Bar dataKey="mbps" radius={[0, 4, 4, 0]} label={{ position:"right",
              fill:P.text, fontSize:11, formatter:fmtMbps }}>
              {data.map((d, i) => <Cell key={i} fill={d.color} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <Note color={P.green}>
        Same hardware, same link, ~{Math.round(tuned / untuned)}× the throughput —
        purely from letting TCP fill the pipe. This is why measurement-driven
        buffer and window sizing pays off before you touch anything else.
      </Note>
    </div>
  );
}

// 12 ── Go deeper ────────────────────────────────────────────────────────────
function RecapSlide() {
  const terms = [
    { t:"RTT",        d:"Round-trip latency. Sets the pipe length.", c:P.yellow },
    { t:"Bandwidth",  d:"Link capacity. The ceiling, not a promise.", c:P.green },
    { t:"BDP",        d:"bandwidth × RTT. Data needed to fill the pipe.", c:P.accent },
    { t:"TCP window", d:"Allowed in-flight data. throughput = window ÷ RTT.", c:P.purple },
    { t:"Buffers",    d:"rmem/wmem. Cap the window — size to ≥ 2× BDP.", c:P.cyan },
    { t:"Kafka knobs",d:"batch.size, linger.ms, partitions map onto all of the above.", c:P.red },
  ];
  return (
    <div>
      <Lead>
        That's the whole chain: <span style={{ color:P.yellow }}>RTT</span> and
        <span style={{ color:P.green }}> bandwidth</span> define the
        <span style={{ color:P.accent }}> BDP</span>; the
        <span style={{ color:P.purple }}> window</span> and
        <span style={{ color:P.cyan }}> buffers</span> decide whether you reach it;
        Kafka's settings inherit the result.
      </Lead>
      <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fit,minmax(240px,1fr))",
        gap:12, margin:"14px 0" }}>
        {terms.map(x => (
          <div key={x.t} style={{ background:P.panel, border:`1px solid ${x.c}44`,
            borderRadius:10, padding:"12px 14px" }}>
            <Pill color={x.c}>{x.t}</Pill>
            <p style={{ color:P.muted, fontSize:"0.9em", marginTop:8, lineHeight:1.5 }}>{x.d}</p>
          </div>
        ))}
      </div>
      <Note color={P.accent}>
        Ready for the math, the charts, and config you can paste? Open the
        <strong> TCP Throughput Explainer</strong> for the theory and the
        <strong> Kafka TCP Tuning Dashboard</strong> to size your own settings
        from measured RTT, bandwidth, and loss.
      </Note>
    </div>
  );
}

const SLIDES = [
  { kicker:"Overview",            title:"TCP & Kafka Throughput",        Body:TitleSlide },
  { kicker:"The mental model",    title:"A network is a pipe",           Body:PipeModelSlide },
  { kicker:"Concept · 1 of 4",    title:"RTT — round-trip time",         Body:RttSlide },
  { kicker:"Concept · 2 of 4",    title:"Bandwidth — the capacity",      Body:BandwidthSlide },
  { kicker:"Concept · 3 of 4",    title:"BDP — filling the pipe",        Body:BdpSlide },
  { kicker:"Concept · 4 of 4",    title:"The TCP window",                Body:WindowSlide },
  { kicker:"The hidden ceiling",  title:"Buffers back the window",       Body:BuffersSlide },
  { kicker:"Putting it together", title:"The bottleneck chain",          Body:ChainSlide },
  { kicker:"From TCP to Kafka",   title:"Producer batches = window",     Body:KafkaSlide },
  { kicker:"Scaling out",         title:"Partitions = parallel pipes",   Body:PartitionsSlide },
  { kicker:"Orders of magnitude", title:"What tuning is worth",          Body:ExampleSlide },
  { kicker:"Recap",               title:"The whole chain & next steps",  Body:RecapSlide },
];

// ════════════════════════════════════════════════════════════════════════════
// Shell
// ════════════════════════════════════════════════════════════════════════════
export default function App() {
  const [idx, setIdx] = useState(0);
  const n = SLIDES.length;
  const go = useCallback(d => setIdx(i => Math.max(0, Math.min(n - 1, i + d))), [n]);

  useEffect(() => {
    const h = e => {
      const tag = e.target?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if (e.key === "ArrowRight" || e.key === "PageDown" || e.key === " ") { e.preventDefault(); go(1); }
      else if (e.key === "ArrowLeft" || e.key === "PageUp") { e.preventDefault(); go(-1); }
      else if (e.key === "Home") setIdx(0);
      else if (e.key === "End") setIdx(n - 1);
    };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [go, n]);

  const slide = SLIDES[idx];

  return (
    <div style={{ background:P.bg, color:P.text, minHeight:"100vh",
      fontFamily:"'Inter','Segoe UI',-apple-system,sans-serif", display:"flex",
      flexDirection:"column" }}>
      <style>{`
        @keyframes slideIn { from { opacity:0; transform:translateY(10px); }
                             to   { opacity:1; transform:translateY(0); } }
        input[type=range]{ -webkit-appearance:none; height:5px; border-radius:3px;
          background:#30363d; }
      `}</style>

      {/* progress bar */}
      <div style={{ height:3, background:"#161b22" }}>
        <div style={{ height:"100%", width:`${((idx + 1) / n) * 100}%`,
          background:P.accent, transition:"width 0.3s ease" }} />
      </div>

      {/* header */}
      <header style={{ padding:"22px 32px 6px", maxWidth:920, margin:"0 auto",
        width:"100%" }}>
        <div style={{ color:P.accent, fontSize:"0.78em", fontWeight:700,
          letterSpacing:"0.12em", textTransform:"uppercase",
          fontFamily:"'JetBrains Mono',monospace" }}>{slide.kicker}</div>
        <h2 style={{ margin:"6px 0 0", fontSize:"1.7em", fontWeight:800,
          color:P.text, letterSpacing:"-0.01em" }}>{slide.title}</h2>
      </header>

      {/* body */}
      <main style={{ flex:1, padding:"18px 32px 24px", maxWidth:920, margin:"0 auto",
        width:"100%", overflowY:"auto" }}>
        <div key={idx} style={{ animation:"slideIn 0.35s ease" }}>
          <slide.Body />
        </div>
      </main>

      {/* footer nav */}
      <footer style={{ borderTop:`1px solid ${P.border}`, padding:"14px 32px",
        display:"flex", alignItems:"center", justifyContent:"space-between",
        gap:16, maxWidth:920, margin:"0 auto", width:"100%" }}>
        <NavBtn onClick={() => go(-1)} disabled={idx === 0}>← Prev</NavBtn>

        <div style={{ display:"flex", gap:8, alignItems:"center", flexWrap:"wrap",
          justifyContent:"center" }}>
          {SLIDES.map((_, i) => (
            <button key={i} onClick={() => setIdx(i)} aria-label={`Slide ${i + 1}`}
              style={{ width:i === idx ? 22 : 9, height:9, borderRadius:5, border:"none",
                background:i === idx ? P.accent : "#30363d", cursor:"pointer",
                padding:0, transition:"all 0.2s" }} />
          ))}
        </div>

        <div style={{ display:"flex", alignItems:"center", gap:14 }}>
          <span style={{ color:P.muted, fontSize:"0.82em",
            fontFamily:"'JetBrains Mono',monospace" }}>{idx + 1} / {n}</span>
          <NavBtn onClick={() => go(1)} disabled={idx === n - 1} primary>Next →</NavBtn>
        </div>
      </footer>
    </div>
  );
}

function NavBtn({ children, onClick, disabled, primary }) {
  return (
    <button onClick={onClick} disabled={disabled}
      style={{ background:primary && !disabled ? P.accent : "transparent",
        color:disabled ? "#3d444d" : primary ? P.bg : P.text,
        border:`1px solid ${disabled ? "#21262d" : primary ? P.accent : P.border}`,
        borderRadius:7, padding:"8px 18px", fontSize:"0.9em", fontWeight:600,
        cursor:disabled ? "default" : "pointer", transition:"all 0.15s",
        fontFamily:"inherit" }}>
      {children}
    </button>
  );
}
