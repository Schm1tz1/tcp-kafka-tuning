# CLAUDE.md — TCP Kafka Tuning

Project context for Claude Code. Read this before making any changes.

---

## Project Overview

**TCP Kafka Tuning** is a measurement-driven configuration reference for TCP/IP and Apache Kafka throughput optimisation. It consists of:

- Three interactive React apps (Vite + recharts): two detailed dashboards plus a concept-level guided slideshow
- Two shell scripts for network measurement and analysis
- A Kubernetes deployment manifest
- A full technical reference document
- A Docker build for self-hosting

The project is grounded in published network performance theory: Little's Law [1961], RFC 1323 [1992], the Mathis throughput bound [1997], and the BBR congestion-control model [2016].

---

## Repository Structure

```
├── dashboards/
│   ├── tcp-throughput-explainer.jsx   # TCP theory explainer — standalone
│   ├── kafka-tcp-tuning.jsx           # Kafka tuning dashboard — standalone
│   └── tcp-kafka-slideshow.jsx        # Guided concept slideshow — standalone
│
├── scripts/
│   ├── kafka-tcp-measure.sh           # ping + iperf3 window sweep → CSV output
│   ├── kafka-tcp-analyze.sh           # reads CSV, computes BDP, writes configs
│   └── kafka-tcp-k8s.yaml             # k8s PVC + Job + reader pod manifests
│
├── docker/
│   ├── Dockerfile                     # multi-stage: node:22-alpine → alpine
│   ├── docker-compose.yml
│   ├── .dockerignore
│   ├── scripts/entrypoint.sh          # starts two httpd instances
│   ├── tcp/                           # Vite project for tcp-throughput-explainer
│   │   ├── package.json               # react@18.3.1, recharts@2.12.7, vite@5.4.2
│   │   ├── vite.config.js             # base path + resolve.dedupe for out-of-root import
│   │   ├── index.html
│   │   └── src/
│   │       └── main.jsx               # imports @dashboards/tcp-throughput-explainer.jsx
│   ├── kafka/
│   │   ├── package.json
│   │   ├── vite.config.js             # base path + resolve.dedupe for out-of-root import
│   │   ├── index.html
│   │   └── src/
│   │       └── main.jsx               # imports @dashboards/kafka-tcp-tuning.jsx
│   └── slides/
│       ├── package.json
│       ├── vite.config.js             # base path + resolve.dedupe for out-of-root import
│       ├── index.html
│       └── src/
│           └── main.jsx               # imports @dashboards/tcp-kafka-slideshow.jsx
│
├── docs/
│   ├── kafka-tcp-tuning-guide.md      # full technical reference (537 lines, 10 sections)
│   └── kafka-tcp-tuning-guide.docx    # Word version for formal distribution
│
├── static/
│   └── index.html                     # landing page linking both dashboards (deployed to GitHub Pages root)
│
├── .gitignore
├── CLAUDE.md                          # ← this file
└── README.md
```

### Single source of truth

`dashboards/tcp-throughput-explainer.jsx`, `dashboards/kafka-tcp-tuning.jsx`, and `dashboards/tcp-kafka-slideshow.jsx` are the **only** copies of the app code. The Vite projects in `docker/tcp/`, `docker/kafka/`, and `docker/slides/` import them directly via the `@dashboards` alias in `main.jsx` — there are no `App.jsx` files inside `docker/*/src/`.

`vite.config.js` in each project sets `resolve.dedupe` for `react`, `react-dom`, and `recharts` so that Vite resolves those packages from the project's own `node_modules` even though the source file lives outside the project root. `server.fs.allow` permits the dev server to serve the out-of-root file.

Edit only `dashboards/*.jsx`. No manual copy step is needed.

---

## Dashboard Architecture

### `tcp-throughput-explainer.jsx`

Self-contained React app (no external state, no API calls). Structure:

**Shared components:**
- `SliderField` — range slider with click-to-type number input (uses named `useState`)
- `LogToggle` / `ChartHeader` — log-Y axis toggle button + chart header row
- `yAxisProps(logScale, minVal, labelText)` — returns recharts YAxis props
- `BdpCalc` — interactive BDP calculator with two SliderField controls
- `BbrComparison` — full BBR vs CUBIC simulation with 4 charts + behaviour table
- `SectionHeading`, `Mono`, `Formula`, `Tag` — layout primitives

**Data generators** (pure functions, no state):
- `bdpData()` — BDP in KB by RTT and bandwidth
- `throughputVsWindow()` — T=W/RTT for window sizes 4KB–64MB
- `cwndData()` — TCP Reno sawtooth simulation (RFC 5681)
- `mathisData()` — Mathis bound T = MSS/(RTT×√p)/125000 Mbps
- `throughputVsRtt()` — T=W/RTT for RTT 1–500ms, 5 window sizes
- `simBbrVsCubic(bwMbps, rttMs, bufMss)` — 70-round BBR/CUBIC simulation

**Sections:**
1. Pipe Analogy — BDP chart + new throughput-vs-RTT chart (paired)
2. Receive Window — throughput vs window size chart
3. Throughput vs RTT — window as ceiling
4. cwnd / Slow Start / AIMD — sawtooth chart
4.5. MTU, MSS, and Path Fragmentation — packet efficiency chart, cloud provider MTU table, PMTUD explanation, fragmentation loss amplification (formula: P_effective = 1 - (1-p)^N, shows how packet loss multiplies with fragment count)
5. Mathis Equation — loss-limited throughput chart
6. Quick Reference Scenarios table
7. **Interactive Linux Tuning Calculator** — scenario dropdown, sliders for bandwidth/RTT/MTU/packet loss, calculates BDP and buffer sizes, generates sysctl config dynamically with recommendations (BBR vs CUBIC, jumbo frames, diagnostics for loss-limited/low-BDP/high-BDP scenarios)
8. BBR vs CUBIC — interactive simulation (4 charts + table)
9. References & Standards (15 clickable entries)

**Log-Y state:** `logBdp`, `logTputRtt`, `logWin`, `logRtt`, `logCwnd`, `logMath` in App; `logCwnd`, `logRtt2`, `logQueue`, `logTput` inside `BbrComparison`.

**Interactive tuning calculator state (Section 7):**
- `tuningScenario`, `tuningBw`, `tuningRtt`, `tuningMtu`, `tuningLoss`
- `applyTuningScenario(id)` — applies preset from SCENARIOS array
- Uses same SCENARIOS array as Kafka dashboard (consolidated cloud/on-prem scenarios)
- Dynamically calculates: BDP, buffer ceiling (2× BDP rounded to power of 2), MSS, Mathis limit, window utilization
- Generates sysctl config with calculated values, BBR recommendation for high-BDP, jumbo frame commands if MTU=9000
- Diagnostic messages for loss-limited, low-BDP (no tuning needed), and high-BDP (RFC 1323 Window Scale required) scenarios

**Import note:** File uses named imports only — `import { useState, useEffect, useRef } from "react"`. Never use `React.useState` — `React` is not imported as a default and will throw `ReferenceError: React is not defined` at runtime.

---

### `kafka-tcp-tuning.jsx`

Self-contained React app. Structure:

**Shared components:**
- `Slider` — range slider with click-to-type (uses named `useState`, `editing`/`draft` state), accepts optional `help` prop for tooltips
- `HelpIcon` — question mark icon with hover tooltip, used by Slider component
- `LogToggle` / `ChartHeader` — same pattern as tcp explainer
- `yAxisProps(logScale, minVal, labelText, extra={})` — recharts YAxis helper
- `StatBox`, `Card`, `Label`, `TabBtn`, `DiagBadge` — layout primitives

**Expected throughput dual-mode control:**
- Toggle button switches between "%" (percentage of link capacity) and "Mbps" (absolute throughput)
- Percentage mode: 1-100%, shows calculated Mbps equivalent
- Absolute mode: 1-bwMbps Mbps, shows percentage equivalent
- State: `throughputMode`, `expectedThroughputPct`, `expectedThroughputMbps`
- Calculation uses `throughputMode === 'percent' ? effectiveMbps * (pct/100) : absoluteMbps`

**Core calculation:**
```js
calcFromMeasurements({
  bwMbps, rttMin, rttAvg, plateauKB, conns, mtu,
  inflight, latencyBudgetMs, pktLoss, partitions, compressionRatio,
  replicationFactor, brokers, producerCount, consumerCount,
  expectedThroughputPct, expectedThroughputMbps, throughputMode
})
```
Returns: `empiricalBDP`, `theoreticalBDP`, `mss`, `bufCeil`, `batchSize`, `batchMin`, `lingerThru`, `lingerLatency`, `mathisMbps`, `kafkaWireMbps`, `kafkaLogicalMbps`, `kafkaWindowMbps`, `effectiveMbps`, `effectiveLogicalMbps`, `expectedProducerMbps`, `perPartWireMbps`, `perPartLogicalMbps`, `perPartWindowBytes`, `perPartBdpPct`, `partitionSeries`, consumer/broker config values, per-broker bandwidth analysis, MTU/MSS metrics.

**Data generators:**
- `simWindowSweep(bwMbps, rttMs)` — plateau detection for overview chart
- `simBbrVsCubic(bwMbps, rttMs, bufMss)` — 70-round BBR/CUBIC simulation

**Tabs:** `overview`, `throughput`, `bbr`, `mtu`, `sysctl`, `kafka`, `consumer`, `broker`, `table`, `scripts`

**MTU tab features:**
- Packet efficiency chart (header overhead %, packets/MB) at MTU 576/1500/9000
- Single-packet throughput vs MTU chart for different RTT scenarios
- Cloud provider MTU limits table (AWS 9001/1500, GCP 8896/1460, Azure 9000/1400)
- Path MTU Discovery (PMTUD) diagnostics with tracepath/ss/iptables commands
- **Fragmentation Loss Amplification analysis** — calculates effective packet loss when configured MTU > path MTU, shows P_effective = 1 - (1-p)^N where N = fragments per packet, displays loss amplification factor and Mathis throughput degradation, highlights current fragmentation scenario with warnings and recommendations

**Scenario presets:** Local DC (Jumbo), Cloud (Same AZ/Zone, Cross-AZ, Cross-Region, Internet egress), On-Prem (Same DC, Cross-DC), Cross-Region WAN, Multi-Region (Global), Satellite, Custom

**Sliders:** per-broker bandwidth limit (10–50000 Mbps), RTT min/avg, packet loss, MTU, connections, latency budget, max in-flight, brokers (1–24), partitions (1–256), replication factor (1–10), compression ratio (1–6×), producer count (1–100), consumer count (1–100), expected throughput (dual mode: % of link capacity OR absolute Mbps value, default 20% or 2000 Mbps)

**Log-Y state:** `logWindow`, `logPartChart`, `logBbrCwnd`, `logBbrRtt`, `logBbrQueue`, `logBbrTput`

**Critical label semantics:**
- "Total wire throughput" / "Total app data rate" = partition-independent totals
- "Per partition (Np)" = total ÷ partitions — this is what responds to the partitions slider
- The Mathis formula `T = MSS / (RTT × √p)` returns **bytes/sec** — divide by **125,000** (not 1,000,000) to get Mbit/s. Dividing by 1e6 gives values 8× too small.

**Consumer configuration (F13, F14):**
- `max.partition.fetch.bytes` ≥ `batch.size` — receive window must accommodate full producer batches
- `fetch.min.bytes` = `batch.size / 2` — broker accumulates this much data before responding
- `fetch.max.wait.ms` — symmetric to producer `linger.ms`, controls batching vs latency on receive side
- `receive.buffer.bytes` ≥ BDP — TCP receive buffer sizing

**Broker replication (F15, F16, F17, F18, F19):**
- `replica.fetch.max.bytes` ≥ `batch.size` — followers fetch complete batches
- `num.replica.fetchers` = `ceil(partitions / 6)` — one fetcher thread per ~6 partitions
- `replica.lag.time.max.ms` = `RTT×4 + fetch.max.wait + 5000ms` — timeout before out-of-sync
- `replica.socket.receive.buffer.bytes` ≥ BDP — follower receive buffer
- Total follower connections = `partitions × (replicationFactor - 1)`

**Per-broker bandwidth model (F18, F19) — CRITICAL:**
- Bandwidth slider = **per-broker NIC limit** (cloud VM network cap), NOT total cluster bandwidth
- Each broker NIC handles: producer ingress + consumer egress + replication IN + replication OUT
- Replication amplification (F18): RF=3 → 3× write amplification on leader NIC (1× ingress + 2× repl OUT)
- Per-broker constraint (F19): `producer_in + consumer_out + repl_in + repl_out ≤ NIC_limit`
- With N partitions, B brokers, RF replication:
  - Partitions/broker = N/B
  - Each broker handles ~N/B leader partitions + follower fetches for other partitions
  - Per-broker replication OUT = (producer_throughput / B) × (RF-1)
  - Per-broker replication IN = producer_throughput × ((N - N/B) / N)
- Bottleneck: when per-broker total > NIC limit → scale brokers or reduce RF

**Import note:** File uses `import { useState, useCallback } from "react"` — same rule, no `React.` prefix anywhere.

---

## Key Formulas

| ID | Formula | Source | Notes |
|---|---|---|---|
| F1 | `T = W / RTT` | Little 1961; RFC 1323 | Master formula — everything derives from this |
| F2 | `BDP = B × RTT` | RFC 1323 §1 | In bytes: `(bwMbps * 1e6 / 8) * (rttMs / 1000)` |
| F4 | `T ≤ MSS / (RTT × √p)` | Mathis et al. 1997 | Result is **bytes/sec** → divide by 125000 for Mbps |
| F9 | `Inflight = BtlBw × RTprop` | Cardwell et al. 2016 | BBR targets exactly BDP |
| F10 | `W_eff = batch.size × max.in.flight` | Kafka docs | Application of F1 |
| F11 | `linger_t = (batch × 8) / B × 1000` | Derived from F1 | Batch drain time in ms |
| F13 | `max.partition.fetch.bytes ≥ batch.size` | Derived from F1 | Consumer receive window |
| F14 | `fetch.max.wait.ms` tradeoff | Symmetric to F11 | Consumer batching vs latency |
| F15 | `replica.lag.time.max.ms = RTT×4 + fetch.max.wait + 5000` | Timeout budget | Replication timeout |
| F16 | `num.replica.fetchers = ceil(partitions / 6)` | Parallelism heuristic | Fetcher thread scaling |
| F17 | `replica.fetch.max.bytes ≥ batch.size` | Derived from F1 | Full-batch replication |
| F18 | `write_amplification = RF` | Replication I/O | Leader NIC sees RF× writes |
| F19 | `producer + consumer + repl_in + repl_out ≤ NIC_limit` | Per-broker constraint | Cloud VM bandwidth cap |
| F20 | `MSS_eff = min(MTU_vpc, MTU_internet) − 40` | Cloud egress constraint | Hybrid cloud/internet paths |

---

## Scripts

### `kafka-tcp-measure.sh`
- **Platform:** `nicolaka/netshoot` (bash + python3 + iperf3 + ping)
- **Dependencies:** `iperf3`, `ping`, `python3`, `bc`, `ss` (optional for MSS capture), `tracepath` (optional for PMTUD) — no GNU grep (`-P` flag not used)
- **Phases:** 
  1. MTU detection (auto-detect via `ip link`/`ifconfig`/`netstat -i`)
  2. ping RTT (200 samples) 
  3. iperf3 window sweep (4KB–4MB)
  4. MSS capture (via `ss -tin` during active connection, Linux only)
  5. parallel streams (1/2/4/8)
  6. Nagle test
  7. Path MTU Discovery (via `tracepath` or `ping -M do`)
- **Output:** `results/<timestamp>/` containing `ping.csv`, `window_sweep.csv`, `parallel_sweep.csv`, `nodelay_comparison.csv`, `mss_capture.csv`, `meta.env`
- **meta.env fields:** Includes `DETECTED_MTU`, `NEGOTIATED_MSS`, `PATH_MTU` for MTU/MSS diagnostics
- **Security:** uses `parse_env()` — never `source` untrusted files; python3 values passed as argv not heredoc interpolation

### `kafka-tcp-analyze.sh`
- **Input:** directory from `kafka-tcp-measure.sh` (-d flag), optional `-m <mtu>` flag (auto-detected if not specified)
- **Computes:** BDP, MSS (from MTU), buffer ceiling (`BDP × conns × 2`, rounded to power of 2), batch size, linger.ms, Mathis bound, MTU/MSS diagnostics
- **MTU/MSS Analysis:** Validates negotiated MSS vs calculated, detects MSS clamping, fragmentation risk, jumbo frame support, batch-to-MSS ratio
- **Output:** `99-kafka-tcp.conf`, `producer-throughput.properties`, `producer-latency.properties`, `broker-additions.properties`, `analysis.env` (includes MTU/MSS metrics)

### `kafka-tcp-k8s.yaml`
- Resources: PVC (`kafka-tcp-results`, ReadWriteOnce, 1Gi), Job (`kafka-tcp-measure`), Pod (`results-reader`)
- **Critical:** Job command must capture timestamp once: `TIMESTAMP=$(date +%Y%m%d-%H%M%S) && /scripts/kafka-tcp-measure.sh ... -o /results/$TIMESTAMP`
- Scripts mounted via ConfigMap (`kafka-tcp-scripts`, `defaultMode: 0755`)
- Results retrieved via: `kubectl exec -i results-reader -- sh -c 'ls -1 /results | grep "^[0-9]" | sort -r | head -n1'` + `tar cf - | tar xf -`

---

## Docker Build

- **Build stage:** `node:22-alpine` — Vite builds both apps
- **Runtime stage:** `alpine:3.20` + `busybox-extras` — serves via `httpd` (NOT `busybox httpd`)
- **Ports:** 3001 (TCP explainer), 3002 (Kafka tuning), 3003 (slideshow)
- **Entrypoint:** `/entrypoint.sh` starts three `httpd -f -p <port> -h /srv/<app>` processes
- **Build context:** Repository root (so Dockerfile can access `dashboards/`)

```bash
# Build (from repository root)
podman build -f docker/Dockerfile -t tcp-kafka-viz .
buildah bud  -f docker/Dockerfile -t tcp-kafka-viz .
docker build -f docker/Dockerfile -t tcp-kafka-viz .

# Or use docker-compose (from repository root)
docker-compose -f docker/docker-compose.yml build

# Run
podman run -p 3001:3001 -p 3002:3002 -p 3003:3003 tcp-kafka-viz
docker run -p 3001:3001 -p 3002:3002 -p 3003:3003 tcp-kafka-viz
```

---

## Running Locally (without Docker)

```bash
# Dev server (hot reload)
cd docker/tcp    && npm install && npm run dev   # → http://localhost:5173
cd docker/kafka  && npm install && npm run dev   # → http://localhost:5174
cd docker/slides && npm install && npm run dev   # → http://localhost:5175

# Static build + serve
cd docker/tcp    && npm install && npm run build
cd docker/kafka  && npm install && npm run build
cd docker/slides && npm install && npm run build
python3 -m http.server 3001 --directory docker/tcp/dist    &
python3 -m http.server 3002 --directory docker/kafka/dist  &
python3 -m http.server 3003 --directory docker/slides/dist &
# or
npx serve -l 3001 docker/tcp/dist
npx serve -l 3002 docker/kafka/dist
```

**Common error:** Black page = `ReferenceError: React is not defined`. Caused by `React.useState` in component code. Always use named import: `useState` not `React.useState`.

---

## Open Items / Next Steps

1. **GitHub Pages deployment** — live via GitHub Actions (`peaceiris/actions-gh-pages`), publishing to the `gh-pages` branch:
   - `.github/workflows/deploy-tcp_tools.yml` — `main` branch → site root. Builds tcp/kafka/slides with `VITE_BASE=/tcp-kafka-tuning/<app>/`, assembles `dist/{tcp,kafka,slides}` + `dist/index.html` (from `static/index.html`).
   - `.github/workflows/deploy-uat.yml` — `uat` branch → `/uat/` subpath (`VITE_BASE=/tcp-kafka-tuning/uat/<app>/`, `destination_dir: uat`).
   - **Adding a new app:** add a build step (with its `VITE_BASE`) AND a copy line in the "Assemble dist/" step of BOTH workflows, or it deploys as a broken link.

2. **Slider component naming inconsistency** — `tcp-throughput-explainer.jsx` uses `SliderField({val, set, fmt})` while `kafka-tcp-tuning.jsx` uses `Slider({value, onChange, unit})`. These should be unified if sharing components.

3. **recharts v2 deprecation warning** — `recharts@2.15.4` is installed; v3 migration guide at https://github.com/recharts/recharts/wiki/3.0-migration-guide. Not blocking but worth addressing.

4. **`dist/` not in repo** — always run `npm run build` before serving statically or testing the Docker build. The `dist/` folder is in `.gitignore`.

5. **`docs/` folder** — currently only contains the reference document. After adding GitHub Pages, `docs/tcp/` and `docs/kafka/` will hold the built app output.

---

## Dependency Versions

| Package | Version | Used in |
|---|---|---|
| react | ^18.3.1 | both Vite projects |
| react-dom | ^18.3.1 | both Vite projects |
| recharts | ^2.12.7 (actual: 2.15.4) | both Vite projects |
| @vitejs/plugin-react | ^4.3.1 | both Vite projects |
| vite | ^5.4.2 | both Vite projects |
| node (build) | 22-alpine | Dockerfile |
| alpine (runtime) | 3.20 | Dockerfile |

---

## Reference Documents

- `docs/kafka-tcp-tuning-guide.md` — 537 lines, 10 sections, 12 formulas with full citations
- `README.md` — repo landing page with full usage guide, deployment options, scenario table
- `static/index.html` — web landing page with cards linking to both dashboards, updated to highlight MTU/MSS fragmentation analysis, interactive Linux tuning calculator, 11 consolidated cloud/on-prem scenarios, dual-mode expected throughput, and scaling to 24 brokers with RF=10

Primary academic sources:
- Little (1961) Op.Res. 9(3) — L=λW
- RFC 1323 (1992) — BDP, window scaling, Long Fat Networks
- Mathis et al. (1997) ACM SIGCOMM CCR 27(3) — T=MSS/(RTT×√p)
- Cardwell et al. (2016) ACM Queue 14(5) — BBR
- Ha, Rhee, Xu (2008) ACM SIGOPS OSR 42(5) — CUBIC
- RFC 5681 (2009) — TCP congestion control

# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
