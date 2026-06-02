# CLAUDE.md — TCP Kafka Tuning

Project context for Claude Code. Read this before making any changes.

---

## Project Overview

**TCP Kafka Tuning** is a measurement-driven configuration reference for TCP/IP and Apache Kafka throughput optimisation. It consists of:

- Two interactive React dashboards (Vite + recharts)
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
│   └── kafka-tcp-tuning.jsx           # Kafka tuning dashboard — standalone
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
│   │   ├── vite.config.js             # base: '/' — must change for GitHub Pages
│   │   ├── index.html
│   │   └── src/
│   │       ├── main.jsx
│   │       └── App.jsx                # ← mirrors dashboards/tcp-throughput-explainer.jsx
│   └── kafka/
│       ├── package.json
│       ├── vite.config.js             # base: '/' — must change for GitHub Pages
│       ├── index.html
│       └── src/
│           ├── main.jsx
│           └── App.jsx                # ← mirrors dashboards/kafka-tcp-tuning.jsx
│
├── docs/
│   ├── kafka-tcp-tuning-guide.md      # full technical reference (537 lines, 10 sections)
│   └── kafka-tcp-tuning-guide.docx    # Word version for formal distribution
│
├── .gitignore
├── CLAUDE.md                          # ← this file
└── README.md
```

### Critical sync rule

`docker/tcp/src/App.jsx` and `docker/kafka/src/App.jsx` are **copies** of the dashboard files. Whenever a dashboard is changed, both locations must be updated:

```bash
cp dashboards/tcp-throughput-explainer.jsx docker/tcp/src/App.jsx
cp dashboards/kafka-tcp-tuning.jsx         docker/kafka/src/App.jsx
```

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
5. Mathis Equation — loss-limited throughput chart
6. Quick Reference Scenarios table
7. Linux Tuning Cheatsheet
8. BBR vs CUBIC — interactive simulation (4 charts + table)
9. References & Standards (15 clickable entries)

**Log-Y state:** `logBdp`, `logTputRtt`, `logWin`, `logRtt`, `logCwnd`, `logMath` in App; `logCwnd`, `logRtt2`, `logQueue`, `logTput` inside `BbrComparison`.

**Import note:** File uses named imports only — `import { useState, useEffect, useRef } from "react"`. Never use `React.useState` — `React` is not imported as a default and will throw `ReferenceError: React is not defined` at runtime.

---

### `kafka-tcp-tuning.jsx`

Self-contained React app. Structure:

**Shared components:**
- `Slider` — range slider with click-to-type (uses named `useState`, `editing`/`draft` state)
- `LogToggle` / `ChartHeader` — same pattern as tcp explainer
- `yAxisProps(logScale, minVal, labelText, extra={})` — recharts YAxis helper
- `StatBox`, `Card`, `Label`, `TabBtn`, `DiagBadge` — layout primitives

**Core calculation:**
```js
calcFromMeasurements({
  bwMbps, rttMin, rttAvg, plateauKB, conns, mtu,
  inflight, latencyBudgetMs, pktLoss, partitions, compressionRatio
})
```
Returns: `empiricalBDP`, `theoreticalBDP`, `mss`, `bufCeil`, `batchSize`, `batchMin`, `lingerThru`, `lingerLatency`, `mathisMbps`, `kafkaWireMbps`, `kafkaLogicalMbps`, `kafkaWindowMbps`, `effectiveMbps`, `effectiveLogicalMbps`, `perPartWireMbps`, `perPartLogicalMbps`, `perPartWindowBytes`, `perPartBdpPct`, `partitionSeries`.

**Data generators:**
- `simWindowSweep(bwMbps, rttMs)` — plateau detection for overview chart
- `simBbrVsCubic(bwMbps, rttMs, bufMss)` — 70-round BBR/CUBIC simulation

**Tabs:** `overview`, `throughput`, `bbr`, `sysctl`, `kafka`, `broker`, `table`, `scripts`

**Scenario presets:** Local DC, Same-AZ, Cross-AZ, Cross-Region, Multi-Region, Satellite, Custom

**Sliders:** bandwidth (10–50000 Mbps), RTT min/avg, packet loss, MTU, connections, latency budget, max in-flight, partitions (1–256), compression ratio (1–6×)

**Log-Y state:** `logWindow`, `logPartChart`, `logBbrCwnd`, `logBbrRtt`, `logBbrQueue`, `logBbrTput`

**Critical label semantics:**
- "Total wire throughput" / "Total app data rate" = partition-independent totals
- "Per partition (Np)" = total ÷ partitions — this is what responds to the partitions slider
- The Mathis formula `T = MSS / (RTT × √p)` returns **bytes/sec** — divide by **125,000** (not 1,000,000) to get Mbit/s. Dividing by 1e6 gives values 8× too small.

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

---

## Scripts

### `kafka-tcp-measure.sh`
- **Platform:** `nicolaka/netshoot` (bash + python3 + iperf3 + ping)
- **Dependencies:** `iperf3`, `ping`, `python3`, `bc` — no GNU grep (`-P` flag not used)
- **Phases:** ping RTT (200 samples) → iperf3 window sweep (4KB–4MB) → parallel streams (1/2/4/8) → Nagle test
- **Output:** `results/<timestamp>/` containing `ping.csv`, `window_sweep.csv`, `parallel_sweep.csv`, `nodelay_comparison.csv`, `meta.env`
- **Security:** uses `parse_env()` — never `source` untrusted files; python3 values passed as argv not heredoc interpolation

### `kafka-tcp-analyze.sh`
- **Input:** directory from `kafka-tcp-measure.sh` (-d flag)
- **Computes:** BDP, buffer ceiling (`BDP × conns × 2`, rounded to power of 2), batch size, linger.ms, Mathis bound
- **Output:** `99-kafka-tcp.conf`, `producer-throughput.properties`, `producer-latency.properties`, `broker-additions.properties`

### `kafka-tcp-k8s.yaml`
- Resources: PVC (`kafka-tcp-results`, ReadWriteOnce, 1Gi), Job (`kafka-tcp-measure`), Pod (`results-reader`)
- **Critical:** Job command must capture timestamp once: `TIMESTAMP=$(date +%Y%m%d-%H%M%S) && /scripts/kafka-tcp-measure.sh ... -o /results/$TIMESTAMP`
- Scripts mounted via ConfigMap (`kafka-tcp-scripts`, `defaultMode: 0755`)
- Results retrieved via: `kubectl exec -i results-reader -- sh -c 'ls -1 /results | grep "^[0-9]" | sort -r | head -n1'` + `tar cf - | tar xf -`

---

## Docker Build

- **Build stage:** `node:22-alpine` — Vite builds both apps
- **Runtime stage:** `alpine:3.20` + `busybox-extras` — serves via `httpd` (NOT `busybox httpd`)
- **Ports:** 3001 (TCP explainer), 3002 (Kafka tuning)
- **Entrypoint:** `/entrypoint.sh` starts two `httpd -f -p <port> -h /srv/<app>` processes

```bash
# Build
podman build -t tcp-kafka-viz docker/
buildah bud   -t tcp-kafka-viz docker/

# Run
podman run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
```

---

## Running Locally (without Docker)

```bash
# Dev server (hot reload)
cd docker/tcp   && npm install && npm run dev   # → http://localhost:5173
cd docker/kafka && npm install && npm run dev   # → http://localhost:5174

# Static build + serve
cd docker/tcp   && npm install && npm run build
cd docker/kafka && npm install && npm run build
python3 -m http.server 3001 --directory docker/tcp/dist   &
python3 -m http.server 3002 --directory docker/kafka/dist &
# or
npx serve -l 3001 docker/tcp/dist
npx serve -l 3002 docker/kafka/dist
```

**Common error:** Black page = `ReferenceError: React is not defined`. Caused by `React.useState` in component code. Always use named import: `useState` not `React.useState`.

---

## Open Items / Next Steps

1. **GitHub Pages deployment** — not yet set up. Requires:
   - `vite.config.js` `base` now reads `process.env.VITE_BASE ?? '/'` — set `VITE_BASE=/tcp-kafka-tuning/tcp/` (or `/kafka/`) in the CI build step
   - Add `.github/workflows/deploy.yml` (GitHub Actions build + deploy)
   - Built output goes to `docs/tcp/` and `docs/kafka/`
   - Enable Pages in repo Settings → Pages → Source: GitHub Actions

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

Primary academic sources:
- Little (1961) Op.Res. 9(3) — L=λW
- RFC 1323 (1992) — BDP, window scaling, Long Fat Networks
- Mathis et al. (1997) ACM SIGCOMM CCR 27(3) — T=MSS/(RTT×√p)
- Cardwell et al. (2016) ACM Queue 14(5) — BBR
- Ha, Rhee, Xu (2008) ACM SIGOPS OSR 42(5) — CUBIC
- RFC 5681 (2009) — TCP congestion control
