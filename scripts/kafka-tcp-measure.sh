#!/usr/bin/env bash
# =============================================================================
# kafka-tcp-measure.sh
# Measures RTT, throughput, and window-sweep data for Kafka TCP tuning.
#
# Usage:
#   ./kafka-tcp-measure.sh -t <broker-ip> [-p <port>] [-s <streams>] [-o <outdir>]
#
# Requirements: iperf3, ping, python3, bc
# Tested on: nicolaka/netshoot (bash + python3 + busybox utils)
# Note: grep -P is NOT required — python3 handles all text parsing
# =============================================================================

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
TARGET=""
PORT=5201
MAX_STREAMS=8
OUTDIR="./kafka-tcp-results-$(date +%Y%m%d-%H%M%S)"
PING_COUNT=200
IPERF_DURATION=15
IPERF_CMD="iperf3"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
ok()      { echo -e "${GREEN}[ OK ]${RESET}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}[ERR ]${RESET}  $*" >&2; exit 1; }
section() { echo -e "\n${BOLD}══════════════════════════════════════════${RESET}"; \
            echo -e "${BOLD}  $*${RESET}"; \
            echo -e "${BOLD}══════════════════════════════════════════${RESET}"; }

# ── Argument parsing ──────────────────────────────────────────────────────────
usage() {
    echo "Usage: $0 -t <broker-ip> [-p <port>] [-s <max-streams>] [-o <outdir>]"
    echo "  -t  Target broker IP/hostname (required)"
    echo "  -p  iperf3 server port        (default: 5201)"
    echo "  -s  Max parallel streams      (default: 8)"
    echo "  -o  Output directory          (default: ./kafka-tcp-results-<timestamp>)"
    exit 1
}

while getopts "t:p:s:o:h" opt; do
    case $opt in
        t) TARGET=$OPTARG ;;
        p) PORT=$OPTARG ;;
        s) MAX_STREAMS=$OPTARG ;;
        o) OUTDIR=$OPTARG ;;
        h|*) usage ;;
    esac
done

[[ -z "$TARGET" ]] && usage

# ── Preflight checks ──────────────────────────────────────────────────────────
for cmd in $IPERF_CMD ping python3 bc; do
    command -v "$cmd" &>/dev/null || error "Required command not found: $cmd"
done

mkdir -p "$OUTDIR"
META="$OUTDIR/meta.env"
PING_CSV="$OUTDIR/ping.csv"
WINDOW_CSV="$OUTDIR/window_sweep.csv"
PARALLEL_CSV="$OUTDIR/parallel_sweep.csv"
NODELAY_CSV="$OUTDIR/nodelay_comparison.csv"
SUMMARY="$OUTDIR/summary.txt"

echo "TARGET=$TARGET"           > "$META"
echo "PORT=$PORT"               >> "$META"
echo "MAX_STREAMS=$MAX_STREAMS" >> "$META"
echo "TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$META"

# ── Helper: extract iperf3 JSON field ─────────────────────────────────────────
iperf_extract() {
    # $1=json_file, $2=python_expression
    python3 - "$1" "$2" << 'PYEOF'
import json, sys
try:
    with open(sys.argv[1]) as f:
        d = json.load(f)
    print(eval(sys.argv[2]))
except Exception as e:
    print(0)
PYEOF
}

run_iperf() {
    # $1=window_bytes $2=streams $3=nodelay(0|1) $4=output_json
    # Returns iperf3 exit code. Stderr written to $4.err for inspection.
    local WIN=$1 STREAMS=$2 NODELAY=$3 OUTJSON=$4
    local NODELAY_FLAG=""
    [[ "$NODELAY" == "1" ]] && NODELAY_FLAG="--no-delay"
    local WIN_FLAG=""
    [[ "$WIN" -gt "0" ]] && WIN_FLAG="-w $WIN"

    # Capture stderr separately so we can surface it on failure
    # Do NOT use 2>/dev/null — we need to know why iperf3 failed
    $IPERF_CMD -c "$TARGET" -p "$PORT" \
        -t "$IPERF_DURATION" \
        -P "$STREAMS" \
        $WIN_FLAG \
        $NODELAY_FLAG \
        -J > "$OUTJSON" 2>"${OUTJSON}.err"
    # Return iperf3's exit code explicitly
    return $?
}

# =============================================================================
# PHASE 1 — RTT measurement
# =============================================================================
section "Phase 1/4 — RTT measurement (ping)"
info "Sending $PING_COUNT pings to $TARGET..."

PING_TMP=$(mktemp)
ping -c "$PING_COUNT" -i 0.05 "$TARGET" > "$PING_TMP" 2>&1 || \
    { rm -f "$PING_TMP"; error "ping failed — is $TARGET reachable?"; }

# Parse with python3 — no grep -P needed, handles all ping output formats:
#   GNU iputils:  rtt min/avg/max/mdev = 0.08/0.12/0.45/0.04 ms
#   BSD/macOS:    round-trip min/avg/max/stddev = 0.08/0.12/0.45/0.04 ms
#   BusyBox:      round-trip min/avg/max = 0.08/0.12/0.45 ms  (no mdev field)
# Writing to a temp file avoids quoting issues with heredoc-inside-$()
read -r RTT_MIN RTT_AVG RTT_MAX RTT_MDEV PKT_LOSS <<< \
    "$(python3 - "$PING_TMP" << 'PYEOF'
import re, sys

with open(sys.argv[1]) as f:
    text = f.read()

# RTT line: "= min/avg/max[/mdev]  ms"
m = re.search(r'=\s*([\d.]+)/([\d.]+)/([\d.]+)(?:/([\d.]+))?\s*ms', text)
if m:
    rtt_min, rtt_avg, rtt_max = m.group(1), m.group(2), m.group(3)
    rtt_mdev = m.group(4) if m.group(4) else "0"
else:
    rtt_min = rtt_avg = rtt_max = rtt_mdev = "0"

# Loss: "N% packet loss" or "N.N% packet loss"
lm = re.search(r'([\d.]+)%\s+packet loss', text)
pkt_loss = lm.group(1) if lm else "0"

print(rtt_min, rtt_avg, rtt_max, rtt_mdev, pkt_loss)
PYEOF
    )"
rm -f "$PING_TMP"

echo "target,rtt_min_ms,rtt_avg_ms,rtt_max_ms,rtt_mdev_ms,packet_loss_pct" > "$PING_CSV"
echo "$TARGET,$RTT_MIN,$RTT_AVG,$RTT_MAX,$RTT_MDEV,$PKT_LOSS" >> "$PING_CSV"
echo "RTT_MIN=$RTT_MIN" >> "$META"
echo "RTT_AVG=$RTT_AVG" >> "$META"
echo "RTT_MDEV=$RTT_MDEV" >> "$META"
echo "PKT_LOSS=$PKT_LOSS" >> "$META"

ok "RTT min=${RTT_MIN}ms  avg=${RTT_AVG}ms  mdev=${RTT_MDEV}ms  loss=${PKT_LOSS}%"

# =============================================================================
# PHASE 2 — Window sweep (single stream)
# =============================================================================
section "Phase 2/4 — Window sweep (single stream)"

WINDOWS=(4096 8192 16384 32768 65536 131072 262144 524288 1048576 2097152 4194304)
echo "window_bytes,throughput_mbps,retransmits,mean_rtt_ms,cwnd_avg" > "$WINDOW_CSV"

PREV_MBPS=0
PLATEAU_WIN=0

for WIN in "${WINDOWS[@]}"; do
    WIN_KB=$((WIN / 1024))
    info "  Testing window=${WIN_KB}KB..."
    TMP_JSON=$(mktemp)

    # Run iperf3 — capture exit code without triggering set -e
    if ! run_iperf "$WIN" 1 0 "$TMP_JSON"; then
        IPERF_ERR=$(cat "${TMP_JSON}.err" 2>/dev/null | head -3)
        warn "  iperf3 failed for window=${WIN_KB}KB — skipping"
        warn "  reason: ${IPERF_ERR:-unknown error}"
        # Record the failure in the CSV so the gap is visible
        echo "${WIN},FAILED,,,," >> "$WINDOW_CSV"
        rm -f "$TMP_JSON" "${TMP_JSON}.err"
        sleep 2
        continue
    fi

    BPS=$(iperf_extract "$TMP_JSON" "d['end']['sum_sent']['bits_per_second']")
    RETX=$(iperf_extract "$TMP_JSON" "d['end']['sum_sent']['retransmits']")
    MRTT=$(iperf_extract "$TMP_JSON" \
        "d['end']['streams'][0]['sender'].get('mean_rtt',0)/1000")
    MBPS=$(echo "scale=2; $BPS / 1000000" | bc)

    # Sanity check: if BPS is 0 the JSON parsed but contained no data
    # (can happen when iperf3 exits 0 but the server rejected the window)
    if [[ "$BPS" == "0" ]]; then
        warn "  iperf3 returned 0 bps for window=${WIN_KB}KB — skipping"
        echo "${WIN},FAILED,,,," >> "$WINDOW_CSV"
        rm -f "$TMP_JSON" "${TMP_JSON}.err"
        sleep 2
        continue
    fi

    # Detect plateau: gain < 5% from previous
    if [[ "$PREV_MBPS" != "0" ]]; then
        GAIN=$(echo "scale=4; ($MBPS - $PREV_MBPS) / $PREV_MBPS * 100" | bc 2>/dev/null || echo "99")
        ABS_GAIN=${GAIN#-}
        if (( $(echo "$ABS_GAIN < 5" | bc -l) )) && [[ "$PLATEAU_WIN" == "0" ]]; then
            PLATEAU_WIN=$WIN
            ok "  → Plateau detected at window=${WIN_KB}KB (gain=${GAIN}%)"
        fi
    fi

    echo "${WIN},${MBPS},${RETX},${MRTT},0" >> "$WINDOW_CSV"
    PREV_MBPS=$MBPS
    LAST_WIN=$WIN
    rm -f "$TMP_JSON" "${TMP_JSON}.err"
    sleep 2
done

# Require at least one successful measurement before continuing
if [[ "$PREV_MBPS" == "0" ]]; then
    error "All iperf3 window sweep runs failed. Check connectivity to $TARGET:$PORT"
fi

# Use last successful window if no plateau detected
[[ "$PLATEAU_WIN" == "0" ]] && PLATEAU_WIN=${LAST_WIN:-131072}
echo "PLATEAU_WIN=$PLATEAU_WIN" >> "$META"
echo "BANDWIDTH_MBPS=$PREV_MBPS" >> "$META"

# =============================================================================
# PHASE 3 — Parallel streams sweep (at plateau window)
# =============================================================================
section "Phase 3/4 — Parallel streams sweep"
info "Using window=${PLATEAU_WIN} bytes (plateau), sweeping 1..${MAX_STREAMS} streams"

echo "streams,window_bytes,throughput_mbps,retransmits,cpu_sender_pct,cpu_recv_pct" > "$PARALLEL_CSV"

for STREAMS in 1 2 4 8; do
    [[ "$STREAMS" -gt "$MAX_STREAMS" ]] && break
    info "  Testing streams=${STREAMS}..."
    TMP_JSON=$(mktemp)

    if ! run_iperf "$PLATEAU_WIN" "$STREAMS" 0 "$TMP_JSON"; then
        IPERF_ERR=$(cat "${TMP_JSON}.err" 2>/dev/null | head -3)
        warn "  iperf3 failed for streams=${STREAMS} — skipping"
        warn "  reason: ${IPERF_ERR:-unknown error}"
        echo "${STREAMS},${PLATEAU_WIN},FAILED,,," >> "$PARALLEL_CSV"
        rm -f "$TMP_JSON" "${TMP_JSON}.err"
        sleep 2
        continue
    fi

    BPS=$(iperf_extract "$TMP_JSON" "d['end']['sum_sent']['bits_per_second']")
    RETX=$(iperf_extract "$TMP_JSON" "d['end']['sum_sent']['retransmits']")
    CPU_S=$(iperf_extract "$TMP_JSON" \
        "d['end']['cpu_utilization_percent']['host_total']")
    CPU_R=$(iperf_extract "$TMP_JSON" \
        "d['end']['cpu_utilization_percent']['remote_total']")
    MBPS=$(echo "scale=2; $BPS / 1000000" | bc)

    echo "${STREAMS},${PLATEAU_WIN},${MBPS},${RETX},${CPU_S},${CPU_R}" >> "$PARALLEL_CSV"
    ok "  streams=${STREAMS}: ${MBPS} Mbps  retx=${RETX}  cpu_s=${CPU_S}%  cpu_r=${CPU_R}%"
    rm -f "$TMP_JSON" "${TMP_JSON}.err"
    sleep 2
done

# =============================================================================
# PHASE 4 — Nagle comparison (TCP_NODELAY effect)
# =============================================================================
section "Phase 4/4 — Nagle / TCP_NODELAY comparison"
echo "mode,window_bytes,throughput_mbps,retransmits" > "$NODELAY_CSV"

for NODELAY in 0 1; do
    MODE=$([[ "$NODELAY" == "1" ]] && echo "no_delay" || echo "nagle_on")
    info "  Testing ${MODE}..."
    TMP_JSON=$(mktemp)

    if ! run_iperf "$PLATEAU_WIN" 1 "$NODELAY" "$TMP_JSON"; then
        IPERF_ERR=$(cat "${TMP_JSON}.err" 2>/dev/null | head -3)
        warn "  iperf3 failed for ${MODE} — skipping"
        warn "  reason: ${IPERF_ERR:-unknown error}"
        echo "${MODE},${PLATEAU_WIN},FAILED," >> "$NODELAY_CSV"
        rm -f "$TMP_JSON" "${TMP_JSON}.err"
        sleep 2
        continue
    fi

    BPS=$(iperf_extract "$TMP_JSON" "d['end']['sum_sent']['bits_per_second']")
    RETX=$(iperf_extract "$TMP_JSON" "d['end']['sum_sent']['retransmits']")
    MBPS=$(echo "scale=2; $BPS / 1000000" | bc)

    echo "${MODE},${PLATEAU_WIN},${MBPS},${RETX}" >> "$NODELAY_CSV"
    ok "  ${MODE}: ${MBPS} Mbps"
    rm -f "$TMP_JSON" "${TMP_JSON}.err"
    sleep 2
done

# =============================================================================
# DONE
# =============================================================================
section "Measurement complete"
ok "Results written to: $OUTDIR/"
echo ""
echo "  ping.csv             → RTT baseline"
echo "  window_sweep.csv     → throughput vs window size"
echo "  parallel_sweep.csv   → throughput vs stream count"
echo "  nodelay_comparison   → Nagle effect"
echo "  meta.env             → extracted key values"
echo ""
echo -e "${BOLD}Next step:${RESET}"
echo "  ./kafka-tcp-analyze.sh -d $OUTDIR"
