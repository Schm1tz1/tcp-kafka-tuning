#!/usr/bin/env bash
# =============================================================================
# kafka-tcp-analyze.sh
# Reads output from kafka-tcp-measure.sh and produces:
#   - sysctl recommendations
#   - Kafka producer/broker properties
#   - Diagnosis of current bottleneck
#   - Scenario comparison (local / regional / multi-region)
#
# Usage:
#   ./kafka-tcp-analyze.sh -d <results-dir> [-c <parallel-connections>] [-m <mtu>]
# =============================================================================

set -euo pipefail

RESULTS_DIR=""
PARALLEL_CONNS=8      # expected Kafka producer connections per broker
MTU=1500              # current path MTU (use 9000 if jumbo frames enabled)
INFLIGHT=5            # Kafka max.in.flight.requests.per.connection
LATENCY_BUDGET_MS=50  # end-to-end latency SLA in ms

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'
DIM='\033[2m'

info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
ok()      { echo -e "${GREEN}[ OK ]${RESET}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
rec()     { echo -e "${BLUE}[ REC]${RESET}  $*"; }
section() { echo -e "\n${BOLD}══════════════════════════════════════════${RESET}"; \
            echo -e "${BOLD}  $*${RESET}"; \
            echo -e "${BOLD}══════════════════════════════════════════${RESET}"; }
kv()      { printf "  ${BOLD}%-38s${RESET} %s\n" "$1" "$2"; }

usage() {
    echo "Usage: $0 -d <results-dir> [-c <connections>] [-m <mtu>] [-l <latency-budget-ms>]"
    echo "  -d  Results directory from kafka-tcp-measure.sh (required)"
    echo "  -c  Expected parallel Kafka connections per broker (default: 8)"
    echo "  -m  Path MTU in bytes                            (default: 1500)"
    echo "  -l  Latency budget in ms                         (default: 50)"
    exit 1
}

while getopts "d:c:m:l:h" opt; do
    case $opt in
        d) RESULTS_DIR=$OPTARG ;;
        c) PARALLEL_CONNS=$OPTARG ;;
        m) MTU=$OPTARG ;;
        l) LATENCY_BUDGET_MS=$OPTARG ;;
        h|*) usage ;;
    esac
done

[[ -z "$RESULTS_DIR" ]] && usage
[[ ! -f "$RESULTS_DIR/meta.env" ]] && { echo "meta.env not found in $RESULTS_DIR"; exit 1; }

# ── Load measurements safely ──────────────────────────────────────────────────
# Do NOT source meta.env — it executes arbitrary shell code.
# Parse it as plain key=value text instead.
parse_env() {
    local file=$1
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip blank lines and comments
        [[ "$line" =~ ^[[:space:]]*$ ]] && continue
        [[ "$line" =~ ^[[:space:]]*# ]]  && continue
        # Accept only lines of the form VARNAME=value
        # VARNAME: letters, digits, underscore only
        # value: printable ASCII, no shell metacharacters
        if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=([^;&|$\`\(\)\{\}]*)$ ]]; then
            local key="${BASH_REMATCH[1]}"
            local val="${BASH_REMATCH[2]}"
            printf '%s=%s\n' "$key" "$val"
        else
            warn "Skipping unsafe line in $file: $line"
        fi
    done < "$file"
}

# Load meta.env into variables — only safe KEY=VALUE lines
while IFS='=' read -r key val; do
    case "$key" in
        TARGET)         TARGET="$val"         ;;
        RTT_MIN)        RTT_MIN="$val"         ;;
        RTT_AVG)        RTT_AVG="$val"         ;;
        RTT_MDEV)       RTT_MDEV="$val"        ;;
        PKT_LOSS)       PKT_LOSS="$val"        ;;
        PLATEAU_WIN)    PLATEAU_WIN="$val"     ;;
        BANDWIDTH_MBPS) BANDWIDTH_MBPS="$val"  ;;
        # Ignore any other keys — do not blindly export unknown variables
    esac
done < <(parse_env "$RESULTS_DIR/meta.env")

RTT_MIN=${RTT_MIN:-1}
RTT_AVG=${RTT_AVG:-2}
RTT_MDEV=${RTT_MDEV:-0.5}
PKT_LOSS=${PKT_LOSS:-0}
PLATEAU_WIN=${PLATEAU_WIN:-131072}
BANDWIDTH_MBPS=${BANDWIDTH_MBPS:-1000}

# ── Validate all inputs are numeric before passing to python3 ─────────────────
is_numeric() {
    [[ "$1" =~ ^[0-9]+(\.[0-9]+)?$ ]] || \
        error "Expected numeric value for $2, got: '$1'"
}
is_integer() {
    [[ "$1" =~ ^[0-9]+$ ]] || \
        error "Expected integer value for $2, got: '$1'"
}

is_numeric  "$RTT_MIN"       "RTT_MIN"
is_numeric  "$RTT_AVG"       "RTT_AVG"
is_numeric  "$RTT_MDEV"      "RTT_MDEV"
is_numeric  "$PKT_LOSS"      "PKT_LOSS"
is_integer  "$PLATEAU_WIN"   "PLATEAU_WIN"
is_numeric  "$BANDWIDTH_MBPS" "BANDWIDTH_MBPS"
is_integer  "$PARALLEL_CONNS" "PARALLEL_CONNS"
is_integer  "$MTU"           "MTU"
is_integer  "$INFLIGHT"      "INFLIGHT"
is_numeric  "$LATENCY_BUDGET_MS" "LATENCY_BUDGET_MS"

# ── Derived calculations ──────────────────────────────────────────────────────
# Values are passed as argv — NOT interpolated into the heredoc.
# The heredoc delimiter is quoted ('PYEOF') so the shell does zero expansion
# inside it. This prevents injection even if a value somehow bypassed validation.
python3 - \
    "$RTT_MIN" "$RTT_AVG" "$RTT_MDEV" "$PKT_LOSS" \
    "$PLATEAU_WIN" "$BANDWIDTH_MBPS" "$PARALLEL_CONNS" \
    "$MTU" "$INFLIGHT" "$LATENCY_BUDGET_MS" \
    "$RESULTS_DIR" \
    << 'PYEOF'
import math, sys

rtt_min, rtt_avg, rtt_mdev, pkt_loss = \
    float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
plateau   = int(sys.argv[5])
bw_mbps   = float(sys.argv[6])
conns     = int(sys.argv[7])
mtu       = int(sys.argv[8])
inflight  = int(sys.argv[9])
lat_ms    = float(sys.argv[10])
results_dir = sys.argv[11]

bw_bytes  = bw_mbps * 1e6 / 8
rtt_s_min = rtt_min / 1000
rtt_s_avg = rtt_avg / 1000

# ── Core derived values ────────────────────────────────────────────────────
theoretical_bdp = int(bw_bytes * rtt_s_min)
empirical_bdp   = plateau  # window size at throughput plateau
mss             = mtu - 40  # IP(20) + TCP(20)

# TCP buffer ceiling: empirical_bdp × connections × 2 (headroom)
buf_ceiling     = empirical_bdp * conns * 2
buf_ceiling_mb  = buf_ceiling / 1048576

# Round buf_ceiling up to power of 2 for clean sysctl
def next_pow2_bytes(n):
    p = 1
    while p < n:
        p <<= 1
    return p

buf_pow2 = next_pow2_bytes(buf_ceiling)

# Kafka settings
batch_min       = empirical_bdp // inflight
# Round to nice power-of-2 KB
batch_targets   = [16384, 32768, 65536, 131072, 262144, 524288, 1048576]
batch_size      = next(b for b in batch_targets if b >= batch_min) \
                  if batch_min <= batch_targets[-1] else batch_targets[-1]

# linger.ms: time to drain one BDP at measured throughput
bdp_drain_ms    = (empirical_bdp * 8) / (bw_mbps * 1e6) * 1000
linger_thru     = max(1, int(bdp_drain_ms))
linger_latency  = max(0, int(lat_ms - rtt_avg - 2))  # 2ms broker overhead estimate

# Mathis bound
mathis_mbps     = 0
if pkt_loss > 0:
    p = pkt_loss / 100
    mathis_mbps = ((mss / rtt_s_avg) * (1 / math.sqrt(p))) / 1e6

# Jitter flag
high_jitter = rtt_mdev > (rtt_avg * 0.3)

# ── Scenario presets ────────────────────────────────────────────────────────
scenarios = {
    "local_dc": {
        "rtt_ms": 0.2, "bw_gbps": 10,
        "batch": 131072, "linger": 5, "buf": "33554432",
        "cc": "bbr", "conns": 8
    },
    "same_region": {
        "rtt_ms": 5, "bw_gbps": 1,
        "batch": 131072, "linger": 10, "buf": "134217728",
        "cc": "bbr", "conns": 4
    },
    "cross_region": {
        "rtt_ms": 60, "bw_gbps": 1,
        "batch": 524288, "linger": 50, "buf": "536870912",
        "cc": "bbr", "conns": 2
    },
    "multi_region": {
        "rtt_ms": 150, "bw_gbps": 0.5,
        "batch": 1048576, "linger": 100, "buf": "1073741824",
        "cc": "bbr", "conns": 2
    },
    "satellite": {
        "rtt_ms": 600, "bw_gbps": 0.05,
        "batch": 1048576, "linger": 500, "buf": "2147483647",
        "cc": "bbr", "conns": 1
    },
}

# ── Bottleneck diagnosis ─────────────────────────────────────────────────────
diagnosis = []
if pkt_loss > 0.1:
    mathis_str = f"{mathis_mbps:.1f}" if mathis_mbps > 0 else "N/A"
    diagnosis.append(f"PACKET LOSS {pkt_loss}% — Mathis bound: {mathis_str} Mbps. Fix network before tuning buffers.")
if empirical_bdp < theoretical_bdp * 0.5:
    diagnosis.append(f"WINDOW-LIMITED: empirical BDP ({empirical_bdp//1024}KB) << theoretical ({theoretical_bdp//1024}KB). Likely CPU or NIC ring buffer limit.")
if high_jitter:
    diagnosis.append(f"HIGH JITTER: mdev={rtt_mdev}ms > 30% of avg RTT. Switch buffer bloat likely. Consider FQ qdisc.")
if pkt_loss == 0 and not high_jitter and empirical_bdp >= theoretical_bdp * 0.8:
    diagnosis.append("PATH HEALTHY: window is the primary lever. Buffer tuning will help directly.")

# ── Write output env for shell to read back ──────────────────────────────────
with open(f"{results_dir}/analysis.env", "w") as f:
    f.write(f"THEORETICAL_BDP={theoretical_bdp}\n")
    f.write(f"EMPIRICAL_BDP={empirical_bdp}\n")
    f.write(f"MSS={mss}\n")
    f.write(f"BUF_CEILING={buf_pow2}\n")
    f.write(f"BUF_CEILING_MB={buf_ceiling_mb:.1f}\n")
    f.write(f"BATCH_SIZE={batch_size}\n")
    f.write(f"BATCH_MIN={batch_min}\n")
    f.write(f"LINGER_THRU={linger_thru}\n")
    f.write(f"LINGER_LATENCY={linger_latency}\n")
    f.write(f"MATHIS_MBPS={mathis_mbps:.1f}\n")
    f.write(f"HIGH_JITTER={'1' if high_jitter else '0'}\n")
    f.write(f"DIAGNOSIS={'|'.join(diagnosis)}\n")
    f.write(f"BUF_POW2={buf_pow2}\n")
PYEOF

# Load analysis.env safely — same parser, explicit allowlist of expected keys
while IFS='=' read -r key val; do
    case "$key" in
        THEORETICAL_BDP) THEORETICAL_BDP="$val" ;;
        EMPIRICAL_BDP)   EMPIRICAL_BDP="$val"   ;;
        MSS)             MSS="$val"             ;;
        BUF_CEILING)     BUF_CEILING="$val"     ;;
        BUF_CEILING_MB)  BUF_CEILING_MB="$val"  ;;
        BATCH_SIZE)      BATCH_SIZE="$val"      ;;
        BATCH_MIN)       BATCH_MIN="$val"       ;;
        LINGER_THRU)     LINGER_THRU="$val"     ;;
        LINGER_LATENCY)  LINGER_LATENCY="$val"  ;;
        MATHIS_MBPS)     MATHIS_MBPS="$val"     ;;
        HIGH_JITTER)     HIGH_JITTER="$val"     ;;
        DIAGNOSIS)       DIAGNOSIS="$val"       ;;
        BUF_POW2)        BUF_POW2="$val"        ;;
    esac
done < <(parse_env "$RESULTS_DIR/analysis.env")

# ── Sanitise TARGET for safe interpolation into config file content ───────────
# Allow only hostname/IP characters: alphanumeric, dots, hyphens, colons
TARGET_SAFE=$(printf '%s' "${TARGET:-unknown}" | tr -cd 'A-Za-z0-9.:-')
[ -z "$TARGET_SAFE" ] && TARGET_SAFE="unknown"

# =============================================================================
# OUTPUT REPORT
# =============================================================================
REPORT="$RESULTS_DIR/recommendations.txt"
exec > >(tee "$REPORT") 2>&1

section "Measurement Summary"
kv "Target:"                   "$TARGET"
kv "Measured bandwidth:"       "${BANDWIDTH_MBPS} Mbps"
kv "RTT min / avg / mdev:"     "${RTT_MIN} / ${RTT_AVG} / ${RTT_MDEV} ms"
kv "Packet loss:"              "${PKT_LOSS}%"
kv "MTU (specified):"          "${MTU} bytes"
kv "MSS (derived):"            "${MSS} bytes  (MTU − 40)"
echo ""
kv "Theoretical BDP:"          "$(echo "scale=1; $THEORETICAL_BDP/1024" | bc) KB  (bandwidth × RTT_min)"
kv "Empirical BDP (plateau):"  "$(echo "scale=1; $EMPIRICAL_BDP/1024" | bc) KB  (window sweep result)"
kv "Buffer ceiling needed:"    "${BUF_CEILING_MB} MB  (BDP × ${PARALLEL_CONNS} conns × 2)"

# ── Diagnosis ─────────────────────────────────────────────────────────────────
section "Bottleneck Diagnosis"
IFS='|' read -ra DIAG_ITEMS <<< "$DIAGNOSIS"
for item in "${DIAG_ITEMS[@]}"; do
    [[ -z "$item" ]] && continue
    # Use case pattern matching — no grep needed, works on any shell
    case "$item" in
        *LOSS*|*LIMIT*)  warn "$item" ;;
        *JITTER*)        warn "$item" ;;
        *)               ok   "$item" ;;
    esac
done

if [[ "${HIGH_JITTER}" == "1" ]]; then
    warn "High jitter detected — verify switch QoS and enable FQ qdisc before tuning buffers"
fi

# ── Nagle comparison ──────────────────────────────────────────────────────────
if [[ -f "$RESULTS_DIR/nodelay_comparison.csv" ]]; then
    section "Nagle / TCP_NODELAY Effect"
    NAGLE_ON=$(awk  -F',' '/nagle_on/  {print $3}' "$RESULTS_DIR/nodelay_comparison.csv")
    NAGLE_OFF=$(awk -F',' '/no_delay/  {print $3}' "$RESULTS_DIR/nodelay_comparison.csv")
    if [[ -n "$NAGLE_ON" && -n "$NAGLE_OFF" ]]; then
        NAGLE_DIFF=$(echo "scale=1; $NAGLE_OFF - $NAGLE_ON" | bc)
        kv "With Nagle (default):"    "${NAGLE_ON} Mbps"
        kv "With TCP_NODELAY:"        "${NAGLE_OFF} Mbps"
        kv "Difference:"              "${NAGLE_DIFF} Mbps"
        if (( $(echo "${NAGLE_DIFF#-} > 10" | bc -l) )); then
            warn "Nagle is adding measurable latency — ensure TCP_NODELAY is set in Kafka"
        else
            ok "Nagle effect negligible at this window size (Kafka batching dominates)"
        fi
    fi
fi

# =============================================================================
# SYSCTL RECOMMENDATIONS
# =============================================================================
section "Linux TCP Tuning — sysctl"
echo ""
echo -e "${DIM}# --- Apply with: sysctl -p /etc/sysctl.d/99-kafka-tcp.conf ---${RESET}"
echo ""

SYSCTL_FILE="$RESULTS_DIR/99-kafka-tcp.conf"
cat > "$SYSCTL_FILE" << SYSCTL
# kafka-tcp-measure.sh generated — $(date)
# Target: $TARGET_SAFE  BDP: ${EMPIRICAL_BDP} bytes  Connections: ${PARALLEL_CONNS}

# Socket buffer ceilings — sized to BDP × connections × 2 headroom
net.core.rmem_max            = $BUF_POW2
net.core.wmem_max            = $BUF_POW2
net.ipv4.tcp_rmem            = 4096 1048576 $BUF_POW2
net.ipv4.tcp_wmem            = 4096 1048576 $BUF_POW2

# Keep kernel autotuning active (do not set fixed buffer sizes)
net.ipv4.tcp_moderate_rcvbuf = 1

# Congestion control — BBR recommended for Kafka on any RTT
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc          = fq

# Reduce SYN retries for faster failure detection on broker restarts
net.ipv4.tcp_syn_retries        = 4
net.ipv4.tcp_synack_retries     = 3

# Keep-alive — detect dead broker connections within ~30s
net.ipv4.tcp_keepalive_time     = 30
net.ipv4.tcp_keepalive_intvl    = 5
net.ipv4.tcp_keepalive_probes   = 3
SYSCTL

cat "$SYSCTL_FILE"
echo ""
ok "Written to: $SYSCTL_FILE"

# =============================================================================
# KAFKA PRODUCER RECOMMENDATIONS
# =============================================================================
section "Kafka Producer Properties"
echo ""

PROD_THRU_FILE="$RESULTS_DIR/producer-throughput.properties"
PROD_LAT_FILE="$RESULTS_DIR/producer-latency.properties"

# Throughput-optimised
cat > "$PROD_THRU_FILE" << KAFKA_T
# kafka-tcp-measure.sh generated — $(date)
# Profile: THROUGHPUT-OPTIMISED
# Path: $TARGET_SAFE  BDP: ${EMPIRICAL_BDP}B  Measured BW: ${BANDWIDTH_MBPS}Mbps  RTT: ${RTT_AVG}ms

# Batching — sized to fill TCP window across all in-flight requests
batch.size                              = $BATCH_SIZE
linger.ms                               = $LINGER_THRU
buffer.memory                           = $(echo "$BUF_POW2 * 2" | bc)

# Compression — lz4 reduces bytes-on-wire, compresses whole batch
compression.type                        = lz4

# Pipeline depth — match to measured parallel stream sweet spot
max.in.flight.requests.per.connection   = $INFLIGHT

# Reliability
acks                                    = all
enable.idempotence                      = true

# Socket buffers — hint to OS (kernel autotuning takes over above this)
send.buffer.bytes                       = $(echo "$BATCH_SIZE * 2" | bc)
receive.buffer.bytes                    = 65536

# Retry
retries                                 = 10
retry.backoff.ms                        = 100
delivery.timeout.ms                     = 120000
KAFKA_T

# Latency-optimised
cat > "$PROD_LAT_FILE" << KAFKA_L
# kafka-tcp-measure.sh generated — $(date)
# Profile: LATENCY-OPTIMISED  (SLA: ${LATENCY_BUDGET_MS}ms)
# Path: $TARGET_SAFE  RTT: ${RTT_AVG}ms  Budget remaining after RTT+broker: ${LINGER_LATENCY}ms

# Batching — small batches, minimal wait
batch.size                              = 16384
linger.ms                               = $LINGER_LATENCY

# Compression — lz4 still worth it (sub-ms overhead)
compression.type                        = lz4

# Single in-flight for strict ordering; increase to 5 if ordering not required
max.in.flight.requests.per.connection   = 1

# Reliability
acks                                    = 1
enable.idempotence                      = false

# Fast failure detection
request.timeout.ms                      = 5000
delivery.timeout.ms                     = 10000
retries                                 = 3
retry.backoff.ms                        = 50
KAFKA_L

echo -e "${BOLD}Throughput profile (batch.size=${BATCH_SIZE}, linger=${LINGER_THRU}ms):${RESET}"
cat "$PROD_THRU_FILE"
echo ""
echo -e "${BOLD}Latency profile (batch.size=16384, linger=${LINGER_LATENCY}ms):${RESET}"
cat "$PROD_LAT_FILE"
ok "Written to: $PROD_THRU_FILE"
ok "Written to: $PROD_LAT_FILE"

# =============================================================================
# KAFKA BROKER RECOMMENDATIONS
# =============================================================================
section "Kafka Broker Properties (server.properties additions)"
BROKER_FILE="$RESULTS_DIR/broker-additions.properties"
cat > "$BROKER_FILE" << BROKER
# kafka-tcp-measure.sh generated — $(date)
# Add/override in server.properties

# Socket — sized to BDP × peak concurrent producers
socket.send.buffer.bytes        = $BUF_POW2
socket.receive.buffer.bytes     = $BUF_POW2
socket.request.max.bytes        = 104857600

# Network threads — one per CPU core is a common starting point
# Increase if CPU recv% was high in parallel sweep
num.network.threads             = 8
num.io.threads                  = 8

# Log flush — let the OS decide (fsync is expensive, replication is your durability)
log.flush.interval.messages     = 9223372036854775807
log.flush.interval.ms           = 9223372036854775807

# Replica fetch — sized for inter-broker replication BDP
replica.fetch.max.bytes         = $BATCH_SIZE
replica.socket.receive.buffer.bytes = $BUF_POW2
BROKER

cat "$BROKER_FILE"
ok "Written to: $BROKER_FILE"

# =============================================================================
# SCENARIO COMPARISON TABLE
# =============================================================================
section "Scenario Reference — Defaults vs Recommended"
echo ""
printf "${BOLD}%-18s %-10s %-8s %-10s %-10s %-12s %-10s %-8s${RESET}\n" \
    "Scenario" "BW" "RTT" "BDP" "batch.size" "tcp_rmem_max" "linger.ms" "CC"
echo "────────────────────────────────────────────────────────────────────────────────"

print_scenario() {
    local name=$1 bw=$2 rtt=$3 bdp=$4 batch=$5 buf=$6 linger=$7 cc=$8
    printf "%-18s %-10s %-8s %-10s %-10s %-12s %-10s %-8s\n" \
        "$name" "$bw" "${rtt}ms" "$bdp" "$batch" "$buf" "${linger}ms" "$cc"
}

print_scenario "Kafka default"    "-"       "-"    "-"      "16KB"    "256KB"     "0"    "cubic"
print_scenario "Local DC (<1ms)"  "10 Gbps" "0.2"  "~250KB" "128KB"   "32MB"      "5"    "bbr"
print_scenario "Same-AZ (1-5ms)"  "1 Gbps"  "5"    "~625KB" "128KB"   "128MB"     "10"   "bbr"
print_scenario "Cross-AZ (5-20ms)" "1 Gbps" "20"   "~2.5MB" "256KB"   "256MB"     "20"   "bbr"
print_scenario "Cross-region(50ms)" "500Mbps" "60"  "~3.75MB" "512KB"  "512MB"     "50"   "bbr"
print_scenario "Multi-region(150ms)" "200Mbps" "150" "~3.75MB" "1MB"   "1GB"       "100"  "bbr"
print_scenario "Satellite(600ms)" "50 Mbps" "600"  "~3.75MB" "1MB"    "2GB"       "500"  "bbr"
echo "────────────────────────────────────────────────────────────────────────────────"
printf "  ${BOLD}%-17s${RESET} %-10s %-8s %-10s %-10s %-12s %-10s %-8s\n" \
    ">>> YOUR PATH <<<" "${BANDWIDTH_MBPS}Mbps" "${RTT_AVG}ms" \
    "$(echo "scale=0; $EMPIRICAL_BDP/1024" | bc)KB" \
    "$(echo "scale=0; $BATCH_SIZE/1024" | bc)KB" \
    "$(echo "scale=0; $BUF_POW2/1048576" | bc)MB" \
    "$LINGER_THRU" "bbr"
echo ""

# =============================================================================
# SUMMARY
# =============================================================================
section "Files Generated"
echo ""
echo "  $SYSCTL_FILE"
echo "    → apply: sudo sysctl -p $SYSCTL_FILE"
echo ""
echo "  $PROD_THRU_FILE"
echo "    → copy relevant lines into producer.properties"
echo ""
echo "  $PROD_LAT_FILE"
echo "    → copy relevant lines into producer.properties (latency profile)"
echo ""
echo "  $BROKER_FILE"
echo "    → add relevant lines to server.properties, restart broker"
echo ""
ok "Full report saved to: $REPORT"
