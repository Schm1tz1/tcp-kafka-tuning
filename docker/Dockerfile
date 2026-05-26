# =============================================================================
# Dockerfile — TCP / Kafka throughput visualisation tools
#
# Build stage:  node:alpine  — Vite bundles both React apps to static files
# Runtime stage: alpine      — busybox httpd serves the static output
#
# Result: ~15 MB image, no Node/npm at runtime, no nginx/caddy/traefik.
#
# Ports:
#   3001 — TCP Throughput Explainer
#   3002 — Kafka TCP Tuning
#
# Build:
#   docker build -t tcp-kafka-viz .
#
# Run:
#   docker run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
# =============================================================================

# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM node:22-alpine AS build

WORKDIR /build

# Build TCP app
COPY tcp/package.json         tcp/
COPY tcp/vite.config.js       tcp/
COPY tcp/index.html           tcp/
COPY tcp/src/                 tcp/src/

RUN cd tcp \
 && npm install --prefer-offline --no-audit --no-fund \
 && npm run build

# Build Kafka app
COPY kafka/package.json       kafka/
COPY kafka/vite.config.js     kafka/
COPY kafka/index.html         kafka/
COPY kafka/src/               kafka/src/

RUN cd kafka \
 && npm install --prefer-offline --no-audit --no-fund \
 && npm run build

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM alpine:3.20

# busybox is already part of alpine — no extra install needed.
# Create document roots and copy built assets.
RUN mkdir -p /srv/tcp /srv/kafka

COPY --from=build /build/tcp/dist/   /srv/tcp/
COPY --from=build /build/kafka/dist/ /srv/kafka/
COPY scripts/entrypoint.sh           /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 3001 3002

ENTRYPOINT ["/entrypoint.sh"]
