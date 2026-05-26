# =============================================================================
# Dockerfile — TCP Kafka Tuning visualisation tools
#
# Build stage:  node:22-alpine — Vite bundles both React apps to static files
# Runtime stage: alpine        — busybox-extras httpd serves the static output
#
# busybox-extras ships the httpd applet as /usr/sbin/httpd.
# Invoke as 'httpd', NOT 'busybox httpd' — the latter fails on Alpine.
#
# Ports:
#   3001 — TCP Throughput Explainer
#   3002 — Kafka TCP Tuning
#
# Build:
#   docker build -t tcp-kafka-viz .        (from docker/ directory)
#   podman build -t tcp-kafka-viz .
#   buildah bud   -t tcp-kafka-viz .
#
# Run:
#   docker run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
#   podman run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
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
# busybox-extras ships the httpd applet and installs as /usr/sbin/httpd.
# Call it as 'httpd' directly — NOT 'busybox httpd' (that form fails on Alpine).
FROM alpine:3.20

RUN apk add --no-cache busybox-extras \
 && mkdir -p /srv/tcp /srv/kafka

COPY --from=build /build/tcp/dist/   /srv/tcp/
COPY --from=build /build/kafka/dist/ /srv/kafka/
COPY scripts/entrypoint.sh           /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 3001 3002

ENTRYPOINT ["/entrypoint.sh"]
