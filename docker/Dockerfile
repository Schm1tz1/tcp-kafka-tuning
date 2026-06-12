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
#   3003 — Guided Slideshow
#
# Build (from repository root):
#   docker build -f docker/Dockerfile -t tcp-kafka-viz .
#   podman build -f docker/Dockerfile -t tcp-kafka-viz .
#   buildah bud  -f docker/Dockerfile -t tcp-kafka-viz .
#
# Run:
#   docker run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
#   podman run -p 3001:3001 -p 3002:3002 tcp-kafka-viz
# =============================================================================

# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM node:22-alpine AS build

WORKDIR /build

# Copy dashboard source files (single source of truth, imported by main.jsx)
# Build context is repository root, so dashboards/ is at root level
COPY dashboards/              dashboards/

# Build TCP app
COPY docker/tcp/package.json         tcp/
COPY docker/tcp/vite.config.js       tcp/
COPY docker/tcp/index.html           tcp/
COPY docker/tcp/src/                 tcp/src/

RUN cd tcp \
 && npm install --prefer-offline --no-audit --no-fund \
 && npm run build

# Build Kafka app
COPY docker/kafka/package.json       kafka/
COPY docker/kafka/vite.config.js     kafka/
COPY docker/kafka/index.html         kafka/
COPY docker/kafka/src/               kafka/src/

RUN cd kafka \
 && npm install --prefer-offline --no-audit --no-fund \
 && npm run build

# Build Slideshow app
COPY docker/slides/package.json      slides/
COPY docker/slides/vite.config.js    slides/
COPY docker/slides/index.html        slides/
COPY docker/slides/src/              slides/src/

RUN cd slides \
 && npm install --prefer-offline --no-audit --no-fund \
 && npm run build

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# busybox-extras ships the httpd applet and installs as /usr/sbin/httpd.
# Call it as 'httpd' directly — NOT 'busybox httpd' (that form fails on Alpine).
FROM alpine:3.20

RUN apk add --no-cache busybox-extras \
 && mkdir -p /srv/tcp /srv/kafka /srv/slides

COPY --from=build /build/tcp/dist/        /srv/tcp/
COPY --from=build /build/kafka/dist/      /srv/kafka/
COPY --from=build /build/slides/dist/     /srv/slides/
COPY docker/scripts/entrypoint.sh         /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 3001 3002 3003

ENTRYPOINT ["/entrypoint.sh"]
