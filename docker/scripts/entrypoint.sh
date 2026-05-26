#!/bin/sh
# Entrypoint: start both apps using busybox httpd
# tcp-throughput-explainer → port 3001
# kafka-tcp-tuning         → port 3002
#
# busybox httpd serves a static directory, no config needed for basic use.
# -f  foreground (we background it with & except the last one)
# -p  port
# -h  document root

set -e

echo "Starting TCP Throughput Explainer on http://0.0.0.0:3001"
busybox httpd -f -p 3001 -h /srv/tcp &
TCP_PID=$!

echo "Starting Kafka TCP Tuning on http://0.0.0.0:3002"
busybox httpd -f -p 3002 -h /srv/kafka &
KAFKA_PID=$!

echo ""
echo "Both apps running:"
echo "  TCP explainer : http://<host>:3001/"
echo "  Kafka tuning  : http://<host>:3002/"
echo ""

# Wait for either process to exit and propagate signals cleanly
trap 'kill $TCP_PID $KAFKA_PID 2>/dev/null; exit 0' TERM INT

wait $TCP_PID $KAFKA_PID
