#!/bin/sh
# Entrypoint: serve both apps with busybox-extras httpd.
# Call as 'httpd' directly — the post-install trigger creates /usr/sbin/httpd.
# Do NOT call as 'busybox httpd' — that form fails on Alpine busybox-extras.

set -e

echo "Starting TCP Throughput Explainer on http://0.0.0.0:3001"
httpd -f -p 3001 -h /srv/tcp &
TCP_PID=$!

echo "Starting Kafka TCP Tuning on http://0.0.0.0:3002"
httpd -f -p 3002 -h /srv/kafka &
KAFKA_PID=$!

echo ""
echo "Both apps running:"
echo "  TCP explainer : http://<host>:3001/"
echo "  Kafka tuning  : http://<host>:3002/"
echo ""

trap 'kill $TCP_PID $KAFKA_PID 2>/dev/null; exit 0' TERM INT

# Poll until either process exits unexpectedly, then bring down the other and exit non-zero
# so the container restarts (when a restart policy is set).
while kill -0 $TCP_PID 2>/dev/null && kill -0 $KAFKA_PID 2>/dev/null; do
    sleep 2
done
echo "httpd process exited unexpectedly — shutting down" >&2
kill $TCP_PID $KAFKA_PID 2>/dev/null
wait
exit 1
