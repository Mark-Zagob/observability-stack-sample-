#!/bin/bash
# stress-testing/scripts/spike_test.sh

echo "⚡ Starting Spike Test"
echo "======================"

cd "$(dirname "$0")/.."

docker-compose run --rm load-test spike \
    --base-rps ${BASE_RPS:-20} \
    --spike-rps ${SPIKE_RPS:-200} \
    --spike-duration ${SPIKE_DURATION:-30} \
    --total-duration ${TOTAL_DURATION:-120} \
    --threads ${THREADS:-20} \
    --kafka kafka:29092 \
    --topic ecommerce.orders \
    --metrics-port 8002
