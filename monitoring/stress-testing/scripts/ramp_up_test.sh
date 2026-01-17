#!/bin/bash
# stress-testing/scripts/ramp_up_test.sh

echo "📈 Starting Ramp-Up Load Test"
echo "=============================="

cd "$(dirname "$0")/.."

docker-compose run --rm load-test rampup \
    --start-rps ${START_RPS:-10} \
    --end-rps ${END_RPS:-100} \
    --duration ${DURATION:-120} \
    --threads ${THREADS:-20} \
    --kafka kafka:29092 \
    --topic ecommerce.orders \
    --metrics-port 8002
