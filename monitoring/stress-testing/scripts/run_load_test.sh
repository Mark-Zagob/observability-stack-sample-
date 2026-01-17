#!/bin/bash
# stress-testing/scripts/run_load_test.sh

echo "🚀 Starting Constant Load Test"
echo "================================"

cd "$(dirname "$0")/.."

docker-compose run --rm load-test constant \
    --rps ${RPS:-50} \
    --duration ${DURATION:-60} \
    --threads ${THREADS:-10} \
    --kafka kafka:29092 \
    --topic ecommerce.orders \
    --metrics-port 8002
