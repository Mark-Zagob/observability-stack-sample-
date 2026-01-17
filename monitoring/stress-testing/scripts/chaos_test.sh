#!/bin/bash
# stress-testing/scripts/chaos_test.sh

echo "💥 Starting Chaos Test"
echo "======================"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cd "$(dirname "$0")/../../"

# Function to check service health
check_health() {
    local service=$1
    local url=$2
    if curl -s "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ $service is UP${NC}"
        return 0
    else
        echo -e "${RED}❌ $service is DOWN${NC}"
        return 1
    fi
}

# Function to wait for recovery
wait_for_recovery() {
    local service=$1
    local url=$2
    local max_wait=60
    local waited=0
    
    echo -e "${YELLOW}⏳ Waiting for $service to recover...${NC}"
    
    while [ $waited -lt $max_wait ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $service recovered after ${waited}s${NC}"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done
    
    echo -e "${RED}❌ $service did not recover within ${max_wait}s${NC}"
    return 1
}

echo ""
echo "📊 Initial Health Check"
echo "-----------------------"
check_health "Producer" "http://localhost:8000/metrics"
check_health "Consumer" "http://localhost:8001/metrics"
check_health "Kafka" "http://localhost:9308/metrics"
check_health "PostgreSQL" "http://localhost:9187/metrics"
check_health "Prometheus" "http://localhost:9090/-/healthy"
check_health "Grafana" "http://localhost:3000/api/health"

echo ""
echo "💥 Test 1: Kill Consumer"
echo "------------------------"
docker stop data-consumer
echo -e "${YELLOW}Consumer stopped. Check Grafana for alerts...${NC}"
echo "Waiting 30 seconds..."
sleep 30

echo ""
echo "🔄 Restarting Consumer..."
docker start data-consumer
wait_for_recovery "Consumer" "http://localhost:8001/metrics"

echo ""
echo "💥 Test 2: Kill Producer"
echo "------------------------"
docker stop data-producer
echo -e "${YELLOW}Producer stopped. Check Grafana for alerts...${NC}"
echo "Waiting 30 seconds..."
sleep 30

echo ""
echo "🔄 Restarting Producer..."
docker start data-producer
wait_for_recovery "Producer" "http://localhost:8000/metrics"

echo ""
echo "💥 Test 3: Kill Kafka (WARNING: This will affect data flow)"
echo "------------------------------------------------------------"
read -p "Do you want to proceed? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker stop kafka
    echo -e "${YELLOW}Kafka stopped. Check Grafana for cascading failures...${NC}"
    echo "Waiting 60 seconds..."
    sleep 60
    
    echo ""
    echo "🔄 Restarting Kafka..."
    docker start kafka
    sleep 10
    wait_for_recovery "Kafka" "http://localhost:9308/metrics"
fi

echo ""
echo "📊 Final Health Check"
echo "---------------------"
check_health "Producer" "http://localhost:8000/metrics"
check_health "Consumer" "http://localhost:8001/metrics"
check_health "Kafka" "http://localhost:9308/metrics"
check_health "PostgreSQL" "http://localhost:9187/metrics"

echo ""
echo "✅ Chaos Test Complete!"
echo "Check Grafana dashboards and Alertmanager for results."
