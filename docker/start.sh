#!/bin/bash
# Startup script for Airflow with Temporal Orchestrator
#
# This script builds the Docker image and starts all services.
#
# Usage:
#   ./start.sh           # Build and start all services
#   ./start.sh --no-build # Start without rebuilding
#   ./start.sh --down    # Stop all services
#   ./start.sh --logs    # Follow logs
#   ./start.sh --rebuild # Force rebuild and start

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse arguments
BUILD=true
DOWN=false
LOGS=false
REBUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-build)
            BUILD=false
            shift
            ;;
        --down)
            DOWN=true
            shift
            ;;
        --logs)
            LOGS=true
            shift
            ;;
        --rebuild)
            REBUILD=true
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Usage: $0 [--no-build|--down|--logs|--rebuild]"
            exit 1
            ;;
    esac
done

# Handle --down
if [ "$DOWN" = true ]; then
    log_info "Stopping all services..."
    docker-compose down
    log_info "Services stopped."
    exit 0
fi

# Handle --logs
if [ "$LOGS" = true ]; then
    docker-compose logs -f
    exit 0
fi

# Build the image
if [ "$BUILD" = true ]; then
    log_info "Building Airflow with Temporal image..."

    BUILD_ARGS=""
    if [ "$REBUILD" = true ]; then
        BUILD_ARGS="--no-cache"
    fi

    docker build $BUILD_ARGS -t airflow-temporal:latest -f Dockerfile "$REPO_ROOT"

    if [ $? -eq 0 ]; then
        log_info "Image built successfully."
    else
        log_error "Failed to build image."
        exit 1
    fi
fi

# Set up directories
log_info "Setting up directories..."
mkdir -p logs dags

# Set AIRFLOW_UID for Linux
if [[ "$(uname)" == "Linux" ]]; then
    export AIRFLOW_UID=$(id -u)
    log_info "Set AIRFLOW_UID=$AIRFLOW_UID"
fi

# Start services
log_info "Starting services..."
docker-compose up -d

log_info "Waiting for services to be healthy..."
sleep 5

# Check service health
check_service() {
    local service=$1
    local status=$(docker-compose ps --format json "$service" 2>/dev/null | grep -o '"Health":"[^"]*"' | cut -d'"' -f4)
    if [ -z "$status" ]; then
        status=$(docker-compose ps "$service" 2>/dev/null | grep -o "healthy\|unhealthy\|starting" | head -1)
    fi
    echo "$status"
}

# Wait for Airflow to be ready (max 2 minutes)
log_info "Waiting for Airflow to be ready..."
for i in {1..24}; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v2/monitor/health 2>/dev/null | grep -q "200"; then
        log_info "Airflow is ready!"
        break
    fi
    if [ $i -eq 24 ]; then
        log_warn "Airflow may not be fully ready yet. Check logs with: docker-compose logs -f airflow-apiserver"
    fi
    sleep 5
done

echo ""
log_info "Services started!"
echo ""
echo "Access points:"
echo "  - Airflow UI:  http://localhost:8080"
echo "  - Temporal UI: http://localhost:8233"
echo ""
echo "Useful commands:"
echo "  - View logs:     docker-compose logs -f"
echo "  - Stop services: ./start.sh --down"
echo "  - Restart:       docker-compose restart"
echo ""
echo "To trigger the example DAG:"
echo "  1. Open Airflow UI at http://localhost:8080"
echo "  2. Enable the 'example_temporal_dag'"
echo "  3. Click 'Trigger DAG'"
echo "  4. Watch execution in Temporal UI at http://localhost:8233"
