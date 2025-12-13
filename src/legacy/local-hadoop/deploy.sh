#!/bin/bash

# Boston Transport Department - Deployment Script
# Deploys time-manager and streamers to Kubernetes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="boston-transport"
IMAGE_PREFIX="boston-transport"
IMAGE_TAG="${IMAGE_TAG:-latest}"

echo -e "${GREEN}Boston Transport Department - Deployment Script${NC}"
echo "================================================"
echo ""

# Function to print step
step() {
    echo -e "${GREEN}[STEP]${NC} $1"
}

# Function to print warning
warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Function to print error
error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Check prerequisites
check_prerequisites() {
    step "Checking prerequisites..."

    if ! command -v kubectl &> /dev/null; then
        error "kubectl is not installed"
    fi

    if ! command -v docker &> /dev/null; then
        error "docker is not installed"
    fi

    echo "✓ All prerequisites met"
    echo ""
}

# Build Docker images
build_images() {
    step "Building Docker images..."

    # Build time-manager
    echo "Building time-manager..."
    docker build -t ${IMAGE_PREFIX}/time-manager:${IMAGE_TAG} time-manager/

    # Build streamers
    echo "Building bike-streamer..."
    docker build -f streamers/Dockerfile.bike -t ${IMAGE_PREFIX}/bike-streamer:${IMAGE_TAG} streamers/

    echo "Building taxi-streamer..."
    docker build -f streamers/Dockerfile.taxi -t ${IMAGE_PREFIX}/taxi-streamer:${IMAGE_TAG} streamers/

    echo "Building weather-streamer..."
    docker build -f streamers/Dockerfile.weather -t ${IMAGE_PREFIX}/weather-streamer:${IMAGE_TAG} streamers/

    echo "✓ All images built successfully"
    echo ""
}

# Load images to kind (if using kind)
load_images_kind() {
    if command -v kind &> /dev/null && kind get clusters &> /dev/null; then
        step "Detected kind cluster, loading images..."

        CLUSTER_NAME=$(kind get clusters | head -n 1)

        kind load docker-image ${IMAGE_PREFIX}/time-manager:${IMAGE_TAG} --name ${CLUSTER_NAME}
        kind load docker-image ${IMAGE_PREFIX}/bike-streamer:${IMAGE_TAG} --name ${CLUSTER_NAME}
        kind load docker-image ${IMAGE_PREFIX}/taxi-streamer:${IMAGE_TAG} --name ${CLUSTER_NAME}
        kind load docker-image ${IMAGE_PREFIX}/weather-streamer:${IMAGE_TAG} --name ${CLUSTER_NAME}

        echo "✓ Images loaded to kind cluster"
        echo ""
    fi
}

# Deploy infrastructure
deploy_infrastructure() {
    step "Deploying infrastructure components..."

    # Create namespace
    kubectl apply -f k8s/namespace.yaml

    # Deploy Zookeeper
    echo "Deploying Zookeeper..."
    kubectl apply -f k8s/zookeeper.yaml

    # Deploy Kafka
    echo "Deploying Kafka..."
    kubectl apply -f k8s/kafka.yaml

    # Deploy Redis
    echo "Deploying Redis..."
    kubectl apply -f k8s/redis.yaml

    echo "✓ Infrastructure components deployed"
    echo ""
}

# Wait for infrastructure
wait_infrastructure() {
    step "Waiting for infrastructure to be ready..."

    echo "Waiting for Redis..."
    kubectl wait --for=condition=ready pod -l app=redis -n ${NAMESPACE} --timeout=300s

    echo "Waiting for Zookeeper..."
    kubectl wait --for=condition=ready pod -l app=zookeeper -n ${NAMESPACE} --timeout=300s

    echo "Waiting for Kafka..."
    kubectl wait --for=condition=ready pod -l app=kafka -n ${NAMESPACE} --timeout=300s

    echo "✓ Infrastructure ready"
    echo ""
}

# Deploy time-manager
deploy_time_manager() {
    step "Deploying time-manager..."

    kubectl apply -f k8s/time-manager.yaml

    echo "Waiting for time-manager to be ready..."
    kubectl wait --for=condition=ready pod -l app=time-manager -n ${NAMESPACE} --timeout=300s

    echo "✓ Time-manager deployed"
    echo ""
}

# Deploy streamers
deploy_streamers() {
    step "Deploying streamers..."

    # Deploy mock data
    kubectl apply -f k8s/data-configmap.yaml

    # Deploy streamers
    kubectl apply -f k8s/bike-streamer.yaml
    kubectl apply -f k8s/taxi-streamer.yaml
    kubectl apply -f k8s/weather-streamer.yaml

    # Wait a bit for streamers to start
    sleep 10

    echo "✓ Streamers deployed"
    echo ""
}

# Show status
show_status() {
    step "Deployment Status"
    echo ""

    echo "Pods:"
    kubectl get pods -n ${NAMESPACE}
    echo ""

    echo "Services:"
    kubectl get services -n ${NAMESPACE}
    echo ""

    echo -e "${GREEN}Deployment complete!${NC}"
    echo ""
    echo "To access time-manager API:"
    echo "  kubectl port-forward -n ${NAMESPACE} service/time-manager 5000:5000"
    echo ""
    echo "To start simulation:"
    echo "  curl -X POST http://localhost:5000/api/v1/clock/start"
    echo ""
    echo "To check logs:"
    echo "  kubectl logs -n ${NAMESPACE} -l app=time-manager -f"
    echo "  kubectl logs -n ${NAMESPACE} -l app=bike-streamer -f"
    echo ""
}

# Main deployment
main() {
    check_prerequisites
    build_images
    load_images_kind
    deploy_infrastructure
    wait_infrastructure
    deploy_time_manager
    deploy_streamers
    show_status
}

# Parse arguments
case "${1:-deploy}" in
    deploy)
        main
        ;;
    build)
        check_prerequisites
        build_images
        load_images_kind
        ;;
    clean)
        step "Cleaning up deployment..."
        kubectl delete namespace ${NAMESPACE} --ignore-not-found=true
        echo "✓ Cleanup complete"
        ;;
    status)
        show_status
        ;;
    *)
        echo "Usage: $0 {deploy|build|clean|status}"
        echo ""
        echo "Commands:"
        echo "  deploy - Build images and deploy to Kubernetes (default)"
        echo "  build  - Only build Docker images"
        echo "  clean  - Delete all deployed resources"
        echo "  status - Show deployment status"
        exit 1
        ;;
esac
