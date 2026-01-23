# Kubernetes Deployment

This directory contains Kubernetes manifests for deploying Conductor Server Temporal.

## Prerequisites

- Kubernetes cluster (1.25+)
- kubectl configured
- Temporal server deployed (e.g., via Helm chart)
- Container image built and pushed to a registry

## Files

| File | Description |
|------|-------------|
| `configmap.yaml` | Application configuration (environment variables) |
| `deployment.yaml` | Deployment with health checks, resource limits, security context |
| `service.yaml` | ClusterIP Service and ServiceAccount |
| `hpa.yaml` | HorizontalPodAutoscaler for auto-scaling |
| `kustomization.yaml` | Kustomize configuration for easy deployment |

## Quick Start

### 1. Build and Push Image

```bash
# From project root
docker build -t your-registry/conductor-server-temporal:latest .
docker push your-registry/conductor-server-temporal:latest
```

### 2. Update Image Reference

Edit `kustomization.yaml` to point to your image:

```yaml
images:
  - name: conductor-server-temporal
    newName: your-registry/conductor-server-temporal
    newTag: v1.0.0
```

### 3. Configure Temporal Connection

Edit `configmap.yaml` or create an overlay:

```yaml
data:
  TEMPORAL_SERVICE_ADDRESS: "temporal-frontend.temporal:7233"
  TEMPORAL_NAMESPACE: "your-namespace"
```

### 4. Deploy

```bash
# Create namespace
kubectl create namespace conductor

# Deploy using kustomize
kubectl apply -k k8s/

# Or deploy individual files
kubectl apply -f k8s/configmap.yaml -n conductor
kubectl apply -f k8s/service.yaml -n conductor
kubectl apply -f k8s/deployment.yaml -n conductor
kubectl apply -f k8s/hpa.yaml -n conductor
```

### 5. Verify Deployment

```bash
# Check pods
kubectl get pods -n conductor -l app=conductor-server

# Check logs
kubectl logs -n conductor -l app=conductor-server -f

# Check service
kubectl get svc -n conductor conductor-server
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_PROFILES_ACTIVE` | `temporal` | Spring profile (`temporal` or `stub`) |
| `TEMPORAL_SERVICE_ADDRESS` | `localhost:7233` | Temporal frontend address |
| `TEMPORAL_NAMESPACE` | `conductor` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `conductor-workflows` | Task queue name |
| `JAVA_OPTS` | (see configmap) | JVM options |

### Resource Requirements

Default resource settings:
- **Requests**: 250m CPU, 512Mi memory
- **Limits**: 1000m CPU, 1Gi memory

Adjust in `deployment.yaml` based on your workload.

### Scaling

The HPA is configured to:
- Maintain 2-10 replicas
- Scale up at 70% CPU or 80% memory utilization
- Scale down gradually (10% per minute)
- Scale up quickly (up to 4 pods per 15 seconds)

## Monitoring

### Health Endpoints

- **Liveness**: `/actuator/health/liveness`
- **Readiness**: `/actuator/health/readiness`
- **Metrics**: `/actuator/prometheus`

### Prometheus Integration

The deployment includes annotations for Prometheus scraping:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/actuator/prometheus"
```

## Security

The deployment follows security best practices:
- Non-root user (UID 1000)
- Read-only root filesystem
- Dropped capabilities
- No privilege escalation
- Pod anti-affinity for high availability

## Troubleshooting

### Pod not starting

```bash
# Check events
kubectl describe pod -n conductor -l app=conductor-server

# Check logs
kubectl logs -n conductor -l app=conductor-server --previous
```

### Cannot connect to Temporal

1. Verify Temporal service address
2. Check network policies
3. Ensure Temporal namespace exists

```bash
# Test connectivity
kubectl run -it --rm debug --image=busybox -n conductor -- nc -zv temporal-frontend.temporal 7233
```
