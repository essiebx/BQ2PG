# PRODUCTION_DEPLOYMENT.md

## BQ2PG Production Deployment Guide

Comprehensive guide for deploying BQ2PG pipeline to production on Google Cloud Platform with Kubernetes.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Infrastructure Setup](#infrastructure-setup)
3. [Deployment](#deployment)
4. [Monitoring & Observability](#monitoring--observability)
5. [Scaling](#scaling)
6. [Disaster Recovery](#disaster-recovery)
7. [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Tools

- `terraform` >= 1.0
- `kubectl` >= 1.27
- `helm` >= 3.12
- `gcloud` CLI configured with appropriate credentials
- `docker` for building images

### GCP Setup

```bash
# Set project
gcloud config set project YOUR_PROJECT_ID

# Enable required APIs
gcloud services enable container.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable sqladmin.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable monitoring.googleapis.com
gcloud services enable logging.googleapis.com

# Create Cloud Storage bucket for Terraform state
gsutil mb gs://bq2pg-terraform-state
gsutil versioning set on gs://bq2pg-terraform-state
```

### GitHub Secrets

Set up the following secrets in GitHub:

```
KUBE_CONFIG_STAGING    - Staging kubeconfig (base64 encoded)
KUBE_CONFIG_PROD       - Production kubeconfig (base64 encoded)
SLACK_WEBHOOK          - Slack webhook for notifications
GCP_PROJECT_ID         - GCP project ID
GCP_SA_KEY             - GCP service account key (JSON)
```

## Infrastructure Setup

### 1. Initialize Terraform

```bash
cd terraform

# Initialize backend
terraform init -backend-config="bucket=bq2pg-terraform-state" \
               -backend-config="prefix=prod"

# Validate configuration
terraform validate

# Plan deployment
terraform plan -var-file=terraform.tfvars -out=tfplan

# Apply configuration
terraform apply tfplan
```

### 2. Configure kubectl

```bash
# Get kubeconfig
gcloud container clusters get-credentials bq2pg-cluster \
  --region us-central1 \
  --project YOUR_PROJECT_ID

# Verify cluster access
kubectl cluster-info
kubectl get nodes
```

### 3. Create Namespaces

```bash
# BQ2PG namespace
kubectl create namespace bq2pg

# Monitoring namespace
kubectl create namespace monitoring

# Label namespaces
kubectl label namespace bq2pg environment=production
kubectl label namespace monitoring environment=monitoring
```

## Deployment

### 1. Prepare Secrets

```bash
# Create service account key
gcloud iam service-accounts keys create \
  /tmp/bq2pg-sa-key.json \
  --iam-account=bq2pg-pipeline@YOUR_PROJECT_ID.iam.gserviceaccount.com

# Create Kubernetes secret
kubectl -n bq2pg create secret generic bq2pg-credentials \
  --from-file=service-account.json=/tmp/bq2pg-sa-key.json \
  --from-literal=db-password='SECURE_PASSWORD' \
  --from-literal=slack-webhook='WEBHOOK_URL'

# Clean up
rm /tmp/bq2pg-sa-key.json
```

### 2. Deploy PostgreSQL

```bash
# Apply PostgreSQL manifests
kubectl apply -f kubernetes/postgres-deployment.yaml

# Wait for PostgreSQL to be ready
kubectl -n bq2pg wait --for=condition=ready pod \
  -l app=postgres --timeout=300s

# Verify PostgreSQL
kubectl -n bq2pg exec postgres-0 -- \
  psql -U postgres -c "SELECT version();"
```

### 3. Deploy BQ2PG Pipeline

```bash
# Option A: Using Kubectl
kubectl apply -f kubernetes/bq2pg-deployment.yaml

# Option B: Using Helm (Recommended)
helm install bq2pg helm/bq2pg/ \
  -n bq2pg \
  --values helm/bq2pg/values-production.yaml \
  --set-string secrets.googleApplicationCredentials="$(cat /path/to/key.json)" \
  --set-string secrets.dbPassword="SECURE_PASSWORD"

# Verify deployment
kubectl -n bq2pg rollout status deployment/bq2pg-pipeline
kubectl -n bq2pg get pods -l app=bq2pg-pipeline
```

### 4. Deploy Monitoring Stack

```bash
# Create monitoring namespace
kubectl create namespace monitoring

# Add Helm repositories
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

# Deploy Prometheus
kubectl apply -f kubernetes/monitoring-stack.yaml

# Wait for deployments
kubectl -n monitoring wait --for=condition=ready pod \
  -l app=prometheus --timeout=300s
```

## Monitoring & Observability

### 1. Access Dashboards

```bash
# Port forward to Grafana
kubectl -n monitoring port-forward svc/grafana 3000:3000

# Access Grafana
# URL: http://localhost:3000
# Username: admin
# Password: (from values-production.yaml)

# Port forward to Prometheus
kubectl -n monitoring port-forward svc/prometheus 9090:9090

# Access Prometheus
# URL: http://localhost:9090
```

### 2. Configure Alerts

```bash
# Create AlertManager config
kubectl apply -f kubernetes/alertmanager-config.yaml

# Verify alerts
kubectl -n monitoring get alert
```

### 3. View Logs

```bash
# BQ2PG pipeline logs
kubectl -n bq2pg logs -l app=bq2pg-pipeline --tail=100 -f

# PostgreSQL logs
kubectl -n bq2pg logs postgres-0 --tail=100 -f

# Prometheus logs
kubectl -n monitoring logs -l app=prometheus --tail=100 -f
```

## Scaling

### 1. Horizontal Pod Autoscaler

HPA is configured automatically. Check status:

```bash
kubectl -n bq2pg get hpa bq2pg-hpa -w
```

Configuration:
- Min replicas: 3
- Max replicas: 10
- CPU threshold: 70%
- Memory threshold: 80%

### 2. Manual Scaling

```bash
# Scale deployment
kubectl -n bq2pg scale deployment bq2pg-pipeline --replicas=5

# Scale StatefulSet (PostgreSQL)
kubectl -n bq2pg scale statefulset postgres --replicas=1
```

### 3. Performance Tuning

```bash
# Check resource usage
kubectl top node
kubectl top pod -n bq2pg

# Update resource limits
kubectl -n bq2pg set resources deployment bq2pg-pipeline \
  --requests=cpu=1,memory=2Gi \
  --limits=cpu=4,memory=8Gi
```

## Disaster Recovery

### 1. Backup Strategy

```bash
# Cloud SQL automated backups (hourly)
gcloud sql backups create \
  --instance=bq2pg-postgres-XXXXX

# Backup verification
gcloud sql backups describe BACKUP_ID \
  --instance=bq2pg-postgres-XXXXX

# Restore from backup
gcloud sql backups restore BACKUP_ID \
  --restore-instance=bq2pg-postgres-XXXXX
```

### 2. PostgreSQL Data Export

```bash
# Export data from PostgreSQL
kubectl -n bq2pg exec postgres-0 -- \
  pg_dump -U bq2pg bq2pg > bq2pg_backup.sql

# Upload to Cloud Storage
gsutil cp bq2pg_backup.sql \
  gs://bq2pg-logs-XXXXX/backups/bq2pg_backup_$(date +%Y%m%d).sql
```

### 3. Restore Procedure

```bash
# Connect to PostgreSQL
kubectl -n bq2pg exec -it postgres-0 -- psql -U bq2pg

# Restore from backup
psql -U bq2pg bq2pg < bq2pg_backup.sql

# Verify data
SELECT COUNT(*) FROM public.dead_letter_queue;
```

## Troubleshooting

### 1. Pod Not Starting

```bash
# Check pod status
kubectl -n bq2pg describe pod POD_NAME

# Check events
kubectl -n bq2pg get events --sort-by='.lastTimestamp'

# Check logs
kubectl -n bq2pg logs POD_NAME --previous
```

### 2. Database Connection Issues

```bash
# Test connection
kubectl -n bq2pg run -it psql --image=postgres:15 --restart=Never -- \
  psql -h postgres-service -U bq2pg -d bq2pg

# Check Cloud SQL proxy
kubectl -n bq2pg get configmap postgres-config -o yaml
```

### 3. High Memory Usage

```bash
# Check memory usage
kubectl top pod -n bq2pg --containers

# Review memory limits
kubectl -n bq2pg get deployment bq2pg-pipeline -o yaml | grep -A 5 resources:

# Update limits if needed
kubectl -n bq2pg set resources deployment bq2pg-pipeline \
  --limits=memory=8Gi
```

### 4. Slow Queries

```bash
# Enable query logging
kubectl -n bq2pg exec postgres-0 -- \
  psql -U bq2pg -d bq2pg \
  -c "ALTER DATABASE bq2pg SET log_min_duration_statement = 1000;"

# View slow query logs
kubectl -n bq2pg logs postgres-0 | grep duration
```

## Maintenance

### 1. Update Pipeline

```bash
# Build new image
docker build -t bq2pg:v1.1.0 .
docker push ghcr.io/yourorg/bq2pg:v1.1.0

# Update deployment
kubectl -n bq2pg set image deployment/bq2pg-pipeline \
  pipeline=ghcr.io/yourorg/bq2pg:v1.1.0

# Verify rollout
kubectl -n bq2pg rollout status deployment/bq2pg-pipeline
```

### 2. Upgrade Kubernetes

```bash
# Create backup
kubectl -n bq2pg exec postgres-0 -- \
  pg_dump -U bq2pg bq2pg > backup_pre_upgrade.sql

# Upgrade GKE cluster (via terraform)
terraform apply -var="cluster_version=1.28"

# Verify cluster
kubectl version
```

### 3. Database Maintenance

```bash
# Run VACUUM
kubectl -n bq2pg exec postgres-0 -- \
  psql -U bq2pg -d bq2pg -c "VACUUM ANALYZE;"

# Check index usage
kubectl -n bq2pg exec postgres-0 -- \
  psql -U bq2pg -d bq2pg \
  -c "SELECT schemaname, tablename, indexname FROM pg_indexes;"
```

## Monitoring Checklist

- [ ] Grafana dashboards accessible
- [ ] Prometheus collecting metrics
- [ ] AlertManager configured
- [ ] Slack notifications working
- [ ] PostgreSQL backups scheduled
- [ ] Log aggregation active
- [ ] Pod resource requests set
- [ ] HPA thresholds appropriate
- [ ] Network policies configured
- [ ] RBAC permissions correct

## Support & Documentation

For issues or questions:

1. Check logs: `kubectl logs -n bq2pg -l app=bq2pg-pipeline`
2. Review Grafana dashboards for metrics
3. Check GitHub Issues: https://github.com/essie/bq2pg/issues
4. Contact: essie@example.com
