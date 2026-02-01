# BQ2PG Deployment with GitHub Student Pack

This guide explains how to use free services from your GitHub Student Pack for deploying, logging, monitoring, and managing the BQ2PG project.

## Table of Contents
- [Free Services Available](#free-services-available)
- [GitHub Actions CI/CD](#github-actions-cicd)
- [Logging & Monitoring](#logging--monitoring)
- [Container Registry](#container-registry)
- [Alternative Free Services](#alternative-free-services)
- [Cost Optimization](#cost-optimization)

---

## Free Services Available

### 1. **GitHub Actions** (Included Free)
- Unlimited public repositories
- 2,000 free minutes/month for private repos (as student)
- Perfect for CI/CD pipeline

### 2. **GitHub Container Registry** (ghcr.io)
- Free public containers
- Included with GitHub
- No bandwidth charges for public repos

### 3. **GitHub Pages**
- Free static hosting for docs/dashboards
- Deploy reports, metrics, documentation

### 4. **GitHub Packages**
- Free Docker image storage
- Python package distribution

### 5. **Azure Free Tier** (via Student Pack)
- 12 months free
- $200 free credits for first month
- Perfect for production deployment

### 6. **DigitalOcean** (via Student Pack)
- $50-100 in credits
- Basic VPS for API hosting
- PostgreSQL database hosting

---

## GitHub Actions CI/CD

### Current Setup
Your workflow is already configured in `.github/workflows/ci-cd.yaml`:
- **Lint & Test**: Runs on Python 3.10, 3.11, 3.12
- **Integration Tests**: PostgreSQL service
- **Code Coverage**: Via Codecov (free for public repos)

### Maximizing Free Usage

```yaml
# Optimize usage to stay within free tier:
- Run tests on schedule: cron '0 2 * * *'  # Daily, off-peak
- Use matrix strategy for parallel testing
- Cache dependencies with actions/setup-python@v4
- Only deploy on tagged releases or main branch
```

### Example: Deploy Workflow
```yaml
name: Deploy to GitHub Pages
on:
  push:
    branches: [main]
  schedule:
    - cron: '0 0 * * 0'  # Weekly

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Generate reports
        run: |
          pip install -r requirements-dev.txt
          pytest tests/ --html=report.html --cov=src --cov-report=html
      - uses: actions/upload-artifact@v3
        with:
          name: coverage-report
          path: htmlcov/
      - uses: actions/upload-pages-artifact@v2
        with:
          path: 'htmlcov/'
      - uses: actions/deploy-pages@v2
```

---

## Logging & Monitoring

### Option 1: GitHub Pages + Static Reports (FREE)
```bash
# Generate HTML reports from tests
pytest tests/ --html=report.html --cov=src --cov-report=html

# Deploy via GitHub Pages (automated via CI/CD)
# Access at: https://your-username.github.io/BQ2PG/
```

### Option 2: Grafana Cloud (Free Tier)
**Limits**: 3 dashboards, 7-day data retention, 1GB metrics storage

```bash
# 1. Sign up at https://grafana.com/auth/sign-up/create-user/account
# 2. Create free account
# 3. Set Prometheus datasource

# Configure in docker-compose.yaml:
prometheus:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"
  volumes:
    - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
  command:
    - '--config.file=/etc/prometheus/prometheus.yml'
    - '--storage.tsdb.path=/prometheus'

# Send metrics to Grafana Cloud:
# export GRAFANA_CLOUD_API_TOKEN=your-token
# export GRAFANA_CLOUD_METRICS_URL=your-url
```

### Option 3: OpenTelemetry + Jaeger (FREE, Self-Hosted)
```bash
# Deploy Jaeger locally in Docker
docker run -d \
  -p 6831:6831/udp \
  -p 16686:16686 \
  jaegertracing/all-in-one

# Access at: http://localhost:16686
```

### Option 4: Loki (FREE Log Aggregation)
```bash
# Configure logging in docker-compose.yaml:
loki:
  image: grafana/loki:latest
  ports:
    - "3100:3100"

# Application sends logs:
# import logging
# handler = logging.handlers.HTTPHandler('localhost', 3100, '/loki/api/v1/push')
```

---

## Container Registry

### Using GitHub Container Registry (GHCR)
Already configured in `.github/workflows/ci-cd.yaml`:

```yaml
env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

- name: Build and push Docker image
  uses: docker/build-push-action@v5
  with:
    context: .
    push: ${{ github.event_name == 'push' }}
    tags: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:latest
    labels: ${{ steps.meta.outputs.labels }}
```

### Push to GHCR
```bash
# 1. Generate token: Settings > Developer settings > Personal access tokens
# 2. Grant `write:packages` permission

# 3. Login to GHCR:
echo $GHCR_TOKEN | docker login ghcr.io -u <username> --password-stdin

# 4. Push image:
docker build -t ghcr.io/your-username/bq2pg:latest .
docker push ghcr.io/your-username/bq2pg:latest
```

---

## Alternative Free Services

### Logging
| Service | Free Tier | Best For |
|---------|-----------|----------|
| **Grafana Cloud** | 3 dashboards, 7d retention | Metrics & visualization |
| **Jaeger** | Self-hosted unlimited | Distributed tracing |
| **Loki** | Self-hosted unlimited | Log aggregation |
| **ELK Stack** | Self-hosted unlimited | Full logging ecosystem |
| **Papertrail** | 7d retention, 48h search | Cloud logging (limited) |

### Monitoring
| Service | Free Tier | Best For |
|---------|-----------|----------|
| **Prometheus** | Self-hosted unlimited | Metrics collection |
| **Grafana** | Self-hosted unlimited | Dashboards & alerts |
| **Uptime Robot** | 50 monitors, 5min check | Uptime monitoring |
| **Better Uptime** | Basic plan free | Status page |

### Hosting
| Service | Free Tier | Best For |
|---------|-----------|----------|
| **Heroku** | Shuttered (use alternatives) | ~~Python apps~~ |
| **Railway** | $5/month free | Python/Docker apps |
| **Render** | $7.50-25/month free tier | Web services |
| **Azure (Student)** | 12 months + $200/mo credit | Production workloads |
| **DigitalOcean (Student)** | $50-100 credits | PostgreSQL + VPS |

---

## Cost Optimization

### 1. Use Azure/DigitalOcean Student Credits
```bash
# Deploy API only (not full pipeline) to save costs
# Use PostgreSQL shared database hosting
# Store BigQuery results in object storage
```

### 2. GitHub Actions Optimization
```yaml
# Strategy: Run expensive jobs less frequently
- schedule:
    - cron: '0 2 * * *'  # Once daily instead of every push

# Conditional runs:
- if: github.event_name == 'push' && github.ref == 'refs/heads/main'
  run: npm run build
```

### 3. Recommended Stack (100% FREE)
```
├── Code Repository: GitHub (free)
├── CI/CD: GitHub Actions (free)
├── Container Registry: GHCR (free)
├── API Hosting: Railway ($5/mo) or Railway free tier
├── Database: PostgreSQL (local dev, Railway for prod)
├── Monitoring: Prometheus + Grafana (self-hosted)
├── Logging: Loki + Grafana (self-hosted)
├── Status Page: Gitops + GitHub Pages (free)
└── Documentation: GitHub Pages (free)
```

### 4. Production Deployment (Using Student Credits)

#### Option A: Azure (Recommended for Students)
```bash
# 1. Activate Azure for Students
# 2. Create resource group
az group create --name bq2pg --location eastus

# 3. Deploy API
az appservice plan create --name bq2pg-plan --resource-group bq2pg --sku FREE
az webapp create --name bq2pg --plan bq2pg-plan --resource-group bq2pg --runtime "PYTHON|3.11"

# 4. Configure PostgreSQL
az postgres server create \
  --name bq2pg-db \
  --resource-group bq2pg \
  --admin-user admin \
  --admin-password password123 \
  --location eastus \
  --sku-name B_Gen5_1

# 5. Deploy via GitHub Actions
# Add secrets: AZURE_CREDENTIALS, AZURE_RG
```

#### Option B: DigitalOcean (Credits)
```bash
# 1. Use $50-100 student credits
# 2. Create $5/mo basic droplet
# 3. Deploy via GitHub Actions

# Example droplet config:
doctl compute droplet create bq2pg-api \
  --region nyc3 \
  --image python-22-04-x64 \
  --size s-1vcpu-512mb-10gb

# Connect via GitHub Actions:
- name: Deploy to DigitalOcean
  uses: appleboy/ssh-action@master
  with:
    host: ${{ secrets.DROPLET_IP }}
    username: root
    key: ${{ secrets.SSH_KEY }}
    script: |
      cd /app
      git pull origin main
      docker-compose up -d
```

---

## Implementation Checklist

- [ ] **GitHub Actions**: Configured and tested
- [ ] **GHCR**: Container images building and pushing
- [ ] **GitHub Pages**: Test reports deployed
- [ ] **Monitoring**: Choose Grafana Cloud or self-hosted
- [ ] **Logging**: Choose Loki or centralized logging
- [ ] **Azure/DigitalOcean**: Account set up with student credits
- [ ] **Secrets Management**: GitHub Secrets configured for API keys
- [ ] **Cost Monitoring**: Set up billing alerts
- [ ] **Documentation**: Added to team wiki/README

---

## Quick Start: Deploy API with Free Services

```bash
#!/bin/bash
# 1. Build container
docker build -t ghcr.io/your-username/bq2pg:latest .

# 2. Test locally
docker run -p 5000:5000 ghcr.io/your-username/bq2pg:latest

# 3. Push to GHCR
docker push ghcr.io/your-username/bq2pg:latest

# 4. Deploy to Railway/Render
# - Connect GitHub repo
# - Set environment variables
# - Deploy automatically from main branch

# 5. Monitor via Grafana Cloud
# - Configure Prometheus datasource
# - Create dashboard
# - Set up alerts
```

---

## Resources

- [GitHub Student Pack](https://education.github.com/pack)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [GHCR Documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
- [Grafana Cloud Free Tier](https://grafana.com/pricing/)
- [Azure for Students](https://azure.microsoft.com/en-us/free/students/)
- [DigitalOcean Student Program](https://www.digitalocean.com/github-students/)
- [Railway Deployment](https://railway.app)
- [Render Deployment](https://render.com)

---

## Support

For issues or questions about free tier limitations:
1. Check service documentation for current free tier limits
2. Review GitHub Student Pack benefits (updated annually)
3. Consider self-hosted alternatives for unlimited scale
