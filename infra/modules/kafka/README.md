# Boston Transport Data Pipeline

## Quick Start
1. Build and push Docker image: `docker build -t oschr20/kafka-producers:latest .`
2. Deploy: `cd infra/environments/local && terraform apply`
3. Check producers: `kubectl logs -n bigdata -l app=bike-producer`