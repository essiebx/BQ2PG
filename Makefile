# Makefile - Podman Commands for BQ2PG Pipeline
.PHONY: help build up down test clean destroy logs psql pgadmin shell init status prune

# Colors for output
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

help:
	@echo "$(GREEN)🚀 BQ2PG Pipeline - Podman Commands$(NC)"
	@echo ""
	@echo "$(YELLOW)Project Setup:$(NC)"
	@echo "  make init         Initialize project structure"
	@echo "  make build        Build Podman images"
	@echo ""
	@echo "$(YELLOW)Service Management:$(NC)"
	@echo "  make up           Start all services (detached)"
	@echo "  make up-logs      Start with logs"
	@echo "  make down         Stop all services"
	@echo "  make logs         View pipeline logs"
	@echo "  make status       Show container status"
	@echo ""
	@echo "$(YELLOW)Pipeline Operations:$(NC)"
	@echo "  make test         Run test pipeline (1000 rows)"
	@echo "  make sample       Run sample pipeline (~10k rows)"
	@echo "  make recent       Load recent patents (30 days)"
	@echo ""
	@echo "$(YELLOW)Database Access:$(NC)"
	@echo "  make psql         Connect to PostgreSQL"
	@echo "  make pgadmin      Open pgAdmin info"
	@echo "  make shell        Open shell in pipeline container"
	@echo ""
	@echo "$(YELLOW)Cleanup:$(NC)"
	@echo "  make clean        Remove containers and networks"
	@echo "  make destroy      Remove everything (including volumes)"
	@echo "  make prune        Clean up Podman system"
	@echo ""
	@echo "$(YELLOW)Utilities:$(NC)"
	@echo "  make env-test     Test Podman environment"
	@echo "  make config-test  Test configuration"

# Initialize project
init:
	@echo "$(GREEN)🚀 Initializing BQ2PG Project...$(NC)"
	mkdir -p credentials data logs scripts queries/patents
	@echo "$(GREEN)📁 Created directories$(NC)"
	@test -f .env.example || cp docker.env .env.example
	@test -f docker.env || cp .env.example docker.env
	@echo "$(GREEN)📋 Created environment files$(NC)"
	@echo ""
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. Edit docker.env with your configuration"
	@echo "  2. Add Google Cloud key to credentials/key.json"
	@echo "  3. Run: make build"
	@echo "  4. Run: make up"
	@echo "  5. Run: make test"

# Build images
build:
	@echo "$(GREEN)🔨 Building Podman images...$(NC)"
	podman-compose build
	@echo "$(GREEN)✅ Images built successfully$(NC)"

# Start services in background
up:
	@echo "$(GREEN)🚀 Starting services...$(NC)"
	podman-compose up -d
	@sleep 5
	@echo "$(GREEN)✅ Services started$(NC)"
	@echo "$(YELLOW)📊 Service endpoints:$(NC)"
	@echo "  PostgreSQL: localhost:5432"
	@echo "  pgAdmin:    http://localhost:8080"
	@echo "  Credentials: admin@bq2pg.com / \$${PGADMIN_PASSWORD:-admin123}"

# Start with logs
up-logs:
	podman-compose up

# Stop services
down:
	@echo "$(YELLOW)🛑 Stopping services...$(NC)"
	podman-compose down
	@echo "$(GREEN)✅ Services stopped$(NC)"

# Run test pipeline
test:
	@echo "$(GREEN)🧪 Running test pipeline (1000 rows)...$(NC)"
	podman-compose run --rm pipeline python main.py --limit 1000 --drop-tables

# Run sample pipeline
sample:
	@echo "$(GREEN)📊 Running sample pipeline (~10k rows)...$(NC)"
	podman-compose run --rm pipeline python main.py --sample-size 10000 --drop-tables

# Run recent pipeline
recent:
	@echo "$(GREEN)🕐 Loading recent patents (30 days)...$(NC)"
	podman-compose run --rm pipeline python main.py --recent-days 30 --drop-tables

# Clean up containers and networks
clean:
	@echo "$(YELLOW)🧹 Cleaning up...$(NC)"
	podman-compose down -v --remove-orphans
	@echo "$(GREEN)✅ Cleaned up containers and networks$(NC)"

# Destroy everything
destroy:
	@echo "$(RED)💥 Destroying all resources...$(NC)"
	podman-compose down -v --rmi all --remove-orphans
	@echo "$(GREEN)✅ All resources destroyed$(NC)"

# View logs
logs:
	podman-compose logs -f pipeline

# Connect to PostgreSQL
psql:
	@echo "$(GREEN)🔗 Connecting to PostgreSQL...$(NC)"
	podman exec -it bq2pg-postgres psql -U postgres -d patents_db

# Open pgAdmin info
pgadmin:
	@echo "$(GREEN)🌐 pgAdmin Web Interface$(NC)"
	@echo "  URL: http://localhost:8080"
	@echo "  Email: admin@bq2pg.com"
	@echo "  Password: \$${PGADMIN_PASSWORD:-admin123}"
	@echo ""
	@echo "$(YELLOW)To add PostgreSQL server in pgAdmin:$(NC)"
	@echo "  1. Login to pgAdmin"
	@echo "  2. Right-click 'Servers' → 'Register' → 'Server'"
	@echo "  3. Name: BQ2PG Pipeline"
	@echo "  4. Connection → Host: postgres, Port: 5432"
	@echo "  5. Username: postgres, Password: \$${POSTGRES_PASSWORD:-postgres123}"

# Shell into pipeline container
shell:
	@echo "$(GREEN)🐚 Opening container shell...$(NC)"
	podman exec -it bq2pg-pipeline /bin/bash

# Test environment
env-test:
	@echo "$(GREEN)🧪 Testing Podman environment...$(NC)"
	podman-compose run --rm pipeline python docker_test.py

# Test configuration
config-test:
	@echo "$(GREEN)⚙️ Testing configuration...$(NC)"
	podman-compose run --rm pipeline python -c "from src.config import config; config.validate(); print('✅ Configuration valid')"

# Show status
status:
	@echo "$(GREEN)📊 Podman System Status$(NC)"
	@echo ""
	@echo "$(YELLOW)Containers:$(NC)"
	podman ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "$(YELLOW)Images:$(NC)"
	podman images --filter "reference=*bq2pg*" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
	@echo ""
	@echo "$(YELLOW)Networks:$(NC)"
	podman network ls --filter "name=bq2pg" --format "table {{.Name}}\t{{.Driver}}\t{{.IPv6}}\t{{.Internal}}"
	@echo ""
	@echo "$(YELLOW)Volumes:$(NC)"
	podman volume ls --filter "name=bq2pg" --format "table {{.Name}}\t{{.Driver}}\t{{.Mountpoint}}"

# Prune system
prune:
	@echo "$(YELLOW)🧹 Pruning Podman system...$(NC)"
	podman system prune -f
	@echo "$(GREEN)✅ System pruned$(NC)"