.PHONY: help up down init trigger logs status clean

help:
	@echo ""
	@echo "DustiniaDelixia Groceria - Available Commands"
	@echo "──────────────────────────────────────────────"
	@echo "  make build   Build the custom Airflow image"
	@echo "  make init    First-time setup (run this once before 'up')"
	@echo "  make up      Start all services"
	@echo "  make down    Stop all services"
	@echo "  make trigger Manually trigger the finance pipeline DAG"
	@echo "  make logs    Tail logs from all containers"
	@echo "  make status  Show running containers and their health"
	@echo "  make clean   Remove all containers and volumes (DELETES DATA)"
	@echo ""

build:
	docker compose build

init:
	@echo "Creating required directories..."
	mkdir -p logs data/raw data/processed
	@echo "Setting permissions for Airflow (it runs as UID 50000)..."
	chmod -R 777 logs data
	@echo "Starting Postgres first..."
	docker compose up postgres -d
	@echo "Waiting 10 seconds for Postgres to be ready..."
	sleep 10
	@echo "Running Airflow DB init and creating admin user..."
	docker compose run --rm airflow-init
	@echo ""
	@echo "Init complete. Run 'make up' to start all services."

up:
	docker compose up -d
	@echo ""
	@echo "Services starting. Access them at:"
	@echo "  Airflow:    http://localhost:8080  (admin / admin)"
	@echo "  Metabase:   http://localhost:3000"
	@echo "  ClickHouse: http://localhost:8123"

down:
	docker compose down

trigger:
	docker compose exec airflow-webserver \
		airflow dags trigger dustinia_finance_analyst_pipeline
	@echo "DAG triggered. Check http://localhost:8080 for progress."

logs:
	docker compose logs -f

status:
	docker compose ps

clean:
	@echo "WARNING: This deletes all containers and stored data."
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ]
	docker compose down -v
	@echo "All containers and volumes removed."
