.DEFAULT_GOAL := help

COMPOSE = docker compose -f docker/docker-compose.yml
FLINK_JM = lakehouse-jobmanager

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
.PHONY: build
build: ## Build the custom Flink image (run once, or after Dockerfile changes)
	$(COMPOSE) build jobmanager

.PHONY: up
up: ## Start all services (builds image if not present)
	$(COMPOSE) up -d

.PHONY: down
down: ## Stop and remove all containers (keeps volumes)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop containers AND delete all volumes (DELETES ALL DATA)
	$(COMPOSE) down -v

.PHONY: ps
ps: ## Show running containers
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail all container logs
	$(COMPOSE) logs -f

.PHONY: logs-flink
logs-flink: ## Tail Flink JobManager logs
	$(COMPOSE) logs -f jobmanager taskmanager

# ---------------------------------------------------------------------------
# Catalog & Table setup
# ---------------------------------------------------------------------------
.PHONY: setup-catalog
setup-catalog: ## Create Iceberg namespace and tables in Nessie
	docker exec $(FLINK_JM) python /opt/flink/catalog/setup_catalog.py

# ---------------------------------------------------------------------------
# Flink jobs
# ---------------------------------------------------------------------------
.PHONY: run-kafka-job
run-kafka-job: ## Submit the Kafka → Iceberg streaming job
	docker exec $(FLINK_JM) flink run \
		--python /opt/flink/jobs/kafka_to_iceberg.py \
		--pyFiles /opt/flink/catalog

.PHONY: flink-ui
flink-ui: ## Open Flink Web UI
	start http://localhost:8081

.PHONY: jobs
jobs: ## List running Flink jobs
	docker exec $(FLINK_JM) flink list

.PHONY: cancel-job
cancel-job: ## Cancel a running job (JOB_ID=<id>)
	docker exec $(FLINK_JM) flink cancel $(JOB_ID)

# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------
.PHONY: compact
compact: ## Run Iceberg compaction on benchmark_events
	docker exec $(FLINK_JM) python /opt/flink/jobs/compaction.py

# ---------------------------------------------------------------------------
# Nessie branches
# ---------------------------------------------------------------------------
.PHONY: nessie-list
nessie-list: ## List Nessie branches and tags
	docker exec $(FLINK_JM) python /opt/flink/nessie/nessie_branches.py --list

.PHONY: nessie-dev
nessie-dev: ## Create a 'dev' branch from main
	docker exec $(FLINK_JM) python /opt/flink/nessie/nessie_branches.py --create dev

.PHONY: nessie-merge
nessie-merge: ## Merge 'dev' branch into main
	docker exec $(FLINK_JM) python /opt/flink/nessie/nessie_branches.py --merge dev --into main

# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------
.PHONY: query
query: ## Query latest snapshot of benchmark_events (local DuckDB)
	uv run python analytics/query_with_duckdb.py

.PHONY: snapshots
snapshots: ## List all snapshots (for time travel)
	uv run python analytics/query_with_duckdb.py --snapshots

# ---------------------------------------------------------------------------
# UI shortcuts
# ---------------------------------------------------------------------------
.PHONY: ui
ui: ## Open all UIs in browser
	start http://localhost:8081   # Flink
	start http://localhost:8080   # Kafka UI
	start http://localhost:9001   # MinIO
	start http://localhost:19120  # Nessie

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
