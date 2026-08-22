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

.PHONY: restart
restart: ## Restart all services
	$(COMPOSE) restart

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
compact: ## Run Iceberg compaction on order_events
	docker exec $(FLINK_JM) python /opt/flink/jobs/compaction.py

# ---------------------------------------------------------------------------
# Nessie branches
# ---------------------------------------------------------------------------
.PHONY: nessie-list
nessie-list: ## List Nessie branches and tags
	docker exec $(FLINK_JM) python /opt/flink/nessie/nessie_branches.py --list

BRANCH ?= dev

.PHONY: nessie-create
nessie-create: ## Create a branch from main (Usage: make nessie-create-branch BRANCH=my-branch)
	docker exec $(FLINK_JM) python /opt/flink/nessie/nessie_branches.py --create $(BRANCH)

.PHONY: nessie-delete
nessie-delete: ## Create a branch from main (Usage: make nessie-create-branch BRANCH=my-branch)
	docker exec $(FLINK_JM) python /opt/flink/nessie/nessie_branches.py --delete $(BRANCH)


.PHONY: nessie-merge
nessie-merge: ## Merge a branch into main (Usage: make nessie-merge BRANCH=my-branch)
	docker exec $(FLINK_JM) python /opt/flink/nessie/nessie_branches.py --merge $(BRANCH) --into main

# ---------------------------------------------------------------------------
# Event Generation
# ---------------------------------------------------------------------------
COUNT ?= 100

.PHONY: generate-events
generate-events: ## Generate N order_events via REST API (Usage: make generate-events COUNT=500)
	@echo "Generating $(COUNT) order events..."
	@curl -s -X POST http://localhost:8090/kafka/generateMessages \
		-d 'topic_name=order_events' \
		-d 'bootstrap_servers=kafka:9092' \
		-d 'count=$(COUNT)' | python -m json.tool
	@echo "Done."

.PHONY: enable-generation
enable-generation: ## Set ACTIVE_GENERATION=true in .env and restart the event-generator
	@powershell -Command "(Get-Content .env) -replace 'ACTIVE_GENERATION=false','ACTIVE_GENERATION=true' | Set-Content .env"
	$(COMPOSE) up -d event-generator
	@echo "Background generation enabled."

.PHONY: disable-generation
disable-generation: ## Set ACTIVE_GENERATION=false in .env and restart the event-generator
	@powershell -Command "(Get-Content .env) -replace 'ACTIVE_GENERATION=true','ACTIVE_GENERATION=false' | Set-Content .env"
	$(COMPOSE) up -d event-generator
	@echo "Background generation disabled."

# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------
N ?= 10

.PHONY: query
query: ## Query latest snapshot of order_events (local DuckDB). Usage: make query N=100
	cd analytics && uv run python query_with_duckdb.py -n $(N)

.PHONY: snapshots
snapshots: ## List all snapshots (for time travel)
	cd analytics && uv run python query_with_duckdb.py --snapshots

# ---------------------------------------------------------------------------
# Trino Query Engine
# ---------------------------------------------------------------------------
.PHONY: trino
trino: ## Launch Trino CLI
	docker exec -it lakehouse-trino trino --server localhost:8082


# ---------------------------------------------------------------------------
# UI shortcuts
# ---------------------------------------------------------------------------
.PHONY: ui
ui: ## Open all UIs in browser
	start http://localhost:8081   # Flink
	start http://localhost:8080   # Kafka UI
	start http://localhost:9001   # MinIO
	start http://localhost:19120  # Nessie
	start http://localhost:8082   # Trino

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
