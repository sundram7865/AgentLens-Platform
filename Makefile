# Convenience targets. Everything here is a one-liner you could type instead;
# they exist so the commands are discoverable and identical to what CI runs.
.DEFAULT_GOAL := help
.PHONY: help setup up down logs migrate test lint fmt typecheck audit \
        api worker traffic loadtest dashboard clean reset

PY ?= python
VENV := .venv/Scripts/python.exe
ifeq ($(wildcard $(VENV)),)
  VENV := .venv/bin/python
endif
ifeq ($(wildcard $(VENV)),)
  VENV := $(PY)
endif

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Create the venv and install everything
	$(PY) -m venv .venv
	$(VENV) -m pip install -q --upgrade pip
	$(VENV) -m pip install -q -r packages/obs-platform/requirements-dev.txt
	$(VENV) -m pip install -q --no-deps -e packages/obs-sdk -e packages/obs-platform
	cd apps/dashboard && npm install
	@echo "Now: cp .env.example .env && make up"

up: ## Start the full local stack
	docker compose up -d
	@echo "API      http://localhost:8000/health"
	@echo "Docs     http://localhost:8000/docs"

down: ## Stop the stack (keeps volumes)
	docker compose down

logs: ## Tail the API and worker logs
	docker compose logs -f api worker

migrate: ## Apply migrations against OBS_DATABASE_DIRECT_URL
	cd packages/obs-platform && $(abspath $(VENV)) -m alembic upgrade head

test: ## Run the unit suite (no containers required)
	$(VENV) -m pytest

test-integration: ## Run the integration suite against real Postgres and Redis
	docker compose up -d postgres redis
	OBS_INTEGRATION=1 OBS_ENVIRONMENT=test $(VENV) -m pytest -m integration

test-e2e: ## Run the end-to-end suite (spawns real worker and API processes)
	docker compose up -d postgres redis
	OBS_E2E=1 $(VENV) -m pytest -m e2e -p no:cacheprovider

test-all: test test-integration test-e2e ## All three layers

lint: ## Lint and format-check Python and the dashboard
	$(VENV) -m ruff check .
	$(VENV) -m ruff format --check .
	cd apps/dashboard && npx --no-install eslint .

fmt: ## Auto-format
	$(VENV) -m ruff check . --fix
	$(VENV) -m ruff format .

typecheck: ## mypy and tsc
	$(VENV) -m mypy
	cd apps/dashboard && npx --no-install tsc --noEmit

audit: ## Dependency vulnerability scan (both ecosystems)
	$(VENV) -m pip_audit --strict -r packages/obs-platform/requirements.txt
	$(VENV) -m pip_audit --strict -r packages/obs-sdk/requirements.txt
	cd apps/dashboard && npm audit --audit-level=high

api: ## Run the API against the local stack
	OBS_DATABASE_URL=postgresql+asyncpg://obs:obs@localhost:5432/obs \
	OBS_REDIS_URL=redis://localhost:6379/0 OBS_LOG_JSON=false \
	PYTHONPATH="packages/obs-sdk/src:packages/obs-platform/src" \
	$(VENV) -m uvicorn obs_platform.api.main:app --reload --port 8000

worker: ## Run all consumer roles
	OBS_DATABASE_URL=postgresql+asyncpg://obs:obs@localhost:5432/obs \
	OBS_REDIS_URL=redis://localhost:6379/0 OBS_LOG_JSON=false \
	PYTHONPATH="packages/obs-sdk/src:packages/obs-platform/src" \
	$(VENV) -m obs_platform.workers.cli --roles storage,guardrail,eval,scheduler

traffic: ## Generate demo traffic (deterministic with --seed)
	OBS_REDIS_URL=redis://localhost:6379/0 $(VENV) scripts/traffic_sim.py --traces 50 --seed 7

loadtest: ## Ingestion load test against the local stack
	OBS_DATABASE_URL=postgresql+asyncpg://obs:obs@localhost:5432/obs \
	OBS_REDIS_URL=redis://localhost:6379/0 \
	$(VENV) loadtest/ingest_load.py --traces 1500 --concurrency 30 --rate 4

tune: ## Re-measure the injection threshold and PII memory footprint
	$(VENV) scripts/tune_injection_threshold.py
	$(VENV) scripts/measure_pii_memory.py

dashboard: ## Run the dashboard in dev mode
	cd apps/dashboard && npm run dev

clean: ## Remove build and cache artefacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache coverage.xml .coverage
	rm -rf apps/dashboard/.next
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

reset: ## Drop all local data and start fresh (DESTRUCTIVE)
	docker compose down -v
	docker compose up -d
