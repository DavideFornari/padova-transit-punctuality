.PHONY: venv up down lint test fmt pre-commit-install dbt-run dbt-test dashboard publish-bq

# Local virtualenv — Python 3.12, matching the Airflow image and CI.
PY_VERSION := 3.12
PYTHON ?= $(if $(OS),py -$(PY_VERSION),python$(PY_VERSION))
VENV_BIN := $(if $(OS),.venv/Scripts,.venv/bin)
AIRFLOW_VERSION := 2.10.5
AIRFLOW_CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PY_VERSION).txt

venv: ## Create .venv (Python 3.12) and install the project with dev extras
	$(PYTHON) -m venv .venv
	$(VENV_BIN)/python -m pip install --upgrade pip
	# Airflow goes in first under its own constraint file: pip cannot resolve
	# apache-airflow reliably without it.  Installing the project afterwards
	# applies our pins on top (they deliberately differ from the constraints
	# for duckdb, pyarrow and ruff, so the two cannot be combined in one step).
	$(VENV_BIN)/python -m pip install "apache-airflow==$(AIRFLOW_VERSION)" --constraint "$(AIRFLOW_CONSTRAINTS)"
	$(VENV_BIN)/python -m pip install -e ".[dev]"
	@echo "Done. Activate with: source $(VENV_BIN)/activate"

up:   ## Start Airflow and Postgres
	docker compose up --build -d

down: ## Stop all services and remove orphan containers
	docker compose down --remove-orphans

lint: ## Run ruff linter and formatter check
	ruff check src/ dags/ tests/
	ruff format --check src/ dags/ tests/

test: ## Run pytest
	pytest

fmt:  ## Auto-format and auto-fix lint issues
	ruff check --fix src/ dags/ tests/
	ruff format src/ dags/ tests/

pre-commit-install: ## Install pre-commit hooks into .git/hooks
	pre-commit install

dbt-run: ## Run dbt models
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

dashboard: ## Launch the Streamlit dashboard
	streamlit run src/padova_transit/dashboard/app.py

publish-bq: ## Publish the dbt marts to BigQuery (optional cloud variant)
	python -m padova_transit.cloud.bigquery
