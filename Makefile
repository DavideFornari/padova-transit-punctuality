.PHONY: up down lint test fmt pre-commit-install dbt-run dbt-test dashboard publish-bq

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
