.PHONY: help setup infra infra-destroy blocks ingest load-bq dbt-run pipeline dashboard clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Prerequisites ───────────────────────────────────────────────────────────

setup: ## Install Python dependencies
	pip install -r requirements.txt

# ── Infrastructure ──────────────────────────────────────────────────────────

infra: ## Provision GCS + BigQuery with Terraform
	cd terraform && terraform init && terraform apply -auto-approve

infra-destroy: ## Tear down all Terraform resources
	cd terraform && terraform destroy -auto-approve

# ── Prefect Blocks (run once) ──────────────────────────────────────────────

blocks: ## Create Prefect GCS & GCP credential blocks
	cd flows && python setup_blocks.py

# ── Pipeline Steps (individual) ─────────────────────────────────────────────

ingest: ## Step 1: Download CSV → clean → Parquet → GCS
	cd flows && python ingest_to_gcs.py

load-bq: ## Step 2: GCS → BigQuery raw table
	cd flows && python gcs_to_bq.py

dbt-run: ## Step 3: Run dbt transformations
	cd dbt && dbt run --profiles-dir .

# ── Full Pipeline ───────────────────────────────────────────────────────────

pipeline: ## Run full DAG (ingest → load → transform)
	cd flows && python pipeline_dag.py

# ── Docker Pipeline ─────────────────────────────────────────────────────────

docker-pipeline: ## Run full pipeline in Docker
	docker compose run --rm pipeline

docker-dashboard: ## Start Streamlit dashboard in Docker
	docker compose up dashboard

# ── Dashboard ───────────────────────────────────────────────────────────────

dashboard: ## Start Streamlit dashboard locally
	streamlit run dashboard/app.py

# ── Cleanup ─────────────────────────────────────────────────────────────────

clean: ## Remove local data files
	rm -rf data/
