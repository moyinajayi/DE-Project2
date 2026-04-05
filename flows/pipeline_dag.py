"""
Prefect DAG: End-to-end Vancouver Crime Data Pipeline.

This is the parent orchestration flow (DAG) that chains all pipeline steps
in the correct order:

    1. Ingest CSV → clean → Parquet → GCS  (data lake)
    2. GCS Parquet → BigQuery raw table     (data warehouse)
    3. dbt run → transformed tables         (analytics-ready)

Scheduled to run weekly on Mondays at 6:00 AM UTC.
"""

import subprocess

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from ingest_to_gcs import ingest_to_gcs
from gcs_to_bq import gcs_to_bq


@task(log_prints=True)
def run_dbt_transformations() -> None:
    """Run dbt models to build transformed tables in BigQuery."""
    print("Running dbt transformations ...")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd="/app/dbt",
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")
    print("dbt transformations complete.")


@flow(name="Vancouver Crime Pipeline DAG", log_prints=True)
def vancouver_crime_pipeline():
    """
    Full DAG orchestrating the end-to-end pipeline:

        [Ingest to GCS] → [Load to BigQuery] → [dbt Transform]

    Each step depends on the previous one completing successfully.
    """

    # Step 1: Download → Clean → Upload to GCS
    print("=" * 60)
    print("STEP 1: Ingesting data to GCS (Data Lake)")
    print("=" * 60)
    ingest_to_gcs()

    # Step 2: GCS → BigQuery raw table
    print("=" * 60)
    print("STEP 2: Loading data from GCS to BigQuery (Warehouse)")
    print("=" * 60)
    gcs_to_bq()

    # Step 3: dbt transformations
    print("=" * 60)
    print("STEP 3: Running dbt transformations")
    print("=" * 60)
    run_dbt_transformations()

    print("=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    vancouver_crime_pipeline()
