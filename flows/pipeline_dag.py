"""
Prefect DAG: End-to-end Chicago Crime Data Pipeline.

Chains all pipeline steps in order:
    1. Ingest CSV -> clean -> Parquet -> GCS  (data lake)
    2. GCS Parquet -> BigQuery raw table      (data warehouse)
    3. dbt run -> transformed tables          (analytics-ready)
"""

import os
import subprocess
from pathlib import Path

from prefect import flow, task

from ingest_to_gcs import ingest_to_gcs
from gcs_to_bq import gcs_to_bq


def _resolve_dbt_dir() -> str:
    """Resolve dbt project directory — works in Docker (/app/dbt) and locally."""
    if Path("/app/dbt").exists():
        return "/app/dbt"
    # Local: dbt dir is sibling of flows/
    local_dbt = Path(__file__).resolve().parent.parent / "dbt"
    if local_dbt.exists():
        return str(local_dbt)
    raise FileNotFoundError("Cannot find dbt project directory")


@task(log_prints=True)
def run_dbt_transformations() -> None:
    """Run dbt models to build transformed tables in BigQuery."""
    dbt_dir = _resolve_dbt_dir()
    print(f"Running dbt transformations from {dbt_dir} ...")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")
    print("dbt transformations complete.")


@task(log_prints=True)
def run_dbt_tests() -> None:
    """Run dbt tests to validate transformed data."""
    dbt_dir = _resolve_dbt_dir()
    print(f"Running dbt tests from {dbt_dir} ...")
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt test failed with exit code {result.returncode}")
    print("dbt tests passed.")


@flow(name="Chicago Crime Pipeline DAG", log_prints=True)
def chicago_crime_pipeline():
    """
    Full DAG orchestrating the end-to-end pipeline:
        [Ingest to GCS] -> [Load to BigQuery] -> [dbt Transform]
    """

    print("=" * 60)
    print("STEP 1: Ingesting data to GCS (Data Lake)")
    print("=" * 60)
    ingest_to_gcs()

    print("=" * 60)
    print("STEP 2: Loading data from GCS to BigQuery (Warehouse)")
    print("=" * 60)
    gcs_to_bq()

    print("=" * 60)
    print("STEP 3: Running dbt transformations")
    print("=" * 60)
    run_dbt_transformations()

    print("=" * 60)
    print("STEP 4: Running dbt tests")
    print("=" * 60)
    run_dbt_tests()

    print("=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    chicago_crime_pipeline()
