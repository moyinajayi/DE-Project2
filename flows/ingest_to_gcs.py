"""
Prefect flow: Download Vancouver Crime Data CSV and upload to GCS (Data Lake).
"""

import os
from pathlib import Path

import pandas as pd
import requests
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

DATA_URL = "https://geodata.vancouver.ca/datasets/crimedata_csv_all_years.csv"
LOCAL_DIR = Path("data")


@task(retries=3, retry_delay_seconds=30, log_prints=True)
def download_crime_data(url: str) -> Path:
    """Download Vancouver crime data CSV to local disk."""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    local_path = LOCAL_DIR / "vancouver_crime_raw.csv"

    print(f"Downloading data from {url} ...")
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    local_path.write_bytes(response.content)
    print(f"Downloaded {local_path} ({local_path.stat().st_size / 1e6:.1f} MB)")
    return local_path


@task(log_prints=True)
def clean_and_convert_to_parquet(csv_path: Path) -> Path:
    """Read CSV, perform light cleaning, and save as Parquet for efficient storage."""
    print("Reading CSV ...")
    df = pd.read_csv(csv_path)

    # Standardize column names
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    # Drop rows missing critical fields
    df = df.dropna(subset=["TYPE", "YEAR", "MONTH"])

    # Ensure integer types
    for col in ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    parquet_path = LOCAL_DIR / "vancouver_crime.parquet"
    df.to_parquet(parquet_path, engine="pyarrow", index=False)
    print(f"Saved Parquet: {parquet_path} ({len(df)} rows)")
    return parquet_path


@task(log_prints=True)
def upload_to_gcs(local_path: Path, gcs_path: str) -> None:
    """Upload a local file to GCS using the configured Prefect GCS block."""
    gcs_block = GcsBucket.load("vancouver-crime-gcs")
    gcs_block.upload_from_path(from_path=local_path, to_path=gcs_path)
    print(f"Uploaded {local_path} -> gs://.../{gcs_path}")


@flow(name="Ingest Vancouver Crime Data to GCS", log_prints=True)
def ingest_to_gcs() -> None:
    """Main flow: download → clean → upload to GCS."""
    csv_path = download_crime_data(DATA_URL)
    parquet_path = clean_and_convert_to_parquet(csv_path)
    upload_to_gcs(parquet_path, "raw/vancouver_crime/vancouver_crime.parquet")


if __name__ == "__main__":
    ingest_to_gcs()
