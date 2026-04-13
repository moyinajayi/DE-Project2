"""
Prefect flow: Download Chicago Crime Data CSV (batch) and upload to GCS (Data Lake).

Uses the City of Chicago Socrata Open Data API to download the full dataset.
The dataset has 8M+ rows (2001-present).
"""

from pathlib import Path

import pandas as pd
import requests
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

# Socrata CSV export — full dataset (2001-present)
DATA_URL = "https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"
LOCAL_DIR = Path("data")

KEEP_COLUMNS = [
    "ID", "Case Number", "Date", "Block", "Primary Type", "Description",
    "Location Description", "Arrest", "Domestic", "Beat", "District",
    "Ward", "Community Area", "Year", "Latitude", "Longitude",
]


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def download_crime_data(url: str) -> Path:
    """Download Chicago crime CSV to local disk."""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    local_path = LOCAL_DIR / "chicago_crime_raw.csv"

    print(f"Downloading data from {url} ...")
    response = requests.get(url, timeout=600, stream=True)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    size_mb = local_path.stat().st_size / 1e6
    print(f"Downloaded {local_path} ({size_mb:.1f} MB)")
    return local_path


@task(log_prints=True)
def clean_and_convert_to_parquet(csv_path: Path) -> Path:
    """Read CSV, clean, and save as Parquet."""
    print("Reading CSV ...")
    df = pd.read_csv(csv_path, usecols=KEEP_COLUMNS, low_memory=False)

    # Standardize column names: lowercase + underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Parse Date column
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce")

    # Drop rows with no date or crime type
    df = df.dropna(subset=["date", "primary_type"])

    # Normalize booleans
    for col in ["arrest", "domestic"]:
        df[col] = df[col].astype(str).str.lower().map({"true": True, "false": False})

    # Ensure numeric types
    for col in ["district", "ward", "community_area", "year"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    parquet_path = LOCAL_DIR / "chicago_crime.parquet"
    df.to_parquet(parquet_path, engine="pyarrow", index=False)
    print(f"Saved Parquet: {parquet_path} ({len(df):,} rows)")
    return parquet_path


@task(log_prints=True)
def upload_to_gcs(local_path: Path, gcs_path: str) -> None:
    """Upload a local file to GCS using the configured Prefect GCS block."""
    gcs_block = GcsBucket.load("chicago-crime-gcs")
    gcs_block.upload_from_path(from_path=local_path, to_path=gcs_path)
    print(f"Uploaded {local_path} -> gs://.../{gcs_path}")


@flow(name="Ingest Chicago Crime Data to GCS", log_prints=True)
def ingest_to_gcs() -> None:
    """Main flow: download -> clean -> upload to GCS."""
    csv_path = download_crime_data(DATA_URL)
    parquet_path = clean_and_convert_to_parquet(csv_path)
    upload_to_gcs(parquet_path, "raw/chicago_crime/chicago_crime.parquet")


if __name__ == "__main__":
    ingest_to_gcs()
