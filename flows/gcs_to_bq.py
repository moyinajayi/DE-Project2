"""
Prefect flow: Load Chicago Crime Data from GCS (Data Lake) into BigQuery (Data Warehouse).
"""

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

GCS_PATH = "raw/chicago_crime/chicago_crime.parquet"
BQ_TABLE = "chicago_crime.chicago_crime_raw"


@task(retries=2, retry_delay_seconds=15, log_prints=True)
def extract_from_gcs(gcs_path: str) -> str:
    """Download parquet from GCS to a local staging path."""
    gcs_block = GcsBucket.load("chicago-crime-gcs")
    local_path = gcs_block.download_object_to_path(
        from_path=gcs_path,
        to_path=f"data/{gcs_path.split('/')[-1]}"
    )
    print(f"Downloaded from GCS: {local_path}")
    return str(local_path)


@task(log_prints=True)
def load_to_bigquery(local_path: str, destination_table: str) -> None:
    """Load a local Parquet file into a BigQuery table with partitioning + clustering."""
    import pandas as pd

    gcp_credentials = GcpCredentials.load("chicago-crime-gcp-creds")
    client = bigquery.Client(
        credentials=gcp_credentials.get_credentials_from_service_account()
    )

    # Read parquet and ensure date column is proper TIMESTAMP (not INT64)
    df = pd.read_parquet(local_path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Cast float columns to nullable Int64 so BQ sees INTEGER (not FLOAT)
    for col in ["district", "ward", "community_area", "year", "beat"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="date",
        ),
        clustering_fields=["primary_type", "district"],
    )

    job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
    job.result()
    table = client.get_table(destination_table)
    print(f"Loaded {table.num_rows:,} rows into {destination_table}")
    print(f"  Partitioned by: date (MONTH)")
    print(f"  Clustered by: primary_type, district")


@flow(name="GCS to BigQuery - Chicago Crime", log_prints=True)
def gcs_to_bq() -> None:
    """Main flow: GCS -> BigQuery."""
    local_path = extract_from_gcs(GCS_PATH)
    load_to_bigquery(local_path, BQ_TABLE)


if __name__ == "__main__":
    gcs_to_bq()
