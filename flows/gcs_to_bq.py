"""
Prefect flow: Load Vancouver Crime Data from GCS (Data Lake) into BigQuery (Data Warehouse).
"""

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

GCS_PATH = "raw/vancouver_crime/vancouver_crime.parquet"
BQ_TABLE = "vancouver_crime.vancouver_crime_raw"


@task(retries=2, retry_delay_seconds=15, log_prints=True)
def extract_from_gcs(gcs_path: str) -> str:
    """Download parquet from GCS to a local staging path."""
    gcs_block = GcsBucket.load("vancouver-crime-gcs")
    local_path = gcs_block.download_object_to_path(from_path=gcs_path, to_path=f"data/{gcs_path.split('/')[-1]}")
    print(f"Downloaded from GCS: {local_path}")
    return str(local_path)


@task(log_prints=True)
def load_to_bigquery(local_path: str, destination_table: str) -> None:
    """Load a local Parquet file into a BigQuery table."""
    gcp_credentials = GcpCredentials.load("vancouver-crime-gcp-creds")
    client = bigquery.Client(credentials=gcp_credentials.get_credentials_from_service_account())

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(local_path, "rb") as f:
        job = client.load_table_from_file(f, destination_table, job_config=job_config)

    job.result()  # Wait for completion
    table = client.get_table(destination_table)
    print(f"Loaded {table.num_rows} rows into {destination_table}")


@flow(name="GCS to BigQuery - Vancouver Crime", log_prints=True)
def gcs_to_bq() -> None:
    """Main flow: GCS → BigQuery."""
    local_path = extract_from_gcs(GCS_PATH)
    load_to_bigquery(local_path, BQ_TABLE)


if __name__ == "__main__":
    gcs_to_bq()
