"""
One-time script: Create Prefect blocks for GCS and GCP credentials.

Run this ONCE before executing the flows:
    python flows/setup_blocks.py
"""

import os

from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
CREDENTIALS_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "..", "creds", "gcpserviceacccountcreds.json"),
)
if not BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME environment variable must be set")
if not PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID environment variable must be set")


def create_gcp_credentials_block():
    with open(os.path.expanduser(CREDENTIALS_PATH), "r") as f:
        service_account_info = f.read()

    gcp_credentials = GcpCredentials(
        service_account_info=service_account_info,
        project=PROJECT_ID,
    )
    gcp_credentials.save("chicago-crime-gcp-creds", overwrite=True)
    print("GcpCredentials block saved: chicago-crime-gcp-creds")


def create_gcs_bucket_block():
    gcp_credentials = GcpCredentials.load("chicago-crime-gcp-creds")

    gcs_bucket = GcsBucket(
        bucket=BUCKET_NAME,
        gcp_credentials=gcp_credentials,
    )
    gcs_bucket.save("chicago-crime-gcs", overwrite=True)
    print("GcsBucket block saved: chicago-crime-gcs")


if __name__ == "__main__":
    create_gcp_credentials_block()
    create_gcs_bucket_block()
    print("Done! You can now run the ingestion flows.")
