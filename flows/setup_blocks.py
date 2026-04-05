"""
One-time script: Create Prefect blocks for GCS and GCP credentials.

Run this ONCE before executing the flows:
    python flows/setup_blocks.py

Prerequisites:
    - Set env var GOOGLE_APPLICATION_CREDENTIALS to your service account JSON key path
    - Update BUCKET_NAME and PROJECT_ID below to match your Terraform outputs
"""

import os

from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# ── Update these to match your environment ──────────────────────────────────
BUCKET_NAME = "vancouver-crime-data-project-95cf2209"  # from terraform output
PROJECT_ID = "project-95cf2209-f1e7-4545-a0c"
CREDENTIALS_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "..", "creds", "gcpserviceacccountcreds.json")
)
# ────────────────────────────────────────────────────────────────────────────


def create_gcp_credentials_block():
    with open(os.path.expanduser(CREDENTIALS_PATH), "r") as f:
        service_account_info = f.read()

    gcp_credentials = GcpCredentials(
        service_account_info=service_account_info,
        project=PROJECT_ID,
    )
    gcp_credentials.save("vancouver-crime-gcp-creds", overwrite=True)
    print("✓ GcpCredentials block saved: vancouver-crime-gcp-creds")


def create_gcs_bucket_block():
    gcp_credentials = GcpCredentials.load("vancouver-crime-gcp-creds")

    gcs_bucket = GcsBucket(
        bucket=BUCKET_NAME,
        gcp_credentials=gcp_credentials,
    )
    gcs_bucket.save("vancouver-crime-gcs", overwrite=True)
    print("✓ GcsBucket block saved: vancouver-crime-gcs")


if __name__ == "__main__":
    create_gcp_credentials_block()
    create_gcs_bucket_block()
    print("\nDone! You can now run the ingestion flows.")
