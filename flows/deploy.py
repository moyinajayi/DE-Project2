"""
Prefect Deployment: Schedule and deploy the Vancouver Crime Pipeline DAG.

This creates a scheduled deployment that runs the full pipeline weekly.
Run this once to register the deployment with the Prefect server:

    python flows/deploy.py

The DAG will then run on the configured schedule automatically
(requires a Prefect agent/worker running).
"""

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from pipeline_dag import vancouver_crime_pipeline


def create_deployment():
    deployment = Deployment.build_from_flow(
        flow=vancouver_crime_pipeline,
        name="vancouver-crime-weekly",
        schedule=CronSchedule(cron="0 6 * * 1", timezone="America/Vancouver"),
        work_queue_name="default",
        tags=["vancouver-crime", "batch", "weekly"],
        description=(
            "Weekly batch pipeline: Ingest Vancouver crime data → "
            "GCS data lake → BigQuery warehouse → dbt transformations."
        ),
    )
    deployment.apply()
    print("Deployment created: vancouver-crime-weekly")
    print("Schedule: Every Monday at 6:00 AM (America/Vancouver)")
    print()
    print("To start processing, run a Prefect agent:")
    print("  prefect agent start -q default")


if __name__ == "__main__":
    create_deployment()
