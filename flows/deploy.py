"""
Prefect Deployment: Schedule the Chicago Crime Pipeline DAG.
"""

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from pipeline_dag import chicago_crime_pipeline


def create_deployment():
    deployment = Deployment.build_from_flow(
        flow=chicago_crime_pipeline,
        name="chicago-crime-weekly",
        schedule=CronSchedule(cron="0 6 * * 1", timezone="America/Chicago"),
        work_queue_name="default",
        tags=["chicago-crime", "batch", "weekly"],
        description=(
            "Weekly batch pipeline: Ingest Chicago crime data -> "
            "GCS data lake -> BigQuery warehouse -> dbt transformations."
        ),
    )
    deployment.apply()
    print("Deployment created: chicago-crime-weekly")
    print("Schedule: Every Monday at 6:00 AM (America/Chicago)")


if __name__ == "__main__":
    create_deployment()
