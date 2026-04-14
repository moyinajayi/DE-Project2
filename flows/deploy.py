"""
Prefect Deployment: Schedule the Chicago Crime Pipeline DAG.
"""

from flows.pipeline_dag import chicago_crime_pipeline


def create_deployment():
    chicago_crime_pipeline.serve(
        name="chicago-crime-weekly",
        cron="0 6 * * 1",
        tags=["chicago-crime", "batch", "weekly"],
        description=(
            "Weekly batch pipeline: Ingest Chicago crime data -> "
            "GCS data lake -> BigQuery warehouse -> dbt transformations."
        ),
    )


if __name__ == "__main__":
    print("Starting deployment: chicago-crime-weekly")
    print("Schedule: Every Monday at 6:00 AM UTC")
    create_deployment()
