FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY flows/ flows/
COPY dbt/ dbt/
COPY dashboard/ dashboard/

# Default: run the full pipeline
CMD ["python", "flows/ingest_to_gcs.py"]
