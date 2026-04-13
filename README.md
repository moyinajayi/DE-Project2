# Chicago Crime Data Pipeline

An end-to-end data pipeline (batch + streaming) that ingests, transforms, and visualizes Chicago crime data from the City of Chicago Open Data Portal.

## Problem Statement

Public safety is a top priority for cities, yet raw crime data is often too large and unstructured for citizens, journalists, or analysts to draw actionable insights from. The City of Chicago publishes over **8.5 million crime incident records** spanning 2001 to present, updated daily — but this data sits as a flat CSV/API with no analytical structure.

**This project solves that problem** by building an automated, end-to-end data pipeline that:

1. **Batch ingests** the full Chicago crime dataset into a cloud data lake (GCS)
2. **Streams** recent crime events via Kafka for near-real-time processing
3. **Loads** the data into a cloud data warehouse (BigQuery) with optimized partitioning and clustering
4. **Transforms** the raw records into analytics-ready tables using dbt (dimensional modeling)
5. **Visualizes** the results in an interactive Streamlit dashboard

The dashboard answers key questions:
- **What types of crime are most prevalent in Chicago?** — helping identify which categories (theft, battery, narcotics, etc.) need the most attention
- **How have crime trends changed over time?** — revealing seasonal patterns, year-over-year changes, and arrest rates across districts
- **What is the arrest rate by crime type?** — showing police effectiveness per category

The batch pipeline runs on a weekly schedule via Prefect, while the Kafka streaming pipeline can process recent events in near-real-time.

## Architecture

```
 ┌──────────────────── BATCH PATH ────────────────────────────────────┐
 │                                                                     │
 │   ┌────────────────┐    ┌────────────────┐    ┌────────────────┐   │
 │   │  Ingest CSV     │    │  GCS → BigQuery │    │  dbt Transform │   │
 │   │  → Parquet → GCS│───▶│  (raw table)    │───▶│  (fact/dim)    │   │
 │   └────────────────┘    └────────────────┘    └────────────────┘   │
 │           ▲                                            │            │
 │    Prefect DAG                                         ▼            │
 │    (weekly schedule)                           BigQuery Tables      │
 │                                              (analytics-ready)      │
 └─────────────────────────────────────────────────────────────────────┘

 ┌──────────────────── STREAMING PATH ────────────────────────────────┐
 │                                                                     │
 │   ┌────────────────┐    ┌────────────────┐    ┌────────────────┐   │
 │   │ Socrata API     │    │     Kafka       │    │  Consumer →    │   │
 │   │ → Producer      │───▶│  (topic)        │───▶│  BigQuery      │   │
 │   └────────────────┘    └────────────────┘    └────────────────┘   │
 └─────────────────────────────────────────────────────────────────────┘

                                    │
                                    ▼
                           ┌────────────────┐
                           │   Streamlit    │
                           │   Dashboard    │
                           └────────────────┘
```

## Tech Stack

| Component | Technology |
|---|---|
| Infrastructure as Code | Terraform |
| Data Lake | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Workflow Orchestration | Prefect |
| Stream Processing | Apache Kafka (Confluent) |
| Transformations | dbt |
| Dashboard | Streamlit |
| Containerization | Docker / Docker Compose |

## Dataset

**City of Chicago — Crimes (2001 to Present)**
- Source: [Chicago Open Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)
- **8.5 million+** records, updated daily
- Columns: `ID`, `Case Number`, `Date`, `Block`, `Primary Type`, `Description`, `Location Description`, `Arrest`, `Domestic`, `Beat`, `District`, `Ward`, `Community Area`, `Year`, `Latitude`, `Longitude`
- Socrata API supports both full CSV download and paginated JSON queries

## Dashboard

The Streamlit dashboard provides **6 visual elements** with interactive sidebar filters (crime type multi-select, year range slider).

### KPI Metrics (top row)
| Total Incidents | Total Arrests | Arrest Rate % | YoY Change % |
|---|---|---|---|
| Sum of all filtered crimes | Sum of arrests | Calculated: arrests ÷ incidents × 100 | Calculated: last full year vs. prior year |

### Charts

| # | Tile | Chart Type | Description |
|---|---|---|---|
| 1 | Crime Distribution by Type | Bar chart | Total incidents per crime category (e.g., THEFT, BATTERY, NARCOTICS) |
| 2 | Arrest Rate by Crime Type | Horizontal bar | Calculated field: arrest % per category — shows police effectiveness |
| 3 | Monthly Crime Trends | Line chart | Crime volume over time, broken down by selected crime types |
| 4 | Year-over-Year Crime Totals | Bar + Table | Annual totals with computed YoY % change and arrest rate % columns |
| 5 | Top Districts by Crime Volume | Bar chart | Top 15 districts ranked by incident count, with arrest rate in detail view |

## Data Warehouse Optimization

BigQuery tables use three optimization strategies: **partitioning**, **clustering**, and **search indexes**.

### Partitioning & Clustering

| Table | Partition | Cluster | Rationale |
|---|---|---|---|
| `chicago_crime_raw` | `date` (monthly) | `primary_type`, `district` | Raw table is partitioned by crime date and clustered by type/district so downstream queries (dbt, ad-hoc) scan fewer bytes. |
| `fact_crime_monthly` | `month_start` (monthly) | `crime_type`, `district` | Dashboard temporal chart filters by month range; categorical chart and district drill-downs filter/group by type and district. |
| `dim_crime_type_summary` | None (small table) | `crime_type` | Only ~35 rows — partitioning would add overhead. Clustering by crime_type aligns with the categorical bar chart query. |

### Search Indexes

BigQuery search indexes are created via dbt `post_hook` for fast text lookups:

| Table | Index | Column | Purpose |
|---|---|---|---|
| `fact_crime_monthly` | `idx_fact_crime_type` | `crime_type` | Fast filtered aggregations by crime type |
| `dim_crime_type_summary` | `idx_dim_crime_type` | `crime_type` | Fast lookups on crime type dimension |

This is configured in the dbt model configs (`partition_by`, `cluster_by`, and `post_hook`) rather than raw DDL, so the optimization is version-controlled and reproducible.

## Setup & Reproduction

A `Makefile` is provided for convenience. Run `make help` to see all available commands.

### Prerequisites

- **GCP account** with a project and billing enabled
- **Terraform** >= 1.0 installed ([install guide](https://developer.hashicorp.com/terraform/install))
- **Docker** & Docker Compose installed ([install guide](https://docs.docker.com/get-docker/))
- **Python** >= 3.10 (if running locally without Docker)
- **GCP service account key** (JSON) with the following roles:
  - BigQuery Admin
  - Storage Admin

### Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/moyinajayi/DE-Project2.git && cd DE-Project2
cp .env.example .env              # edit with your GCP project ID
mkdir -p creds
cp /path/to/service-account.json creds/service-account.json

# 2. Provision cloud infrastructure
make infra                        # Terraform: creates GCS bucket + BigQuery dataset

# 3. Install dependencies & configure Prefect blocks
make setup                        # pip install -r requirements.txt
make blocks                       # creates Prefect GCS/GCP blocks

# 4. Run the full batch pipeline
make pipeline                     # Ingest → Load → Transform

# 5. (Optional) Run streaming pipeline
make stream-up                    # Start Kafka + Zookeeper
make stream-produce               # Produce recent crime events to Kafka
make stream-consume               # Consume events → BigQuery

# 6. Launch the dashboard
make dashboard                    # opens http://localhost:8501
```

### Detailed Steps

#### Step 1: Infrastructure (Terraform)

```bash
cd terraform

cat > terraform.tfvars <<EOF
project          = "your-gcp-project-id"
gcs_bucket_name  = "your-unique-bucket-name"
EOF

terraform init
terraform plan
terraform apply
```

#### Step 2: Configure Environment

```bash
cp .env.example .env
# Edit .env with your GCP project ID

mkdir -p creds
cp /path/to/your/service-account.json creds/service-account.json
```

#### Step 3: Set Up Prefect Blocks

```bash
pip install -r requirements.txt
# Edit flows/setup_blocks.py with your bucket name and project ID
python flows/setup_blocks.py
```

#### Step 4: Run the Batch Pipeline

The full DAG runs 4 steps in order: **Ingest → Load → dbt Transform → dbt Test**

```bash
# Option A: Full DAG (recommended — runs all 4 steps)
make pipeline

# Option B: Docker
docker compose run --rm pipeline

# Option C: Individual steps locally
make ingest       # Step 1: Download CSV → Parquet → GCS
make load-bq      # Step 2: GCS → BigQuery (partitioned + clustered)
make dbt-run      # Step 3: dbt transformations (staging view + core tables)
make dbt-test     # Step 4: dbt tests (not_null, unique, data quality)
```

#### Step 5: Run the Streaming Pipeline

```bash
# Start Kafka infrastructure
make stream-up

# In separate terminals:
make stream-produce    # Fetches recent records from Socrata API → Kafka
make stream-consume    # Kafka → BigQuery streaming table
```

#### Step 6: Launch Dashboard

```bash
mkdir -p dashboard/.streamlit
cp dashboard/.streamlit/secrets.toml.example dashboard/.streamlit/secrets.toml
# Edit secrets.toml with your project ID and credentials path

# Docker:
docker compose up dashboard

# Or locally:
streamlit run dashboard/app.py
```

## Project Structure

```
├── terraform/              # IaC — GCS bucket + BigQuery dataset
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── flows/                  # Prefect batch orchestration
│   ├── pipeline_dag.py     # Parent DAG — chains all steps in order
│   ├── deploy.py           # Register scheduled deployment with Prefect
│   ├── ingest_to_gcs.py    # Step 1: Download CSV → clean → Parquet → GCS
│   ├── gcs_to_bq.py        # Step 2: GCS Parquet → BigQuery raw table
│   └── setup_blocks.py     # One-time Prefect block configuration
├── streaming/              # Kafka streaming pipeline
│   ├── producer.py         # Socrata API → Kafka topic
│   └── consumer.py         # Kafka topic → BigQuery
├── dbt/                    # Data transformations (dbt)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_chicago_crime.sql   # Staging view with type casting
│       │   └── schema.yml              # Source definition + column tests
│       └── core/
│           ├── fact_crime_monthly.sql   # Partitioned + clustered + indexed
│           ├── dim_crime_type_summary.sql  # Clustered + indexed
│           └── schema.yml              # Model docs + column tests
├── dashboard/              # Streamlit dashboard
│   ├── app.py
│   └── .streamlit/
│       └── secrets.toml.example
├── Dockerfile
├── docker-compose.yml
├── Makefile                # Quick commands: make pipeline, make stream-up, etc.
├── requirements.txt
├── .env.example            # Template for environment variables
└── README.md
```

## Evaluation Criteria Mapping

How each project component maps to the grading rubric:

| # | Criterion | What This Project Does | Evidence |
|---|---|---|---|
| 1 | **Problem description** (2 pts) | Clear problem statement with 3 specific questions the dashboard answers | README § Problem Statement |
| 2 | **Cloud** (4 pts) | Terraform provisions GCS bucket + BigQuery dataset; IaC is version-controlled | `terraform/main.tf`, `terraform/variables.tf` |
| 3 | **Data ingestion — batch/orchestration** (4 pts) | Prefect DAG with 4 chained steps (ingest → load → dbt transform → dbt test); weekly cron schedule via `deploy.py` | `flows/pipeline_dag.py`, `flows/deploy.py` |
| 4 | **Data ingestion — stream** (4 pts) | Kafka producer (Socrata API → topic) + consumer (topic → BigQuery); Kafka + Zookeeper in Docker Compose | `streaming/producer.py`, `streaming/consumer.py`, `docker-compose.yml` |
| 5 | **Data warehouse** (4 pts) | BigQuery raw table partitioned by date (monthly) + clustered by primary_type/district; dbt core tables with partitioning, clustering, and search indexes; documented rationale per table | `flows/gcs_to_bq.py`, `dbt/models/core/*.sql`, README § DWH Optimization |
| 6 | **Transformations** (4 pts) | dbt staging view + 2 core tables (fact + dim); 12 data quality tests (not_null, unique) in `schema.yml`; dbt test step in DAG | `dbt/models/`, `dbt/models/*/schema.yml` |
| 7 | **Dashboard** (4 pts) | 4 KPI metrics + 5 chart tiles (bar, horizontal bar, line, bar+table, bar); 2 calculated fields (arrest rate %, YoY % change); interactive filters | `dashboard/app.py` |
| 8 | **Reproducibility** (4 pts) | README with step-by-step instructions; Makefile with all commands; `.env.example` template; Docker Compose for all services; `.gitignore` excludes secrets/data | README, `Makefile`, `.env.example`, `docker-compose.yml` |