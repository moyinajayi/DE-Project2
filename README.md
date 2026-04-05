# Vancouver Crime Data Pipeline

An end-to-end batch data pipeline that ingests, transforms, and visualizes Vancouver Police Department crime data.

## Problem Statement

Public safety is a top priority for cities, yet raw crime data published by police departments is often too large and unstructured for citizens, journalists, or city planners to draw actionable insights from. The Vancouver Police Department publishes over **650,000 crime incident records** spanning multiple years, but this data sits as a flat CSV with no analytical structure.

**This project solves that problem** by building an automated, end-to-end batch data pipeline that:

1. **Ingests** the raw VPD crime data from Vancouver Open Data into a cloud data lake (GCS)
2. **Loads** the data into a cloud data warehouse (BigQuery) with optimized partitioning and clustering
3. **Transforms** the raw records into analytics-ready tables using dbt (dimensional modeling)
4. **Visualizes** the results in an interactive Streamlit dashboard

The dashboard answers two key questions:
- **What types of crime are most prevalent in Vancouver?** — helping identify which categories (theft, break & enter, mischief, etc.) need the most attention
- **How have crime trends changed over time?** — revealing seasonal patterns, year-over-year changes, and whether crime is increasing or decreasing across neighbourhoods

The pipeline runs on a weekly schedule via Prefect, ensuring the dashboard always reflects the latest available data.

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │      Prefect DAG (pipeline_dag.py)  │
                    │         Scheduled: Weekly           │
                    └─────────────┬───────────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
     ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
     │  Step 1: Ingest │  │  Step 2: Load  │  │  Step 3: dbt   │
     │  CSV → Parquet  │  │  GCS → BigQuery│  │  Transform     │
     │  → GCS          │  │  (raw table)   │  │  (fact/dim)    │
     └────────────────┘  └────────────────┘  └────────────────┘
              │                   │                   │
              ▼                   ▼                   ▼
         GCS Bucket         BigQuery Raw        BigQuery Tables
        (Data Lake)          Table            (Analytics-Ready)
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
| Transformations | dbt |
| Dashboard | Streamlit |
| Containerization | Docker / Docker Compose |

## Dataset

**Vancouver Police Department Crime Data**
- Source: [Vancouver Open Data](https://data.vancouver.ca/)
- ~650,000+ records of crime incidents
- Columns: `TYPE`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `HUNDRED_BLOCK`, `NEIGHBOURHOOD`, `X`, `Y`
- Updated regularly with recent crime reports

## Dashboard

Two tiles as required:

1. **Crime Distribution by Type** — Bar chart showing total incidents per crime category (e.g., Theft from Vehicle, Break and Enter, Mischief)
2. **Monthly Crime Trends** — Line chart showing crime volume over time, broken down by crime type

Interactive filters: crime type selection, year range slider.

## Data Warehouse Optimization

BigQuery tables are **partitioned and clustered** for query performance and cost:

| Table | Partition | Cluster | Rationale |
|---|---|---|---|
| `fact_crime_monthly` | `month_start` (monthly) | `crime_type`, `neighbourhood` | Dashboard temporal chart filters by month range; categorical chart and neighbourhood drill-downs filter/group by type and neighbourhood. Partitioning reduces scanned data for time-range queries; clustering enables block-level pruning on the most-filtered columns. |
| `dim_crime_type_summary` | None (small table) | `crime_type` | Only ~15 rows — partitioning would add overhead. Clustering by crime_type aligns with the categorical bar chart query. |

This is configured in the dbt model configs (`partition_by` and `cluster_by`) rather than raw DDL, so the optimization is version-controlled and reproducible.

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

### Quick Start (5 commands)

```bash
# 1. Clone and configure
git clone <your-repo-url> && cd DE-Project2
cp .env.example .env              # edit with your GCP project ID
mkdir -p creds
cp /path/to/service-account.json creds/service-account.json

# 2. Provision cloud infrastructure
make infra                        # Terraform: creates GCS bucket + BigQuery dataset

# 3. Install dependencies & configure Prefect blocks
make setup                        # pip install -r requirements.txt
make blocks                       # creates Prefect GCS/GCP blocks (edit setup_blocks.py first)

# 4. Run the full pipeline
make pipeline                     # Ingest → Load → Transform (or: make docker-pipeline)

# 5. Launch the dashboard
make dashboard                    # opens http://localhost:8501
```

### Detailed Steps

### Step 1: Infrastructure (Terraform)

```bash
cd terraform

# Create a terraform.tfvars file:
cat > terraform.tfvars <<EOF
project          = "your-gcp-project-id"
gcs_bucket_name  = "your-unique-bucket-name"
EOF

terraform init
terraform plan
terraform apply
```

### Step 2: Configure Environment

```bash
# Copy and edit environment variables
cp .env.example .env
# Edit .env with your GCP project ID

# Place your GCP service account key at:
mkdir -p creds
cp /path/to/your/service-account.json creds/service-account.json
```

### Step 3: Set Up Prefect Blocks

```bash
pip install -r requirements.txt

# Edit flows/setup_blocks.py with your bucket name and project ID
python flows/setup_blocks.py
```

### Step 4: Run the Pipeline

```bash
# Option A: Full DAG — runs all 3 steps in order (recommended)
docker compose run pipeline

# Option B: Run individual steps separately
docker compose run ingest      # Step 1: Download data → GCS
docker compose run load-bq     # Step 2: GCS → BigQuery
docker compose run dbt         # Step 3: dbt transformations

# Option C: Run locally
python flows/pipeline_dag.py   # Full DAG
# ... or individual steps:
python flows/ingest_to_gcs.py
python flows/gcs_to_bq.py
cd dbt && dbt run --profiles-dir .
```

### Step 4b (Optional): Schedule the Pipeline

```bash
# Register a weekly deployment with Prefect (runs Mondays at 6 AM Vancouver time)
python flows/deploy.py

# Start a Prefect agent to pick up scheduled runs
prefect agent start -q default
```

### Step 5: Launch Dashboard

```bash
# Create Streamlit secrets
mkdir -p dashboard/.streamlit
cp dashboard/.streamlit/secrets.toml.example dashboard/.streamlit/secrets.toml
# Edit secrets.toml with your project ID and credentials path

# Option A: Docker Compose
docker compose up dashboard
# Open http://localhost:8501

# Option B: Run locally
streamlit run dashboard/app.py
```

## Project Structure

```
├── terraform/              # IaC — GCS bucket + BigQuery dataset
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── flows/                  # Prefect orchestration
│   ├── pipeline_dag.py     # Parent DAG — chains all steps in order
│   ├── deploy.py           # Register scheduled deployment with Prefect
│   ├── ingest_to_gcs.py    # Step 1: Download CSV → clean → Parquet → GCS
│   ├── gcs_to_bq.py        # Step 2: GCS Parquet → BigQuery raw table
│   └── setup_blocks.py     # One-time Prefect block configuration
├── dbt/                    # Data transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_vancouver_crime.sql
│       │   └── schema.yml
│       └── core/
│           ├── fact_crime_monthly.sql
│           ├── dim_crime_type_summary.sql
│           └── schema.yml
├── dashboard/              # Streamlit dashboard
│   ├── app.py
│   └── .streamlit/
│       └── secrets.toml.example
├── Dockerfile
├── docker-compose.yml
├── Makefile                # Quick commands: make pipeline, make dashboard, etc.
├── requirements.txt
└── README.md
```
```