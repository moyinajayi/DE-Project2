"""One-time helper: load local parquet to BigQuery."""
import sys
sys.path.insert(0, ".")
from gcs_to_bq import load_to_bigquery

# The parquet is already local from the convert step
load_to_bigquery.fn("data/chicago_crime.parquet", "chicago_crime.chicago_crime_raw")
print("BQ load done!")
