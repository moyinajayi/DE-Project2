"""One-time helper: convert already-downloaded CSV to Parquet and upload to GCS."""
import sys
sys.path.insert(0, ".")
from pathlib import Path
from ingest_to_gcs import clean_and_convert_to_parquet, upload_to_gcs

csv_path = Path(__file__).resolve().parent.parent / "data" / "chicago_crime_raw.csv"
parquet_path = clean_and_convert_to_parquet.fn(csv_path)
upload_to_gcs.fn(parquet_path, "raw/chicago_crime/chicago_crime.parquet")
print("Done!")
