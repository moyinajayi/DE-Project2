output "bucket_name" {
  value       = google_storage_bucket.data_lake_bucket.name
  description = "Data Lake bucket name"
}

output "bigquery_dataset" {
  value       = google_bigquery_dataset.dataset.dataset_id
  description = "BigQuery dataset name"
}
