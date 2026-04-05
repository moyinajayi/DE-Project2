variable "project" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  default     = "us-central1"
  type        = string
}

variable "location" {
  description = "Project location"
  default     = "US"
  type        = string
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default     = "vancouver_crime"
  type        = string
}

variable "gcs_bucket_name" {
  description = "Storage bucket name for the data lake. Must be globally unique."
  type        = string
}

variable "gcs_storage_class" {
  description = "Storage class for the bucket"
  default     = "STANDARD"
  type        = string
}
