variable "project" {
  description = "Project"
  default     = "peppy-caster-484310-c1"
}

variable "region" {
  description = "My Project region"
  default     = "europe-central2"
}

variable "location" {
  description = "My Project location"
  default     = "EUROPE-CENTRAL2"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket name"
  default     = "peppy-caster-484310-c1-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}
