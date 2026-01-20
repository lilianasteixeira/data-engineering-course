terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file("/Users/lilianateixeira/Downloads/my-creds.json")
  project     = "peppy-caster-484310-c1"
  region      = "europe-central2"
}

resource "google_storage_bucket" "auto-expire" {
  name          = "peppy-caster-484310-c1-terra-bucket"
  location      = "EUROPE-CENTRAL2"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}