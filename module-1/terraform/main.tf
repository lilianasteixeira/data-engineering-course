terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file("/Users/lilianateixeira/Downloads/my-creds.json")
  project     = "peppy-caster-484310-c1"
  region      = "us-central1"
}