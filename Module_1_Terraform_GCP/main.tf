terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

# alternatively don't provide credentials run:
# export GOOGLE_CREDENTIALS='key location'

provider "google" {
  project     = var.Project
  credentials = file(var.credentials)
  region      = "australia-southeast1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
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

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location = "australia-southeast1"
}