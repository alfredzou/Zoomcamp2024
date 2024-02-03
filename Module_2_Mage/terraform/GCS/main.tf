terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.14.0"
    }
  }
}

provider "google" {
  project = var.project_id
  credentials = file(var.credentials)
  region  = var.region
  zone    = var.zone
}

resource "google_storage_bucket" "static-site" {
  name          = "${var.project_id}-bucket"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
  public_access_prevention = "enforced"
}
