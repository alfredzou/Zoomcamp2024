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

resource "google_compute_address" "default" {
  name   = "batch-static-ip"
}

resource "google_service_account" "default" {
  account_id   = "batch-sa"
  display_name = "batch service account"
}

resource "google_compute_instance" "default" {
  name         = "batch-vm"
  machine_type = "e2-standard-4"

  boot_disk {
    initialize_params {
      image = "ubuntu-2004-focal-v20240110"
      size = 100
    }
  }


  network_interface {
    network = "default"

    access_config {
      nat_ip = google_compute_address.default.address
    }
  }

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = google_service_account.default.email
    scopes = ["cloud-platform"]
  }

}

