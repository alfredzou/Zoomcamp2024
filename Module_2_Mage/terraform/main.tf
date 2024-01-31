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
  name   = "postgres-mage-static-ip"
  region = var.region
}

resource "google_service_account" "default" {
  account_id   = "postgres-mage-sa"
  display_name = "postgres and mage service account"
}

resource "google_compute_instance" "default" {
  name         = "postgres-mage-vm"
  machine_type = "e2-standard-4"

  boot_disk {
    initialize_params {
      image = "ubuntu-2004-focal-v20240110"
      size = 20
    }
  }


  network_interface {
    network = "default"

    access_config {
      nat_ip = google_compute_address.default.address
    }
  }

  metadata = {
    account = var.vm_account
  }

  metadata_startup_script = "${file("init.sh")}"

  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    email  = google_service_account.default.email
    scopes = ["cloud-platform"]
  }
}