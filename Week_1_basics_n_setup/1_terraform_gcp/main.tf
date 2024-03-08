terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.19.0"
    }
  }
}

provider "google" {
  #   credentials = file(var.credentials)

  project = var.project
  region  = var.region
  zone    = var.zone
}

# resource for creating a gcs bucket
resource "google_storage_bucket" "hw-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# resource for a creating a bugquery dataset
resource "google_bigquery_dataset" "hw-dataset" {
  dataset_id                  = var.bq_dataset_name
  friendly_name               = "Homework Dataset"
  description                 = ""
  location                    = var.location
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }
}
