# terraform/main.tf

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
  
  backend "gcs" {
    bucket  = "bq2pg-terraform-state"
    prefix  = "prod"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# GKE Cluster
resource "google_container_cluster" "bq2pg" {
  name     = "bq2pg-cluster"
  location = var.region
  
  # Cluster config
  initial_node_count = 1
  
  node_config {
    preemptible  = false
    machine_type = "n1-standard-4"
    
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
  
  # Network config
  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name
  
  # Workload Identity
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
  
  # Network Policy
  network_policy {
    enabled = true
  }
  
  # Monitoring
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
    managed_prometheus {
      enabled = true
    }
  }
  
  # Logging
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }
  
  # Security
  master_auth {
    client_certificate_config {
      issue_client_certificate = false
    }
  }
  
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }
  
  addons_config {
    network_policy_config {
      disabled = false
    }
  }
  
  maintenance_policy {
    daily_maintenance_window {
      start_time = "03:00"
    }
  }
  
  enable_shielded_nodes = true
  
  depends_on = [
    google_project_service.container,
    google_project_service.compute
  ]
}

# Node Pool
resource "google_container_node_pool" "bq2pg_nodes" {
  name           = "bq2pg-node-pool"
  cluster        = google_container_cluster.bq2pg.name
  node_count     = var.node_count
  
  autoscaling {
    min_node_count = var.min_nodes
    max_node_count = var.max_nodes
  }
  
  node_config {
    preemptible  = var.use_preemptible_nodes
    machine_type = var.machine_type
    
    disk_size_gb = 100
    
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    
    metadata = {
      disable-legacy-endpoints = "true"
    }
    
    labels = {
      workload = "bq2pg"
      managed  = "terraform"
    }
    
    tags = ["bq2pg", "prod"]
  }
  
  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# VPC Network
resource "google_compute_network" "vpc" {
  name                    = "bq2pg-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "bq2pg-subnet"
  ip_cidr_range = "10.0.0.0/16"
  region        = var.region
  network       = google_compute_network.vpc.id
  
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.1.0.0/16"
  }
  
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.2.0.0/16"
  }
}

# Cloud SQL (PostgreSQL)
resource "google_sql_database_instance" "postgres" {
  name             = "bq2pg-postgres-${random_string.db_suffix.result}"
  database_version = "POSTGRES_15"
  region           = var.region
  
  settings {
    tier              = var.cloudsql_machine_type
    availability_type = "REGIONAL"
    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = 7
      backup_retention_settings {
        retained_backups = 30
        retention_unit   = "COUNT"
      }
    }
    
    ip_configuration {
      require_ssl = true
      
      authorized_networks {
        name  = "GKE Network"
        value = "10.0.0.0/16"
      }
    }
    
    database_flags {
      name  = "cloudsql_iam_authentication"
      value = "on"
    }
  }
  
  deletion_protection = true
}

resource "google_sql_database" "bq2pg" {
  name     = "bq2pg"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "postgres" {
  name     = "bq2pg"
  instance = google_sql_database_instance.postgres.name
  type     = "BUILT_IN"
  password = random_password.db_password.result
}

# BigQuery Dataset
resource "google_bigquery_dataset" "bq2pg" {
  dataset_id    = "bq2pg_${replace(var.environment, "-", "_")}"
  friendly_name = "BQ2PG Dataset - ${var.environment}"
  description   = "BigQuery dataset for BQ2PG pipeline"
  location      = var.bq_location
  
  access {
    role          = "OWNER"
    user_by_email = google_service_account.bq2pg.email
  }
  
  labels = {
    environment = var.environment
    managed     = "terraform"
  }
}

# Service Account
resource "google_service_account" "bq2pg" {
  account_id   = "bq2pg-pipeline"
  display_name = "BQ2PG Pipeline Service Account"
}

resource "google_project_iam_member" "bq2pg_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.bq2pg.email}"
}

resource "google_project_iam_member" "bq2pg_cloudsql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.bq2pg.email}"
}

# Workload Identity Binding
resource "google_service_account_iam_member" "bq2pg_workload_identity" {
  service_account_id = google_service_account.bq2pg.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[bq2pg/bq2pg]"
}

# Cloud Storage for logs and backups
resource "google_storage_bucket" "logs" {
  name          = "bq2pg-logs-${random_string.bucket_suffix.result}"
  location      = var.region
  force_destroy = false
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      num_newer_versions = 10
    }
    action {
      action = "Delete"
    }
  }
  
  labels = {
    environment = var.environment
    managed     = "terraform"
  }
}

# Random suffixes for unique resource names
resource "random_string" "db_suffix" {
  length  = 8
  special = false
  lower   = true
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  lower   = true
}

resource "random_password" "db_password" {
  length  = 32
  special = true
}

# Enable required APIs
resource "google_project_service" "container" {
  service            = "container.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudsql" {
  service            = "sqladmin.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

# Kubernetes Provider
provider "kubernetes" {
  host  = "https://${google_container_cluster.bq2pg.endpoint}"
  token = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(
    google_container_cluster.bq2pg.master_auth[0].cluster_ca_certificate,
  )
}

provider "helm" {
  kubernetes {
    host  = "https://${google_container_cluster.bq2pg.endpoint}"
    token = data.google_client_config.default.access_token
    cluster_ca_certificate = base64decode(
      google_container_cluster.bq2pg.master_auth[0].cluster_ca_certificate,
    )
  }
}

data "google_client_config" "default" {}
