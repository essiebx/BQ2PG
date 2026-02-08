# terraform/outputs.tf

output "kubernetes_cluster_name" {
  description = "GKE Cluster Name"
  value       = google_container_cluster.bq2pg.name
}

output "kubernetes_cluster_host" {
  description = "GKE Cluster Host"
  value       = google_container_cluster.bq2pg.endpoint
  sensitive   = true
}

output "region" {
  description = "GCP region"
  value       = var.region
}

output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "sql_instance_connection_name" {
  description = "Cloud SQL Instance Connection Name"
  value       = google_sql_database_instance.postgres.connection_name
}

output "sql_instance_private_ip" {
  description = "Cloud SQL Instance Private IP"
  value       = google_sql_database_instance.postgres.private_ip_address
}

output "database_password" {
  description = "PostgreSQL Database Password"
  value       = random_password.db_password.result
  sensitive   = true
}

output "bigquery_dataset_id" {
  description = "BigQuery Dataset ID"
  value       = google_bigquery_dataset.bq2pg.dataset_id
}

output "service_account_email" {
  description = "BQ2PG Service Account Email"
  value       = google_service_account.bq2pg.email
}

output "logs_bucket_name" {
  description = "Cloud Storage Bucket for Logs"
  value       = google_storage_bucket.logs.name
}

output "gke_config_command" {
  description = "Command to configure kubectl"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.bq2pg.name} --region ${var.region} --project ${var.project_id}"
}
