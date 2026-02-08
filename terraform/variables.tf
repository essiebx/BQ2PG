# terraform/variables.tf

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "node_count" {
  description = "Initial number of nodes in node pool"
  type        = number
  default     = 3
}

variable "min_nodes" {
  description = "Minimum number of nodes in node pool"
  type        = number
  default     = 3
}

variable "max_nodes" {
  description = "Maximum number of nodes in node pool"
  type        = number
  default     = 10
}

variable "use_preemptible_nodes" {
  description = "Use preemptible nodes to reduce cost"
  type        = bool
  default     = false
}

variable "machine_type" {
  description = "Machine type for nodes"
  type        = string
  default     = "n1-standard-4"
}

variable "cloudsql_machine_type" {
  description = "Cloud SQL machine tier"
  type        = string
  default     = "db-custom-4-16384"  # 4 vCPU, 16GB RAM
}

variable "enable_monitoring" {
  description = "Enable Cloud Monitoring and Logging"
  type        = bool
  default     = true
}

variable "enable_workload_identity" {
  description = "Enable Workload Identity for pod authentication"
  type        = bool
  default     = true
}
