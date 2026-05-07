# FinFlow — Dev Environment (OCI Free Tier)
# Respects OCI free tier constraints per AGENTS.md.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 5.0"
    }
  }
}

provider "oci" {
  region = var.region
}

variable "region" {
  type    = string
  default = "ap-singapore-1"
}

variable "compartment_id" {
  type        = string
  description = "OCI compartment OCID"
}

module "networking" {
  source         = "../../modules/networking"
  compartment_id = var.compartment_id
  environment    = "dev"
}

module "storage" {
  source      = "../../modules/storage"
  environment = "dev"
  minio_mode  = "standalone"  # No erasure coding — not HA on free tier
}

module "streaming" {
  source          = "../../modules/streaming"
  environment     = "dev"
  redpanda_smp    = 1       # Single core on free tier
  redpanda_memory = "1G"    # 1G memory limit on free tier
}

module "compute" {
  source                = "../../modules/compute"
  environment           = "dev"
  airflow_executor      = "LocalExecutor"   # No Redis/Celery on free tier
  spark_executor_memory = "2g"              # Limited RAM on Ampere A1
}
