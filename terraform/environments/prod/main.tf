# FinFlow — Production Environment
# Full deployment on OCI with HA configuration.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket   = "finflow-terraform-state"
    key      = "prod/terraform.tfstate"
    region   = "ap-singapore-1"
    endpoint = "https://NAMESPACE.compat.objectstorage.ap-singapore-1.oraclecloud.com"
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
  environment    = "prod"
}

module "storage" {
  source      = "../../modules/storage"
  environment = "prod"
  minio_mode  = "distributed"
}

module "streaming" {
  source          = "../../modules/streaming"
  environment     = "prod"
  redpanda_smp    = 4
  redpanda_memory = "8G"
}

module "compute" {
  source                = "../../modules/compute"
  environment           = "prod"
  airflow_executor      = "CeleryExecutor"
  spark_executor_memory = "8g"
}

module "security" {
  source      = "../../modules/security"
  environment = "prod"
}

module "observability" {
  source      = "../../modules/observability"
  environment = "prod"
}
