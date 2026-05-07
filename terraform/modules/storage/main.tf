# FinFlow — Storage Module
# MinIO (self-hosted S3), Iceberg REST catalog, and bucket definitions.

variable "environment" {
  type        = string
  description = "Environment name (local, dev, prod)"
}

variable "minio_image" {
  type        = string
  default     = "minio/minio:RELEASE.2024-03-15T01-07-19Z"
  description = "MinIO Docker image — always pinned, never latest (AGENTS.md rule #7)"
}

variable "iceberg_rest_image" {
  type        = string
  default     = "tabulario/iceberg-rest:1.5.0"
  description = "Iceberg REST catalog Docker image"
}

variable "minio_mode" {
  type        = string
  default     = "standalone"
  description = "MinIO deployment mode (standalone for dev, distributed for prod)"
}

# MinIO container
resource "docker_container" "minio" {
  name  = "finflow-minio-${var.environment}"
  image = var.minio_image

  command = ["server", "/data", "--console-address", ":9001"]

  ports {
    internal = 9000
    external = 9000
  }
  ports {
    internal = 9001
    external = 9001
  }

  env = [
    "MINIO_ROOT_USER=minioadmin",
    "MINIO_ROOT_PASSWORD=minioadmin",
  ]

  volumes {
    host_path      = "/var/lib/finflow/minio"
    container_path = "/data"
  }
}

# Iceberg REST catalog
resource "docker_container" "iceberg_rest" {
  name  = "finflow-iceberg-rest-${var.environment}"
  image = var.iceberg_rest_image

  ports {
    internal = 8181
    external = 8181
  }

  env = [
    "CATALOG_WAREHOUSE=s3a://finflow-silver/",
    "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
    "CATALOG_S3_ENDPOINT=http://finflow-minio-${var.environment}:9000",
    "AWS_ACCESS_KEY_ID=minioadmin",
    "AWS_SECRET_ACCESS_KEY=minioadmin",
    "AWS_REGION=us-east-1",
  ]

  depends_on = [docker_container.minio]
}

# MinIO buckets are created via mc commands in the entrypoint
# See docker-compose.yml for bucket initialization
output "minio_endpoint" {
  value = "http://finflow-minio-${var.environment}:9000"
}

output "iceberg_rest_uri" {
  value = "http://finflow-iceberg-rest-${var.environment}:8181"
}
