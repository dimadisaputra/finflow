# FinFlow — Compute Module
# Airflow, Spark, FastAPI, and DuckDB.

variable "environment" {
  type        = string
  description = "Environment name (local, dev, prod)"
}

variable "airflow_image" {
  type        = string
  default     = "apache/airflow:2.9.1-python3.11"
  description = "Airflow Docker image — pinned version"
}

variable "spark_image" {
  type        = string
  default     = "apache/spark:3.5.1-python3"
  description = "Spark Docker image — pinned version"
}

variable "airflow_executor" {
  type        = string
  default     = "LocalExecutor"
  description = "Airflow executor type (LocalExecutor for dev, CeleryExecutor for prod)"
}

variable "spark_executor_memory" {
  type        = string
  default     = "2g"
  description = "Spark executor memory (2g for OCI free tier dev)"
}

# Placeholder — actual container definitions depend on deployment target
# (Docker Compose for local, OCI for dev/prod)

output "airflow_webserver_url" {
  value = "http://localhost:8080"
}

output "fastapi_url" {
  value = "http://localhost:8000"
}
