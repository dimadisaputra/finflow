# FinFlow — Observability Module
# Prometheus, Grafana, Loki, and Marquez (OpenLineage).

variable "environment" {
  type        = string
  description = "Environment name (local, dev, prod)"
}

variable "prometheus_image" {
  type        = string
  default     = "prom/prometheus:v2.51.1"
  description = "Prometheus Docker image — pinned version"
}

variable "grafana_image" {
  type        = string
  default     = "grafana/grafana:10.4.1"
  description = "Grafana Docker image — pinned version"
}

variable "loki_image" {
  type        = string
  default     = "grafana/loki:2.9.6"
  description = "Loki Docker image — pinned version"
}

variable "marquez_image" {
  type        = string
  default     = "marquezproject/marquez:0.47.0"
  description = "Marquez Docker image — pinned version"
}

# Placeholder — actual container definitions depend on deployment target

output "prometheus_url" {
  value = "http://localhost:9090"
}

output "grafana_url" {
  value = "http://localhost:3000"
}

output "marquez_url" {
  value = "http://localhost:5000"
}
