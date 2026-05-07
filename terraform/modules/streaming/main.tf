# FinFlow — Streaming Module
# Redpanda broker and Schema Registry.

variable "environment" {
  type        = string
  description = "Environment name (local, dev, prod)"
}

variable "redpanda_image" {
  type        = string
  default     = "docker.redpanda.com/redpandadata/redpanda:v24.1.1"
  description = "Redpanda Docker image — always pinned, never latest (AGENTS.md rule #7)"
}

variable "redpanda_smp" {
  type        = number
  default     = 1
  description = "Number of cores for Redpanda (1 for dev, more for prod)"
}

variable "redpanda_memory" {
  type        = string
  default     = "1G"
  description = "Memory limit for Redpanda (1G for dev)"
}

# Redpanda container
resource "docker_container" "redpanda" {
  name  = "finflow-redpanda-${var.environment}"
  image = var.redpanda_image

  command = [
    "redpanda", "start",
    "--smp", tostring(var.redpanda_smp),
    "--memory", var.redpanda_memory,
    "--overprovisioned",
    "--kafka-addr", "internal://0.0.0.0:9092,external://0.0.0.0:19092",
    "--advertise-kafka-addr", "internal://finflow-redpanda-${var.environment}:9092,external://localhost:19092",
    "--pandaproxy-addr", "internal://0.0.0.0:8082,external://0.0.0.0:18082",
    "--advertise-pandaproxy-addr", "internal://finflow-redpanda-${var.environment}:8082,external://localhost:18082",
    "--schema-registry-addr", "internal://0.0.0.0:8081,external://0.0.0.0:18081",
  ]

  ports {
    internal = 9092
    external = 9092
  }
  ports {
    internal = 19092
    external = 19092
  }
  ports {
    internal = 8081
    external = 8081
  }
  ports {
    internal = 8082
    external = 8082
  }

  volumes {
    host_path      = "/var/lib/finflow/redpanda"
    container_path = "/var/lib/redpanda/data"
  }
}

output "bootstrap_servers" {
  value = "finflow-redpanda-${var.environment}:9092"
}

output "schema_registry_url" {
  value = "http://finflow-redpanda-${var.environment}:8081"
}
