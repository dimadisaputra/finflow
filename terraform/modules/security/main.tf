# FinFlow — Security Module
# HashiCorp Vault, OPA, and TLS configuration.

variable "environment" {
  type        = string
  description = "Environment name (local, dev, prod)"
}

variable "vault_image" {
  type        = string
  default     = "hashicorp/vault:1.16.1"
  description = "Vault Docker image — pinned version"
}

# Vault container
resource "docker_container" "vault" {
  name  = "finflow-vault-${var.environment}"
  image = var.vault_image

  command = ["server", "-dev"]

  ports {
    internal = 8200
    external = 8200
  }

  env = [
    "VAULT_DEV_ROOT_TOKEN_ID=finflow-dev-token",
    "VAULT_DEV_LISTEN_ADDRESS=0.0.0.0:8200",
  ]

  capabilities {
    add = ["IPC_LOCK"]
  }
}

output "vault_addr" {
  value = "http://finflow-vault-${var.environment}:8200"
}
