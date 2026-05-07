# FinFlow — Local Environment
# Docker Compose-based deployment for local development.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

module "storage" {
  source      = "../../modules/storage"
  environment = "local"
}

module "streaming" {
  source      = "../../modules/streaming"
  environment = "local"
}

module "security" {
  source      = "../../modules/security"
  environment = "local"
}
