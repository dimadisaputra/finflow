# FinFlow — Redpanda Topic Definitions
# When adding a new transaction source, add the topic here AND
# to the subscribe list in streaming/silver_writer.py.

variable "bootstrap_servers" {
  type        = string
  description = "Redpanda bootstrap servers"
}

# Transaction topics — one per source
resource "redpanda_topic" "transactions_bca" {
  name               = "finflow.transactions.bca"
  partition_count     = 3
  replication_factor  = 1
  config = {
    "retention.ms" = "604800000"  # 7 days
    "cleanup.policy" = "delete"
  }
}

resource "redpanda_topic" "transactions_mandiri" {
  name               = "finflow.transactions.mandiri"
  partition_count     = 3
  replication_factor  = 1
  config = {
    "retention.ms" = "604800000"
    "cleanup.policy" = "delete"
  }
}

resource "redpanda_topic" "transactions_gopay" {
  name               = "finflow.transactions.gopay"
  partition_count     = 3
  replication_factor  = 1
  config = {
    "retention.ms" = "604800000"
    "cleanup.policy" = "delete"
  }
}

resource "redpanda_topic" "transactions_ovo" {
  name               = "finflow.transactions.ovo"
  partition_count     = 3
  replication_factor  = 1
  config = {
    "retention.ms" = "604800000"
    "cleanup.policy" = "delete"
  }
}

resource "redpanda_topic" "transactions_visa" {
  name               = "finflow.transactions.visa"
  partition_count     = 3
  replication_factor  = 1
  config = {
    "retention.ms" = "604800000"
    "cleanup.policy" = "delete"
  }
}

# Fraud alerts topic
resource "redpanda_topic" "fraud_alerts" {
  name               = "finflow.fraud.alerts"
  partition_count     = 3
  replication_factor  = 1
  config = {
    "retention.ms" = "604800000"
    "cleanup.policy" = "delete"
  }
}
