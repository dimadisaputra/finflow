# FinFlow — Networking Module
# VCN, subnets, security lists, and internet gateway.

terraform {
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 5.0"
    }
  }
}

variable "compartment_id" {
  type        = string
  description = "OCI compartment OCID"
}

variable "vcn_cidr_block" {
  type        = string
  default     = "10.0.0.0/16"
  description = "CIDR block for the VCN"
}

variable "environment" {
  type        = string
  description = "Environment name (local, dev, prod)"
}

# VCN
resource "oci_core_vcn" "finflow" {
  compartment_id = var.compartment_id
  cidr_blocks    = [var.vcn_cidr_block]
  display_name   = "finflow-${var.environment}-vcn"
  dns_label      = "finflow${var.environment}"
}

# Public subnet
resource "oci_core_subnet" "public" {
  compartment_id = var.compartment_id
  vcn_id         = oci_core_vcn.finflow.id
  cidr_block     = cidrsubnet(var.vcn_cidr_block, 8, 1)
  display_name   = "finflow-${var.environment}-public"
  dns_label      = "public"
}

# Private subnet
resource "oci_core_subnet" "private" {
  compartment_id             = var.compartment_id
  vcn_id                     = oci_core_vcn.finflow.id
  cidr_block                 = cidrsubnet(var.vcn_cidr_block, 8, 2)
  display_name               = "finflow-${var.environment}-private"
  dns_label                  = "private"
  prohibit_public_ip_on_vnic = true
}

output "vcn_id" {
  value = oci_core_vcn.finflow.id
}

output "public_subnet_id" {
  value = oci_core_subnet.public.id
}

output "private_subnet_id" {
  value = oci_core_subnet.private.id
}
