terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

locals {
  namespace = "bigdata"
}

# This module will deploy the bigdata services(time manager, data streamers, kafka connectors, dashboard, etc.)