terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
  required_providers {
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = "1.19.0"
    }
  }
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = "docker-desktop"
}

provider "helm" {
  kubernetes = {
    config_path    = "~/.kube/config"
    config_context = "docker-desktop"
  }
}

provider "kubectl" {
  config_path    = "~/.kube/config"
  config_context = "docker-desktop"
}

locals {
  namespace = "bigdata"
}


resource "kubernetes_namespace" "default" {
  metadata {
    name = local.namespace
  }
}

# Deploy Operators
module "operators" {
  depends_on = [kubernetes_namespace.default]
  source     = "../../modules/operators"
  namespace  = local.namespace
}

# Deploy Hadoop cluster
module "hadoop" {
  depends_on = [module.operators]
  source     = "../../modules/hadoop"
  namespace  = local.namespace
}

module "kafka" {
  source    = "../../modules/kafka"
  namespace = local.namespace

  depends_on = [
    module.hadoop
  ]
}

# Publish schemas to Schema Registry
resource "null_resource" "publish_schemas" {
  depends_on = [module.kafka]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-schemas.py"
  }
}

# Deploy BigData services
module "bigdata" {
  depends_on = [module.hadoop, null_resource.publish_schemas]
  source     = "../../modules/bigdata"
  namespace  = local.namespace
}

# Deploy ETL
module "etl" {
  depends_on = [module.bigdata]
  source     = "../../modules/etl"
  namespace  = local.namespace
}
