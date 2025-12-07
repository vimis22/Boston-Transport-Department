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

# Deploy Hadoop cluster
module "hadoop" {
  depends_on = [kubernetes_namespace.default]
  source = "../../modules/hadoop"
  namespace = local.namespace
}

# Publish schemas to Schema Registry
resource "null_resource" "publish_schemas" {
  depends_on = [module.hadoop]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-schemas.py"
  }
}

# Deploy BigData services
module "bigdata" {
  source = "../../modules/bigdata"
  namespace = local.namespace
  depends_on = [null_resource.publish_schemas]
}