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

module "hadoop" {
  source = "../../modules/hadoop"
  namespace = local.namespace
}

