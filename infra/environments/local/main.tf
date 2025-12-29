terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}

locals {
  kubeconfig_path = "~/.kube/config"
  context = "docker-desktop"
  namespace = "bigdata"
}

provider "kubernetes" {
  config_path    = local.kubeconfig_path
  config_context = local.context
}


resource "kubernetes_namespace_v1" "default" {
  metadata {
    name = local.namespace
  }
}

# Deploy Hadoop cluster
module "hadoop" {
  depends_on = [kubernetes_namespace_v1.default]
  source     = "../../modules/hadoop"
  namespace  = local.namespace
}

# Setup datasets, schemas, topics and connectors
module "bootstrap" {
  depends_on = [module.hadoop]
  source            = "../../modules/bootstrap"
  namespace         = local.namespace
  kubeconfig_path   = local.kubeconfig_path
  context           = local.context
}

# Deploy BigData services
module "bigdata" {
  depends_on = [module.hadoop, module.bootstrap]
  source     = "../../modules/bigdata"
  namespace  = local.namespace
}

# Deploy ETL
module "etl" {
  depends_on = [module.bigdata]
  source     = "../../modules/etl"
  namespace  = local.namespace
}