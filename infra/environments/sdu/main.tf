terraform {
  backend "kubernetes" {
    secret_suffix    = "state"
    config_path      = "~/Downloads/bd-stud-magre21-sa-bd-bd-stud-magre21-kubeconfig.yaml"
    namespace        = "bd-bd-stud-magre21"
  }
}

locals {
  kubeconfig_path = "~/Downloads/bd-stud-magre21-sa-bd-bd-stud-magre21-kubeconfig.yaml"
  context = "bd-bd-stud-magre21-context"
  namespace = "bd-bd-stud-magre21"
}

provider "kubernetes" {
  config_path    = local.kubeconfig_path
  config_context = local.context
}

# Deploy Hadoop cluster
module "hadoop" {
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