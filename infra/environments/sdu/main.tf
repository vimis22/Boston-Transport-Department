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

# Create datasets
resource "terraform_data" "create_datasets" {
  depends_on = [module.hadoop]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-datasets.py --namespace ${local.namespace} --kubeconfig ${pathexpand(local.kubeconfig_path)} --context ${local.context}"
  }
}

# Publish schemas to Schema Registry
resource "terraform_data" "publish_schemas" {
  depends_on = [module.hadoop]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-schemas.py --namespace ${local.namespace} --kubeconfig ${pathexpand(local.kubeconfig_path)} --context ${local.context}"
  }
}

# Publish topics to Kafka
resource "terraform_data" "publish_topics" {
  depends_on = [module.hadoop]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-topics.py --namespace ${local.namespace} --kubeconfig ${pathexpand(local.kubeconfig_path)} --context ${local.context}"
  }
}

# Create Kafka Connectors
resource "terraform_data" "create_connectors" {
  depends_on = [module.hadoop, terraform_data.publish_topics]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-connectors.py --namespace ${local.namespace} --kubeconfig ${pathexpand(local.kubeconfig_path)} --context ${local.context}"
  }
}

# Deploy BigData services
module "bigdata" {
  depends_on = [module.hadoop, terraform_data.publish_schemas, terraform_data.publish_topics]
  source     = "../../modules/bigdata"
  namespace  = local.namespace
}

# Deploy ETL
module "etl" {
  depends_on = [module.bigdata]
  source     = "../../modules/etl"
  namespace  = local.namespace
}