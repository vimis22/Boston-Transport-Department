terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}

provider "kubernetes" {
  config_path = "~/.kube/config"
  config_context = "docker-desktop"
}

provider "helm" {
  kubernetes = provider.kubernetes
}

resource "kubernetes_namespace" "default" {
  metadata {
    name = "bigdata"
  }
}

module "hdfs" {
  source = "../../modules/hdfs"
  namespace = kubernetes_namespace.default.metadata[0].name
  master_pvc_size = "10Gi"
  master_pvc_storage_class = "hostpath"
  datanode_replicas = 3
  datanode_pvc_size = "10Gi"
  datanode_pvc_storage_class = "hostpath"
}

module "hive" {
  source = "../../modules/hive"
  namespace = kubernetes_namespace.default.metadata[0].name
  default_fs_url = module.hdfs.default_fs
}

module "spark" {
  source = "../../modules/spark"
  namespace = kubernetes_namespace.default.metadata[0].name
  hdfs_url = module.hdfs.default_fs
  hive_url = module.hive.hive_server
}

module "jupyter" {
  source = "../../modules/jupyter"
  namespace = kubernetes_namespace.default.metadata[0].name
  hdfs_url = module.hdfs.default_fs
  spark_master_url = module.spark.spark_master_url
  hive_url = module.hive.hive_server
}

output "hdfs_ui" {
  description = "The HDFS UI URI"
  value       = module.hdfs.default_ui
}

output "hdfs_server" {
  description = "The HDFS Server URI"
  value       = module.hdfs.default_fs
}

output "hive_server" {
  description = "The Hive Server URI"
  value       = module.hive.hive_server
}

output "hive_ui" {
  description = "The Hive UI URI"
  value       = module.hive.hive_ui
}

output "spark_master_url" {
  description = "The Spark Master URL"
  value       = module.spark.spark_master_url
}

output "spark_master_ui_url" {
  description = "The Spark Master UI URL"
  value       = module.spark.spark_master_ui_url
}

output "jupyter_url" {
  description = "The Jupyter URL"
  value       = module.jupyter.jupyter_url
}