locals {
  kubeconfig_flag = var.kubeconfig_path != "" ? "--kubeconfig ${pathexpand(var.kubeconfig_path)}" : ""
  context_flag    = var.context != "" ? "--context ${var.context}" : ""
  namespace_flag  = "--namespace ${var.namespace}"

  common_args = "${local.namespace_flag} ${local.kubeconfig_flag} ${local.context_flag}"
}

# Create datasets
resource "terraform_data" "create_datasets" {
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-datasets.py ${local.common_args}"
  }
}

# Publish schemas to Schema Registry
resource "terraform_data" "publish_schemas" {
  depends_on = [terraform_data.create_datasets]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-schemas.py ${local.common_args}"
  }
}

# Publish topics to Kafka
resource "terraform_data" "publish_topics" {
  depends_on = [terraform_data.publish_schemas]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-topics.py ${local.common_args}"
  }
}

# Create Kafka Connectors
resource "terraform_data" "create_connectors" {
  depends_on = [terraform_data.publish_topics]
  provisioner "local-exec" {
    command = "uv run ../../../tools/create-connectors.py ${local.common_args}"
  }
}

output "publish_schemas_id" {
  value = terraform_data.publish_schemas.id
}

output "publish_topics_id" {
  value = terraform_data.publish_topics.id
}

