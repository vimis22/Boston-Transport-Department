output "hive_server" {
  description = "The Hive Server URI"
  value       = "hive2://hive-server-service.${var.namespace}.svc.cluster.local:${local.hive_server_port}"
}

output "hive_ui" {
  description = "The Hive UI URI"
  value       = "http://hive-server-service.${var.namespace}.svc.cluster.local:${local.hive_server_ui_port}"
}

