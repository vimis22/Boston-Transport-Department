output "jupyter_url" {
  description = "The Jupyter URL"
  value       = "http://${kubernetes_service.jupyter.metadata[0].name}.${var.namespace}.svc.cluster.local:${local.jupyter_port}"
}