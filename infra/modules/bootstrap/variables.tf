variable "namespace" {
  type        = string
  description = "The namespace to deploy into"
}

variable "kubeconfig_path" {
  type        = string
  description = "Path to the kubeconfig file"
  default     = ""
}

variable "context" {
  type        = string
  description = "The kubernetes context to use"
  default     = ""
}

