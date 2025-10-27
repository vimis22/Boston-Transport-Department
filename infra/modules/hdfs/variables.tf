variable "namespace" {
  type = string
  description = "The namespace to deploy the hdfs to"
  default = "bigdata"
}

variable "master_pvc_size" {
  type = string
  description = "The size of the pvc to deploy the hdfs master to"
  default = "10Gi"
}

variable "master_pvc_storage_class" {
  type = string
  description = "The storage class of the pvc to deploy the hdfs master to"
  default = "hostpath"
}

variable "datanode_replicas" {
  type = number
  description = "The number of datanode replicas to deploy"
  default = 1
}

variable "datanode_pvc_size" {
  type = string
  description = "The size of the pvc to deploy the hdfs datanode to"
  default = "10Gi"
}

variable "datanode_pvc_storage_class" {
  type = string
  description = "The storage class of the pvc to deploy the hdfs datanode to"
  default = "hostpath"
}
