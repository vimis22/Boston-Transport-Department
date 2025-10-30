variable "namespace" {
  type = string
  description = "The namespace to deploy the hive to"
  default = "bigdata"
}

variable "default_fs_url" {
  type = string
  description = "The default HDFS filesystem URI"
}