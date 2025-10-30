variable "namespace" {
  type = string
  description = "The namespace to deploy the spark to"
  default = "bigdata"
}

variable "hdfs_url" {
  type = string
  description = "The HDFS filesystem URI"
}

variable "hive_url" {
  type = string
  description = "The Hive metastore URI"
}