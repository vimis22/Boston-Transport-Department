variable "namespace" {  
  type = string
  description = "The namespace to deploy the jupyter to"
  default = "bigdata"
}

variable "hdfs_url" {
  type = string
  description = "The HDFS filesystem URI"
}

variable "spark_master_url" {
  type = string
  description = "The Spark Master URL"
}

variable "hive_url" {
  type = string
  description = "The Hive metastore URI"
}