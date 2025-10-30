terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

locals {
  spark_master_port    = 7077
  spark_master_ui_port = 8080
  spark_master_url     = "spark://spark-primary.${var.namespace}.svc.cluster.local:${local.spark_master_port}"
  spark_master_ui_url  = "http://spark-primary.${var.namespace}.svc.cluster.local:${local.spark_master_ui_port}"
}

resource "kubernetes_config_map" "spark_master_config" {
  metadata {
    name      = "spark-primary-config"
    namespace = var.namespace
  }

  data = {
    "spark-defaults.conf" = <<-EOF
      spark.master                          ${local.spark_master_url}
      spark.eventLog.enabled                true
      spark.eventLog.dir                    ${var.hdfs_url}/spark-logs
      spark.serializer                      org.apache.spark.serializer.KryoSerializer
      spark.hadoop.fs.defaultFS             ${var.hdfs_url}
      spark.hadoop.dfs.nameservices         ${var.namespace}
      spark.ui.enabled                      true
      spark.ui.port                         8080
      spark.rpc.askTimeout                  120
      spark.worker.timeout                  180
    EOF

    "core-site.xml" = <<-EOF
      <?xml version="1.0" encoding="UTF-8"?>
      <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
      <configuration>
        <property>
          <name>fs.defaultFS</name>
          <value>${var.hdfs_url}</value>
        </property>
        <property>
          <name>hadoop.tmp.dir</name>
          <value>/tmp/hadoop</value>
        </property>
        <property>
          <name>fs.hdfs.impl</name>
          <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
        </property>
      </configuration>
    EOF

    "hdfs-site.xml" = <<-EOF
      <?xml version="1.0" encoding="UTF-8"?>
      <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
      <configuration>
        <property>
          <name>dfs.replication</name>
          <value>1</value>
        </property>
        <property>
          <name>dfs.blocksize</name>
          <value>134217728</value>
        </property>
      </configuration>
    EOF
  }
}

resource "kubernetes_stateful_set" "spark_master" {
  metadata {
    name      = "spark-primary"
    namespace = var.namespace
  }

  spec {
    service_name = "spark-primary"
    replicas     = 1

    selector {
      match_labels = {
        app = "spark-primary"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-primary"
        }
      }

      spec {
        container {
          name  = "spark-primary"
          image = "apache/spark:4.0.1"

          command = ["/bin/bash", "-c"]
          args = [
            <<EOF
# Start the master
/opt/spark/sbin/start-master.sh --port 7077 --webui-port 8080
EOF
          ]

          port {
            container_port = 7077
            name           = "master"
          }

          port {
            container_port = 8080
            name           = "webui"
          }

          volume_mount {
            name       = "spark-primary-config"
            mount_path = "/opt/spark/conf"
          }

          resources {
            requests = {
              memory = "1Gi"
              cpu    = "500m"
            }
            limits = {
              memory = "2Gi"
              cpu    = "1000m"
            }
          }

          env {
            name  = "SPARK_LOCAL_DIRS"
            value = "/tmp/spark"
          }

          env {
            name  = "SPARK_NO_DAEMONIZE"
            value = "true"
          }

          env {
            name  = "SPARK_MASTER_PORT"
            value = "7077"
          }
        }

        volume {
          name = "spark-primary-config"
          config_map {
            name = kubernetes_config_map.spark_master_config.metadata[0].name
          }
        }
      }
    }

    update_strategy {
      type = "RollingUpdate"
    }

    pod_management_policy = "Parallel"

  }

  depends_on = [kubernetes_config_map.spark_master_config]
}

resource "kubernetes_service" "spark_master" {
  metadata {
    name      = "spark-primary"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "spark-primary"
    }

    port {
      name        = "primary"
      port        = local.spark_master_port
      target_port = local.spark_master_port
    }

    port {
      name        = "webui"
      port        = local.spark_master_ui_port
      target_port = local.spark_master_ui_port
    }

    type = "NodePort"
  }
}

resource "kubernetes_config_map" "spark_worker_config" {
  metadata {
    name      = "spark-worker-config"
    namespace = var.namespace
  }

  data = {
    "spark-defaults.conf" = <<EOF
spark.master                          ${local.spark_master_url}
spark.eventLog.enabled                true
spark.eventLog.dir                    ${var.hdfs_url}/spark-logs
spark.serializer                      org.apache.spark.serializer.KryoSerializer
spark.sql.warehouse.dir               ${var.hdfs_url}/spark-warehouse
spark.hadoop.hive.metastore.uris      ${var.hive_url}
spark.sql.hive.convertMetastoreOrc    false
spark.hadoop.validateOutputSpecs      false
spark.sql.adaptive.enabled            true
spark.sql.adaptive.coalescePartitions.enabled true
spark.executor.memory                 1g
spark.executor.cores                  1
spark.cores.max                       2
spark.dynamicAllocation.enabled       false
spark.hadoop.fs.defaultFS             ${var.hdfs_url}
spark.hadoop.dfs.nameservices         bigdata
EOF

    "spark-env.sh" = <<EOF
export SPARK_WORKER_CORES=1
export SPARK_WORKER_MEMORY=1g
export SPARK_EXECUTOR_MEMORY=1g
export SPARK_EXECUTOR_CORES=1
export SPARK_LOCAL_IP=\$(hostname)
export PYSPARK_PYTHON=python3
export HADOOP_CONF_DIR=/opt/spark/conf
export HIVE_CONF_DIR=/opt/spark/conf
EOF

    "core-site.xml" = <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${var.hdfs_url}</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop</value>
  </property>
  <property>
    <name>fs.hdfs.impl</name>
    <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
  </property>
</configuration>
EOF

    "hdfs-site.xml" = <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value>
  </property>
</configuration>
EOF
  }
}

resource "kubernetes_deployment" "spark_worker" {
  metadata {
    name      = "spark-worker"
    namespace = var.namespace
  }

  spec {
    replicas = 2

    selector {
      match_labels = {
        app = "spark-worker"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-worker"
        }
      }

      spec {
        container {
          name  = "spark-worker"
          image = "apache/spark:4.0.1"

          command = ["/bin/bash", "-c"]
          args = ["/opt/spark/sbin/start-worker.sh ${local.spark_master_url}"]

          port {
            container_port = 8081
            name           = "webui"
          }

          port {
            container_port = 10000
            name           = "shuffle"
          }

          port {
            container_port = 10001
            name           = "blockmanager"
          }

          volume_mount {
            name       = "spark-worker-config"
            mount_path = "/opt/spark/conf"
          }

          env {
            name  = "SPARK_LOCAL_DIRS"
            value = "/tmp/spark"
          }

          env {
            name  = "SPARK_WORKER_WEBUI_PORT"
            value = "8081"
          }

          env {
            name  = "SPARK_NO_DAEMONIZE"
            value = "true"
          }

          resources {
            requests = {
              memory = "1Gi"
              cpu    = "500m"
            }
            limits = {
              memory = "2Gi"
              cpu    = "1000m"
            }
          }
        }

        volume {
          name = "spark-worker-config"
          config_map {
            name = kubernetes_config_map.spark_worker_config.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "spark_worker" {
  metadata {
    name      = "spark-worker"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "spark-worker"
    }

    port {
      name        = "webui"
      port        = 8081
      target_port = 8081
    }

    cluster_ip = null
    type       = "ClusterIP"
  }

  depends_on = [kubernetes_deployment.spark_worker]
}