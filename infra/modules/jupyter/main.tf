terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

locals {
  jupyter_port = 8888
}

resource "kubernetes_config_map" "jupyter_config" {
  metadata {
    name      = "jupyter-config"
    namespace = var.namespace
  }

  data = {
    "spark-defaults.conf" = <<-EOF
      spark.master                          ${var.spark_master_url}
      spark.eventLog.enabled                true
      spark.eventLog.dir                    ${var.hdfs_url}/spark-logs
      spark.serializer                      org.apache.spark.serializer.KryoSerializer
      spark.hadoop.fs.defaultFS             ${var.hdfs_url}
      spark.sql.warehouse.dir               ${var.hdfs_url}/spark-warehouse
      spark.hadoop.hive.metastore.uris      ${var.hive_url}
      spark.sql.hive.convertMetastoreOrc    false
      spark.hadoop.validateOutputSpecs      false
      spark.sql.adaptive.enabled            true
      spark.sql.adaptive.coalescePartitions.enabled true
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
  }
}

resource "kubernetes_deployment" "jupyter" {
  metadata {
    name      = "jupyter"
    namespace = var.namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "jupyter"
      }
    }

    template {
      metadata {
        labels = {
          app = "jupyter"
        }
      }

      spec {
        container {
          name  = "jupyter"
          image = "quay.io/jupyter/all-spark-notebook:latest"

          port {
            container_port = local.jupyter_port
            name           = "notebook"
          }

          env {
            name  = "JUPYTER_ENABLE_LAB"
            value = "yes"
          }

          env {
            name  = "GRANT_SUDO"
            value = "1"
          }

          volume_mount {
            name       = "jupyter-config"
            mount_path = "/home/jovyan/.spark/conf"
          }

          volume_mount {
            name       = "hdfs-config"
            mount_path = "/home/jovyan/.hadoop/conf"
          }

          working_dir = "/home/jovyan"

          command = [
            "start-notebook.sh",
            "--ip=0.0.0.0",
            "--port=8888",
            "--no-browser",
            "--allow-root",
            "--NotebookApp.token=''",
            "--NotebookApp.password=''"
          ]

          resources {
            requests = {
              memory = "2Gi"
              cpu    = "1000m"
            }
            limits = {
              memory = "4Gi"
              cpu    = "2000m"
            }
          }

          liveness_probe {
            http_get {
              path = "/"
              port = local.jupyter_port
            }
            initial_delay_seconds = 120
            period_seconds        = 30
          }

          readiness_probe {
            http_get {
              path = "/"
              port = local.jupyter_port
            }
            initial_delay_seconds = 60
            period_seconds        = 10
          }
        }

        volume {
          name = "jupyter-config"
          config_map {
            name = kubernetes_config_map.jupyter_config.metadata[0].name
          }
        }

        volume {
          name = "hdfs-config"
          config_map {
            name = kubernetes_config_map.jupyter_config.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [kubernetes_config_map.jupyter_config]
}

resource "kubernetes_service" "jupyter" {
  metadata {
    name      = "jupyter"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "jupyter"
    }

    port {
      name        = "notebook"
      port        = local.jupyter_port
      target_port = local.jupyter_port
      protocol    = "TCP"
    }

    type = "NodePort"
  }

  depends_on = [kubernetes_deployment.jupyter]
}
