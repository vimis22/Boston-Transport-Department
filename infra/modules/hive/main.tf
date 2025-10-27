terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

locals {
    hive_server_port = 10000
    hive_server_ui_port = 10002
}

resource "kubernetes_config_map" "hive" {
  metadata {
    name      = "hive-server-config"
    namespace = var.namespace
  }

  data = {
    "core-site.xml" = <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${var.default_fs_url}</value>
    <final>false</final>
  </property>
</configuration>
EOF

    "hive-site.xml" = <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>hive.execution.engine</name>
    <value>mr</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value> 
  </property>
</configuration>
EOF

    "mapred-site.xml" = <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
  <property>
    <name>mapreduce.jobtracker.address</name>
    <value>local</value>
  </property>
</configuration>
EOF
  }
}

resource "kubernetes_deployment" "hive_server" {
  metadata {
    name      = "hive-server"
    namespace = var.namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "hive-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "hive-server"
        }
      }

      spec {
        container {
          name  = "hive-server"
          image = "apache/hive:4.1.0"

          port {
            container_port = local.hive_server_port
            name           = "hiveserver2"
          }

          port {
            container_port = local.hive_server_ui_port
            name           = "ui"
          }

          volume_mount {
            name       = "hive-config"
            mount_path = "/opt/hive/conf"
          }

          env {
            name  = "SERVICE_NAME"
            value = "hiveserver2"
          }

          liveness_probe {
            tcp_socket {
              port = local.hive_server_port
            }
            initial_delay_seconds = 60
            period_seconds        = 10
          }

          readiness_probe {
            tcp_socket {
              port = local.hive_server_port
            }
            initial_delay_seconds = 60
            period_seconds        = 5
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
          name = "hive-config"
          config_map {
            name = kubernetes_config_map.hive.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "hive_server" {
  metadata {
    name      = "hive-server-service"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "hive-server"
    }

    cluster_ip = "None"

    port {
      name        = "hiveserver2"
      port        = local.hive_server_port
    }

    port {
      name        = "ui"
      port        = local.hive_server_ui_port
    }
  }
}

