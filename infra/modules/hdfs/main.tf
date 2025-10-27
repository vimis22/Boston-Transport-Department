terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

locals {
    hdfs_port = 9000
    hdfs_ui_port = 9870
    default_fs = "hdfs://namenode.${var.namespace}.svc.cluster.local:${local.hdfs_port}"
    default_ui = "http://namenode.${var.namespace}.svc.cluster.local:${local.hdfs_ui_port}"
}

resource "kubernetes_config_map" "hdfs" {
  metadata {
    name      = "hdfs-config"
    namespace = var.namespace
  }

  data = {
    "core-site.xml" = <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${local.default_fs}</value>
  </property>
</configuration>
EOF

    "hdfs-site.xml" = <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>  <!-- Set to 2+ with more DataNodes -->
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///hadoop/dfs/data</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>  <!-- Simplified for dev -->
  </property>
</configuration>
EOF
  }
}


resource "kubernetes_persistent_volume_claim" "master" {
  metadata {
    name      = "master"
    namespace = var.namespace
  }
  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = {
        storage = var.master_pvc_size
      }
    }
    storage_class_name = var.master_pvc_storage_class
  }
}

resource "kubernetes_job" "hdfs_format" {
  metadata {
    name      = "hdfs-format"
    namespace = var.namespace
  }

  spec {
    backoff_limit = 1
    template {
      metadata {
        name = "hdfs-format"
      }
      spec {
        restart_policy = "OnFailure"
        container {
          name  = "formatter"
          image = "apache/hadoop:3"

          command = ["hdfs"]
          args    = ["namenode", "-format"]

          env {
            name  = "HADOOP_CONF_DIR"
            value = "/etc/hadoop"
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/hadoop"
          }

          volume_mount {
            name       = "hdfs-storage"
            mount_path = "/hadoop/dfs"
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.hdfs.metadata[0].name
          }
        }

        volume {
          name = "hdfs-storage"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.master.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [
    kubernetes_persistent_volume_claim.master,
    kubernetes_config_map.hdfs
  ]
}



resource "kubernetes_stateful_set" "namenode" {
  depends_on = [
    kubernetes_job.hdfs_format
  ]

  metadata {
    name      = "namenode"
    namespace = var.namespace
  }

  spec {
    service_name = "namenode"
    replicas     = 1

    selector {
      match_labels = {
        app = "hdfs-namenode"
      }
    }

    template {
      metadata {
        labels = {
          app = "hdfs-namenode"
        }
      }

      spec {
        container {
          name  = "namenode"
          image = "apache/hadoop:3"

          port {
            name           = "rpc"
            container_port = local.hdfs_port
          }

          port {
            name           = "http"
            container_port = local.hdfs_ui_port
          }

          env {
            name  = "HADOOP_CONF_DIR"
            value = "/etc/hadoop"
          }

          volume_mount {
            name       = "hdfs-storage"
            mount_path = "/hadoop/dfs"
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/hadoop"
          }

          command = ["hdfs"]
          args    = ["namenode"]
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.hdfs.metadata[0].name
          }
        }

        volume {
          name = "hdfs-storage"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.master.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_stateful_set" "datanode" {
  depends_on = [
    kubernetes_stateful_set.namenode
  ]

  metadata {
    name      = "datanode"
    namespace = var.namespace
  }

  spec {
    service_name = "datanode"
    replicas     = var.datanode_replicas

    selector {
      match_labels = {
        app = "hdfs-datanode"
      }
    }

    template {
      metadata {
        labels = {
          app = "hdfs-datanode"
        }
      }

      spec {
        container {
          name  = "datanode"
          image = "apache/hadoop:3"

          port {
            container_port = 9864
            name           = "http"
          }

          port {
            container_port = 9866
            name           = "ipc"
          }

          env {
            name  = "HADOOP_CONF_DIR"
            value = "/etc/hadoop"
          }

          env {
            name  = "CLUSTER_NAME"
            value = "bigdata"
          }

          env {
            name  = "SERVICE_PRECONDITION"
            value = "namenode:${local.hdfs_ui_port}"
          }

          volume_mount {
            name       = "config"
            mount_path = "/etc/hadoop"
          }

          volume_mount {
            name       = "hdfs-datanode-storage"
            mount_path = "/hadoop/dfs"
          }

          command = ["hdfs"]
          args    = ["datanode"]
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.hdfs.metadata[0].name
          }
        }
      }
    }

    persistent_volume_claim_retention_policy {
      when_deleted = "Delete"
      when_scaled = "Delete"
    }

    volume_claim_template {
      metadata {
        name = "hdfs-datanode-storage"
      }

      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = var.datanode_pvc_storage_class

        resources {
          requests = {
            storage = var.datanode_pvc_size
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "namenode" {
  metadata {
    name      = "namenode"
    namespace = var.namespace
    labels = {
      app = "hdfs-namenode"
    }
  }

  spec {
    port {
      name = "rpc"
      port = local.hdfs_port
    }

    port {
      name = "http"
      port = local.hdfs_ui_port
    }

    cluster_ip = "None"  # Headless service for StatefulSet

    selector = {
      app = "hdfs-namenode"
    }
  }
}

resource "kubernetes_service" "datanode" {
  metadata {
    name      = "datanodes"
    namespace = var.namespace
    labels = {
      app = "hdfs-datanode"
    }
  }

  spec {
    port {
      name = "http"
      port = 9864
    }

    selector = {
      app = "hdfs-datanode"
    }
  }
}
