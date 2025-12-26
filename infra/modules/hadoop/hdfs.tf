resource "kubernetes_config_map_v1" "zookeeper_znode" {
  metadata {
    name      = "zookeeper-znode"
    namespace = var.namespace
  }

  data = {
    ZOOKEEPER             = "zookeeper:2181"
    ZOOKEEPER_CHROOT      = "/"
    ZOOKEEPER_CLIENT_PORT = "2181"
    ZOOKEEPER_HOSTS       = "zookeeper:2181"
  }
}

resource "kubernetes_config_map_v1" "hdfs_config" {
  metadata {
    name      = "hdfs-config"
    namespace = var.namespace
  }

  data = {
    "core-site.xml" = <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://hdfs-namenode:8020</value>
  </property>
</configuration>
EOF
    "hdfs-site.xml" = <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/hadoop/dfs/data</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>hdfs-namenode:8020</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-bind-host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>0.0.0.0:9870</value>
  </property>
  <property>
    <name>dfs.namenode.http-bind-host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>
</configuration>
EOF
  }
}

resource "kubernetes_service_v1" "hdfs_namenode" {
  metadata {
    name      = "hdfs-namenode"
    namespace = var.namespace
  }

  spec {
    selector = {
      app  = "hdfs"
      role = "namenode"
    }

    port {
      name = "rpc"
      port = 8020
    }

    port {
      name = "http"
      port = 9870
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_service_v1" "hdfs_datanode" {
  metadata {
    name      = "hdfs-datanode"
    namespace = var.namespace
  }

  spec {
    selector = {
      app  = "hdfs"
      role = "datanode"
    }

    port {
      name = "http"
      port = 9864
    }

    port {
      name = "data"
      port = 9866
    }

    port {
      name = "ipc"
      port = 9867
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set_v1" "hdfs_namenode" {
  metadata {
    name      = "hdfs-namenode"
    namespace = var.namespace
  }

  spec {
    service_name = "hdfs-namenode"
    replicas     = 1

    selector {
      match_labels = {
        app  = "hdfs"
        role = "namenode"
      }
    }

    template {
      metadata {
        labels = {
          app  = "hdfs"
          role = "namenode"
        }
      }

      spec {
        init_container {
          name  = "format-namenode"
          image = "apache/hadoop:3.4.1"
          command = ["bash", "-c", "if [ ! -d /hadoop/dfs/name/current ]; then /opt/hadoop/bin/hdfs namenode -format -nonInteractive; fi"]
          security_context {
            run_as_user = 0
          }
          env {
            name  = "HDFS_NAMENODE_USER"
            value = "root"
          }
          volume_mount {
            name       = "data"
            mount_path = "/hadoop/dfs/name"
          }
          volume_mount {
            name       = "config"
            mount_path = "/opt/hadoop/etc/hadoop"
          }
        }

        container {
          name  = "namenode"
          image = "apache/hadoop:3.4.1"
          command = ["/opt/hadoop/bin/hdfs", "namenode"]

          security_context {
            run_as_user = 0
          }

          env {
            name  = "HDFS_NAMENODE_USER"
            value = "root"
          }
          env {
            name  = "HADOOP_HOME"
            value = "/opt/hadoop"
          }

          port {
            container_port = 8020
            name           = "rpc"
          }
          port {
            container_port = 9870
            name           = "http"
          }

          volume_mount {
            name       = "data"
            mount_path = "/hadoop/dfs/name"
          }
          volume_mount {
            name       = "config"
            mount_path = "/opt/hadoop/etc/hadoop"
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map_v1.hdfs_config.metadata[0].name
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = {
            storage = "5Gi"
          }
        }
      }
    }
  }
}

resource "kubernetes_stateful_set_v1" "hdfs_datanode" {
  metadata {
    name      = "hdfs-datanode"
    namespace = var.namespace
  }

  spec {
    service_name = "hdfs-datanode"
    replicas     = 1

    selector {
      match_labels = {
        app  = "hdfs"
        role = "datanode"
      }
    }

    template {
      metadata {
        labels = {
          app  = "hdfs"
          role = "datanode"
        }
      }

      spec {
        container {
          name  = "datanode"
          image = "apache/hadoop:3.4.1"
          command = ["/opt/hadoop/bin/hdfs", "datanode"]

          security_context {
            run_as_user = 0
          }

          env {
            name  = "HDFS_DATANODE_USER"
            value = "root"
          }
          env {
            name  = "HADOOP_HOME"
            value = "/opt/hadoop"
          }

          port {
            container_port = 9864
            name           = "http"
          }
          port {
            container_port = 9866
            name           = "data"
          }
          port {
            container_port = 9867
            name           = "ipc"
          }

          volume_mount {
            name       = "data"
            mount_path = "/hadoop/dfs/data"
          }
          volume_mount {
            name       = "config"
            mount_path = "/opt/hadoop/etc/hadoop"
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map_v1.hdfs_config.metadata[0].name
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = {
            storage = "5Gi"
          }
        }
      }
    }
  }
}
