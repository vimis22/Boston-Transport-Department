resource "kubernetes_secret_v1" "hive_credentials" {
  metadata {
    name      = "hive-credentials"
    namespace = var.namespace
  }

  data = {
    username = "hive"
    password = "hive"
  }
}

resource "kubernetes_config_map_v1" "hive_config" {
  metadata {
    name      = "hive-config"
    namespace = var.namespace
  }

  data = {
    "hive-site.xml" = <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.metastore.port</name>
    <value>9083</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>hdfs://hdfs-namenode:8020/user/hive/warehouse</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://hive-postgresql:5432/hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://hive-metastore:9083</value>
  </property>
  <property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
  </property>
</configuration>
EOF
  }
}

resource "kubernetes_service_v1" "hive_metastore" {
  metadata {
    name      = "hive-metastore"
    namespace = var.namespace
  }

  spec {
    selector = {
      app  = "hive"
      role = "metastore"
    }

    port {
      port        = 9083
      target_port = 9083
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set_v1" "hive_metastore" {
  metadata {
    name      = "hive-metastore"
    namespace = var.namespace
  }

  spec {
    service_name = "hive-metastore"
    replicas     = 1

    selector {
      match_labels = {
        app  = "hive"
        role = "metastore"
      }
    }

    template {
      metadata {
        labels = {
          app  = "hive"
          role = "metastore"
        }
      }

      spec {
        init_container {
          name  = "init-schema"
          image = "apache/hive:3.1.3"
          command = ["sh", "-c", "/opt/hive/bin/schematool -dbType postgres -initSchema || /opt/hive/bin/schematool -dbType postgres -upgradeSchema"]
          
          volume_mount {
            name       = "hive-config"
            mount_path = "/opt/hive/conf/hive-site.xml"
            sub_path   = "hive-site.xml"
          }
        }

        container {
          name  = "metastore"
          image = "apache/hive:3.1.3"
          command = ["sh", "-c", "/opt/hive/bin/hive --service metastore"]

          port {
            container_port = 9083
          }

          volume_mount {
            name       = "hive-config"
            mount_path = "/opt/hive/conf/hive-site.xml"
            sub_path   = "hive-site.xml"
          }
          
          volume_mount {
            name       = "hdfs-config"
            mount_path = "/opt/hadoop/etc/hadoop"
          }
        }

        volume {
          name = "hive-config"
          config_map {
            name = kubernetes_config_map_v1.hive_config.metadata[0].name
          }
        }

        volume {
          name = "hdfs-config"
          config_map {
            name = kubernetes_config_map_v1.hdfs_config.metadata[0].name
          }
        }
      }
    }
  }
}

