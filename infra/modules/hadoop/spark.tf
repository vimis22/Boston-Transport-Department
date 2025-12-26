resource "kubernetes_service_account_v1" "spark" {
  metadata {
    name      = "spark"
    namespace = var.namespace
  }
}

resource "kubernetes_role_v1" "spark" {
  metadata {
    name      = "spark-role"
    namespace = var.namespace
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "services", "configmaps", "secrets", "persistentvolumeclaims"]
    verbs      = ["get", "list", "watch", "create", "delete", "update", "patch"] // "deletecollection"
  }

  rule {
    api_groups = ["apps"]
    resources  = ["statefulsets", "deployments"]
    verbs      = ["get", "list", "watch", "create", "delete", "update", "patch"] // "deletecollection"
  }

  rule {
    api_groups = ["batch"]
    resources  = ["jobs"]
    verbs      = ["get", "list", "watch", "create", "delete", "update", "patch"] // "deletecollection"
  }
}

resource "kubernetes_role_binding_v1" "spark" {
  metadata {
    name      = "spark-role-binding"
    namespace = var.namespace
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.spark.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.spark.metadata[0].name
    namespace = var.namespace
  }
}

# SPARK THRIFT SERVER
resource "kubernetes_service_v1" "spark_thrift" {
  metadata {
    name      = "spark-thrift"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "spark-thrift"
    }

    port {
      name = "thrift"
      port = 10000
    }

    port {
      name = "ui"
      port = 4040
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set_v1" "spark_thrift" {
  depends_on = [kubernetes_stateful_set_v1.hive_metastore]
  metadata {
    name      = "spark-thrift"
    namespace = var.namespace
    labels = {
      app = "spark-thrift"
    }
  }

  spec {
    service_name = "spark-thrift"
    replicas     = 1

    selector {
      match_labels = {
        app = "spark-thrift"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-thrift"
        }
      }

      spec {
        service_account_name = kubernetes_service_account_v1.spark.metadata[0].name

        init_container {
          name  = "wait-for-hive-metastore"
          image = "busybox:1.28"
          command = ["sh", "-c", "until nc -z hive-metastore 9083; do echo waiting for hive-metastore; sleep 2; done;"]
        }

        container {
          name  = "spark-thrift"
          image = "apache/spark:4.0.1"

          env {
            name  = "HADOOP_CONF_DIR"
            value = "/etc/hadoop"
          }

          command = ["bash", "-c"]
          args = [
            <<-EOT
            /opt/spark/sbin/start-thriftserver.sh \
            --master k8s://https://kubernetes.default.svc.cluster.local:443 \
            --hiveconf hive.server2.thrift.port=10000 \
            --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
            --hiveconf hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:8020 \
            --conf spark.sql.warehouse.dir=hdfs://hdfs-namenode:8020/user/hive/warehouse \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.kubernetes.container.image=apache/spark:4.0.1 \
            --conf spark.kubernetes.namespace=${var.namespace} \
            --conf spark.driver.host=$(hostname -i) \
            --conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.driver.port=7078

            tail -f /opt/spark/logs/*.out
            EOT
          ]

          port {
            container_port = 10000
          }
          port {
            container_port = 4040
          }

          volume_mount {
            name       = "hdfs-config"
            mount_path = "/etc/hadoop"
          }
        }

        volume {
          name = "hdfs-config"
          config_map {
            name = "hdfs-config"
          }
        }
      }
    }
  }
}

# SPARK CONNECT SERVER
resource "kubernetes_service_v1" "spark_connect" {
  metadata {
    name      = "spark-connect-server"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "spark-connect-server"
    }

    port {
      name = "connect"
      port = 15002
    }

    port {
      name = "ui"
      port = 4040
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_deployment_v1" "spark_connect" {
  depends_on = [kubernetes_stateful_set_v1.hive_metastore]
  metadata {
    name      = "spark-connect-server"
    namespace = var.namespace
    labels = {
      app = "spark-connect-server"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "spark-connect-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-connect-server"
        }
      }

      spec {
        service_account_name = kubernetes_service_account_v1.spark.metadata[0].name

        init_container {
          name  = "wait-for-hive-metastore"
          image = "busybox:1.28"
          command = ["sh", "-c", "until nc -z hive-metastore 9083; do echo waiting for hive-metastore; sleep 2; done;"]
        }

        container {
          name  = "spark-connect-server"
          image = "apache/spark:4.0.1"

          env {
            name  = "HADOOP_CONF_DIR"
            value = "/etc/hadoop"
          }

          command = ["bash", "-c"]
          args = [
            <<-EOT
            /opt/spark/bin/spark-submit \
            --class org.apache.spark.sql.connect.service.SparkConnectServer \
            --master k8s://https://kubernetes.default.svc.cluster.local:443 \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode:8020 \
            --conf spark.hadoop.hive.metastore.warehouse.dir=/user/hive/warehouse \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.catalog.hive=org.apache.spark.sql.hive.HiveCatalog \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.spark:spark-hive_2.13:4.0.1,org.apache.spark:spark-avro_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.1 \
            --conf spark.sql.warehouse.dir=hdfs://hdfs-namenode:8020/user/hive/warehouse \
            --conf spark.kubernetes.container.image=apache/spark:4.0.1 \
            --conf spark.kubernetes.namespace=${var.namespace} \
            --conf spark.driver.host=$(hostname -i) \
            --conf spark.driver.bindAddress=0.0.0.0 \
            --conf spark.driver.port=7079
            EOT
          ]

          port {
            container_port = 15002
          }
          port {
            container_port = 4040
          }

          volume_mount {
            name       = "hdfs-config"
            mount_path = "/etc/hadoop"
          }
        }

        volume {
          name = "hdfs-config"
          config_map {
            name = "hdfs-config"
          }
        }
      }
    }
  }
}

