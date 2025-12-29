resource "kubernetes_service_v1" "kafka_connect" {
  metadata {
    name      = "kafka-connect"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "kafka-connect"
    }

    port {
      port        = 8083
      target_port = 8083
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_deployment_v1" "kafka_connect" {
  metadata {
    name      = "kafka-connect"
    namespace = var.namespace
    labels = {
      app = "kafka-connect"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "kafka-connect"
      }
    }

    template {
      metadata {
        labels = {
          app = "kafka-connect"
        }
      }

      spec {
        security_context {
          fs_group   = 1000
          run_as_user = 1000
        }

        init_container {
          name  = "install-plugins"
          image = "confluentinc/cp-server-connect:7.9.0"
          command = ["sh", "-c", "confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:10.2.17"]
          volume_mount {
            name       = "plugins"
            mount_path = "/usr/share/confluent-hub-components"
          }
        }

        container {
          name  = "kafka-connect"
          image = "confluentinc/cp-server-connect:7.9.0"

          env {
            name  = "CONNECT_BOOTSTRAP_SERVERS"
            value = "kafka-broker:9092"
          }
          env {
            name  = "CONNECT_REST_ADVERTISED_HOST_NAME"
            value = "kafka-connect"
          }
          env {
            name  = "CONNECT_REST_PORT"
            value = "8083"
          }
          env {
            name  = "CONNECT_GROUP_ID"
            value = "connect-cluster"
          }
          env {
            name  = "CONNECT_CONFIG_STORAGE_TOPIC"
            value = "connect-configs"
          }
          env {
            name  = "CONNECT_OFFSET_STORAGE_TOPIC"
            value = "connect-offsets"
          }
          env {
            name  = "CONNECT_STATUS_STORAGE_TOPIC"
            value = "connect-status"
          }
          env {
            name  = "CONNECT_KEY_CONVERTER"
            value = "io.confluent.connect.avro.AvroConverter"
          }
          env {
            name  = "CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL"
            value = "http://schema-registry:8081"
          }
          env {
            name  = "CONNECT_VALUE_CONVERTER"
            value = "io.confluent.connect.avro.AvroConverter"
          }
          env {
            name  = "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL"
            value = "http://schema-registry:8081"
          }
          env {
            name  = "CONNECT_INTERNAL_KEY_CONVERTER"
            value = "org.apache.kafka.connect.json.JsonConverter"
          }
          env {
            name  = "CONNECT_INTERNAL_VALUE_CONVERTER"
            value = "org.apache.kafka.connect.json.JsonConverter"
          }
          env {
            name  = "CONNECT_PLUGIN_PATH"
            value = "/usr/share/java,/usr/share/confluent-hub-components"
          }
          env {
            name  = "CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR"
            value = "1"
          }
          env {
            name  = "CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR"
            value = "1"
          }
          env {
            name  = "CONNECT_STATUS_STORAGE_REPLICATION_FACTOR"
            value = "1"
          }
          env {
            name  = "KAFKA_OPTS"
            value = "--add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
          }

          port {
            container_port = 8083
          }

          volume_mount {
            name       = "plugins"
            mount_path = "/usr/share/confluent-hub-components"
          }
          volume_mount {
            name       = "hdfs-config"
            mount_path = "/etc/hadoop"
          }
        }

        volume {
          name = "plugins"
          empty_dir {}
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

