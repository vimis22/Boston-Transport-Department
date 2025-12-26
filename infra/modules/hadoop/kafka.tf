resource "kubernetes_service_v1" "kafka_headless" {
  metadata {
    name      = "kafka-broker-headless"
    namespace = var.namespace
    labels = {
      app = "kafka"
    }
  }

  spec {
    selector = {
      app = "kafka"
    }
    cluster_ip = "None"
    port {
      port = 9092
      name = "kafka"
    }
  }
}

resource "kubernetes_service_v1" "kafka" {
  metadata {
    name      = "kafka-broker"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "kafka"
    }
    port {
      port        = 9092
      target_port = 9092
    }
    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set_v1" "kafka" {
  metadata {
    name      = "kafka"
    namespace = var.namespace
    labels = {
      app = "kafka"
    }
  }

  spec {
    service_name = "kafka-broker-headless"
    replicas     = 3

    selector {
      match_labels = {
        app = "kafka"
      }
    }

    template {
      metadata {
        labels = {
          app = "kafka"
        }
      }

      spec {
        container {
          name  = "kafka"
          image = "confluentinc/cp-kafka:7.9.0"

          command = ["/bin/sh", "-c", "export KAFKA_BROKER_ID=$${HOSTNAME##*-} && /etc/confluent/docker/run"]

          port {
            container_port = 9092
            name           = "kafka"
          }

          env {
            name = "POD_NAME"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }
          env {
            name  = "KAFKA_LISTENERS"
            value = "PLAINTEXT://:9092"
          }
          env {
            name  = "KAFKA_ADVERTISED_LISTENERS"
            value = "PLAINTEXT://$(POD_NAME).kafka-broker-headless:9092"
          }
          env {
            name  = "KAFKA_ZOOKEEPER_CONNECT"
            value = "zookeeper:2181"
          }
          env {
            name  = "KAFKA_LOG_DIRS"
            value = "/kafka/kafka-logs"
          }
          env {
            name  = "KAFKA_CLEANUP_POLICY"
            value = "compact"
          }

          volume_mount {
            name       = "kafka-storage"
            mount_path = "/kafka"
          }

        }
      }
    }

    volume_claim_template {
      metadata {
        name = "kafka-storage"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests = {
            storage = "1Gi"
          }
        }
      }
    }

    persistent_volume_claim_retention_policy {
      when_deleted = "Delete"
    }
  }
}
