resource "kubernetes_service_v1" "kafka_rest_proxy" {
  metadata {
    name      = "kafka-rest-proxy"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "kafka-rest-proxy"
    }

    port {
      port        = 8082
      target_port = 8082
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_deployment_v1" "kafka_rest_proxy" {
  metadata {
    name      = "kafka-rest-proxy"
    namespace = var.namespace
    labels = {
      app = "kafka-rest-proxy"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "kafka-rest-proxy"
      }
    }

    template {
      metadata {
        labels = {
          app = "kafka-rest-proxy"
        }
      }

      spec {
        container {
          name  = "kafka-rest-proxy"
          image = "confluentinc/cp-kafka-rest:7.9.0"

          env {
            name  = "KAFKA_REST_HOST_NAME"
            value = "kafka-rest-proxy"
          }
          env {
            name  = "KAFKA_REST_BOOTSTRAP_SERVERS"
            value = "PLAINTEXT://kafka-broker:9092"
          }
          env {
            name  = "KAFKA_REST_SCHEMA_REGISTRY_URL"
            value = "http://schema-registry:8081"
          }
          env {
            name  = "KAFKA_REST_LISTENERS"
            value = "http://0.0.0.0:8082"
          }

          port {
            container_port = 8082
          }
        }
      }
    }
  }
}

