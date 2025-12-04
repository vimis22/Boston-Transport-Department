# Schema Registry is disabled for now due to compatibility issues
# Producers will auto-register schemas when they connect to Kafka
# This approach is simpler and works well for development

# Uncomment below if you want to try Schema Registry again later
/*
resource "kubernetes_deployment" "schema_registry" {
  metadata {
    name      = "schema-registry"
    namespace = var.namespace
    labels = {
      app = "schema-registry"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "schema-registry"
      }
    }

    template {
      metadata {
        labels = {
          app = "schema-registry"
        }
      }

      spec {
        container {
          name  = "schema-registry"
          image = "confluentinc/cp-schema-registry:7.5.0"

          port {
            name           = "http"
            container_port = 8081
            protocol       = "TCP"
          }

          env {
            name  = "SCHEMA_REGISTRY_HOST_NAME"
            value = "schema-registry"
          }

          env {
            name  = "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
          }

          env {
            name  = "SCHEMA_REGISTRY_LISTENERS"
            value = "http://0.0.0.0:8081"
          }
          
          env {
            name = "SCHEMA_REGISTRY_HEAP_OPTS"
            value = "-Xms256m -Xmx512m"
          }

          liveness_probe {
            http_get {
              path = "/subjects"
              port = 8081
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }

          readiness_probe {
            http_get {
              path = "/subjects"
              port = 8081
            }
            initial_delay_seconds = 15
            period_seconds        = 5
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "schema_registry" {
  metadata {
    name      = "schema-registry"
    namespace = var.namespace
    labels = {
      app = "schema-registry"
    }
  }

  spec {
    type = "ClusterIP"
    
    selector = {
      app = "schema-registry"
    }

    port {
      name        = "http"
      port        = 8081
      target_port = 8081
      protocol    = "TCP"
    }
  }
}
*/