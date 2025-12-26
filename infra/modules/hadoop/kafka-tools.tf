resource "kubernetes_service_v1" "schema_registry" {
  metadata {
    name      = "schema-registry"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "schema-registry"
    }

    port {
      port        = 8081
      target_port = 8081
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_deployment_v1" "schema_registry" {
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
        enable_service_links = false
        container {
          name  = "schema-registry"
          image = "confluentinc/cp-schema-registry:7.9.0"

          env {
            name  = "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS"
            value = "PLAINTEXT://kafka-broker:9092"
          }
          env {
            name  = "SCHEMA_REGISTRY_HOST_NAME"
            value = "schema-registry"
          }
          env {
            name  = "SCHEMA_REGISTRY_LISTENERS"
            value = "http://0.0.0.0:8081"
          }
          env {
            name  = "SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR"
            value = "1"
          }

          port {
            container_port = 8081
          }
        }
      }
    }
  }
}

resource "kubernetes_deployment_v1" "kafka_ui" {
  metadata {
    name      = "kafka-ui"
    namespace = var.namespace
    labels = {
      app = "kafka-ui"
    }
  }

  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "kafka-ui"
      }
    }

    template {
      metadata {
        labels = {
          app = "kafka-ui"
        }
      }

      spec {
        container {
          name  = "kafka-ui"
          image = "provectuslabs/kafka-ui:latest"

          port {
            name           = "http"
            container_port = 8080
            protocol       = "TCP"
          }

          env {
            name  = "KAFKA_CLUSTERS_0_NAME"
            value = "kafka-broker"
          }

          env {
            name  = "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS"
            value = "kafka-0.kafka-broker-headless.bigdata.svc.cluster.local:9092,kafka-1.kafka-broker-headless.bigdata.svc.cluster.local:9092,kafka-2.kafka-broker-headless.bigdata.svc.cluster.local:9092"
          }

          env {
            name  = "KAFKA_CLUSTERS_0_SCHEMAREGISTRY"
            value = "http://schema-registry.bigdata.svc.cluster.local:8081"
          }

          env {
            name  = "DYNAMIC_CONFIG_ENABLED"
            value = "true"
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "kafka_ui" {
  metadata {
    name      = "kafka-ui"
    namespace = var.namespace
    labels = {
      app = "kafka-ui"
    }
  }

  spec {
    type = "ClusterIP"
    selector = {
      app = "kafka-ui"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }
  }
}
