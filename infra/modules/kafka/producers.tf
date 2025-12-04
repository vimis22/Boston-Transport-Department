# Bike Producer Deployment
resource "kubernetes_deployment" "bike_producer" {
  metadata {
    name      = "bike-producer"
    namespace = var.namespace
    labels = {
      app = "bike-producer"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "bike-producer"
      }
    }

    template {
      metadata {
        labels = {
          app = "bike-producer"
        }
      }

      spec {
        container {
          name  = "bike-producer"
          image = "oschr20/kafka-producers:latest"
          
          env {
            name  = "KAFKA_BOOTSTRAP_SERVERS"
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
          }

          # Schema Registry URL - Confluent Kafka has built-in Schema Registry
          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://kafka-broker-0.kafka-broker.${var.namespace}.svc.cluster.local:8081"
          }

          resources {
            requests = {
              memory = "128Mi"
              cpu    = "50m"
            }
            limits = {
              memory = "256Mi"
              cpu    = "200m"
            }
          }
        }
      }
    }
  }
}

# Taxi Producer Deployment
resource "kubernetes_deployment" "taxi_producer" {
  metadata {
    name      = "taxi-producer"
    namespace = var.namespace
    labels = {
      app = "taxi-producer"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "taxi-producer"
      }
    }

    template {
      metadata {
        labels = {
          app = "taxi-producer"
        }
      }

      spec {
        container {
          name  = "taxi-producer"
          image = "oschr20/kafka-producers:latest"
          
          command = ["python", "producers/taxi_producer.py"]
          
          env {
            name  = "KAFKA_BOOTSTRAP_SERVERS"
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
          }

          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
          }

          resources {
            requests = {
              memory = "256Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "512Mi"
              cpu    = "500m"
            }
          }
        }
      }
    }
  }
}

# Weather Producer Deployment
resource "kubernetes_deployment" "weather_producer" {
  metadata {
    name      = "weather-producer"
    namespace = var.namespace
    labels = {
      app = "weather-producer"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "weather-producer"
      }
    }

    template {
      metadata {
        labels = {
          app = "weather-producer"
        }
      }

      spec {
        container {
          name  = "weather-producer"
          image = "oschr20/kafka-producers:latest"
          
          command = ["python", "producers/weather_producer.py"]
          
          env {
            name  = "KAFKA_BOOTSTRAP_SERVERS"
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
          }

          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
          }

          resources {
            requests = {
              memory = "256Mi"
              cpu    = "100m"
            }
            limits = {
              memory = "512Mi"
              cpu    = "500m"
            }
          }
        }
      }
    }
  }
}