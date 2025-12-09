terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = "1.19.0"
    }
  }
}

// Create Topics
resource "kubectl_manifest" "bike-weather-distance" {
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: bike-weather-distance
  namespace: ${var.namespace}
spec:
  replicas: 1
  partitionCount: 1
YAML
}

resource "kubectl_manifest" "bike-weather-aggregate" {
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: bike-weather-aggregate
  namespace: ${var.namespace}
spec:
  replicas: 1
  partitionCount: 1
YAML
}

// Create ETL Jobs
resource "kubernetes_job" "bike-weather-data-aggregation" {
  metadata {
    name      = "bike-weather-data-aggregation"
    namespace = var.namespace
  }
  spec {
    template {
      metadata {}
      spec {
        restart_policy          = "OnFailure"
        active_deadline_seconds = 600
        container {
          name              = "bike-weather-data-aggregation"
          image             = "ghcr.io/vimis22/etl:1.0.9"
          image_pull_policy = "IfNotPresent"
          command = [
            "python",
            "/app/jobs/bike-weather-data-aggregation.py",
          ]
          env {
            name  = "SPARK_CONNECT_URL"
            value = "sc://spark-connect-server.${var.namespace}.svc.cluster.local:15002"
          }
          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
          }
          env {
            name  = "KAFKA_BOOTSTRAP"
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
          }
          env {
            name  = "OUTPUT_TOPIC"
            value = "bike-weather-aggregate"
          }
          env {
            name  = "CHECKPOINT_LOCATION"
            value = "/tmp/bike-weather-aggregate-checkpoint"
          }
        }
      }
    }
  }
}

resource "kubernetes_job" "bike-weather-distance" {
  metadata {
    name      = "bike-weather-distance"
    namespace = var.namespace
  }
  spec {
    template {
      metadata {}
      spec {
        restart_policy          = "OnFailure"
        active_deadline_seconds = 600
        container {
          name              = "bike-weather-distance"
          image             = "ghcr.io/vimis22/etl:1.0.9"
          image_pull_policy = "IfNotPresent"
          command = [
            "python",
            "/app/jobs/bike-weather-distance.py",
          ]
          env {
            name  = "SPARK_CONNECT_URL"
            value = "sc://spark-connect-server.${var.namespace}.svc.cluster.local:15002"
          }
          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
          }
          env {
            name  = "KAFKA_BOOTSTRAP"
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
          }

          env {
            name  = "CHECKPOINT_LOCATION"
            value = "/tmp/bike-weather-distance-checkpoint"
          }
        }
      }
    }
  }
}
