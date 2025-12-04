terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

# Time Manager Deployment
resource "kubernetes_deployment" "timemanager" {
  metadata {
    name      = "timemanager"
    namespace = var.namespace
    labels = {
      app = "timemanager"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "timemanager"
      }
    }

    template {
      metadata {
        labels = {
          app = "timemanager"
        }
      }

      spec {
        container {
          name  = "timemanager"
          image = "ghcr.io/vimis22/timemanager:1.0.1"
          image_pull_policy = "IfNotPresent"

          port {
            name           = "http"
            container_port = 8000
            protocol       = "TCP"
          }

          env {
            name  = "PORT"
            value = "8000"
          }

          env {
            name  = "INITIAL_TIME"
            value = "2018-01-01T00:00:00"
          }

          env {
            name  = "INITIAL_SPEED"
            value = "300"
          }
        }
      }
    }
  }
}

# Time Manager Service
resource "kubernetes_service" "timemanager" {
  metadata {
    name      = "timemanager"
    namespace = var.namespace
    labels = {
      app = "timemanager"
    }
  }

  spec {
    type = "ClusterIP"

    selector = {
      app = "timemanager"
    }

    port {
      name        = "http"
      port        = 8000
      target_port = 8000
      protocol    = "TCP"
    }
  }
}

# Streamer Deployment
resource "kubernetes_deployment" "streamer" {
  metadata {
    name      = "streamer"
    namespace = var.namespace
    labels = {
      app = "streamer"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "streamer"
      }
    }

    template {
      metadata {
        labels = {
          app = "streamer"
        }
      }

      spec {
        container {
          name  = "streamer"
          image = "ghcr.io/vimis22/streamer:1.0.1"
          image_pull_policy = "IfNotPresent"

          env {
            name  = "WEBHDFS_URL"
            value = "http://hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local:9870"
          }

          env {
            name  = "WEBHDFS_DATANODE_URL"
            value = "http://hdfs-cluster-datanode-default.${var.namespace}.svc.cluster.local:9864"
          }

          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
          }

          env {
            name  = "KAFKA_REST_PROXY_URL"
            value = "http://kafkarestproxy.${var.namespace}.svc.cluster.local:8082"
          }

          env {
            name  = "TIME_MANAGER_URL"
            value = "http://timemanager.${var.namespace}.svc.cluster.local:8000"
          }
        }
      }
    }
  }
}

# Streamer Service
resource "kubernetes_service" "streamer" {
  metadata {
    name      = "streamer"
    namespace = var.namespace
    labels = {
      app = "streamer"
    }
  }

  spec {
    type = "ClusterIP"

    selector = {
      app = "streamer"
    }

    port {
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }
  }
}
