terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

# Time Manager Deployment
resource "kubernetes_deployment_v1" "timemanager" {
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
          name              = "timemanager"
          image             = "ghcr.io/vimis22/timemanager:1.0.13"
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
resource "kubernetes_service_v1" "timemanager" {
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
resource "kubernetes_deployment_v1" "streamer" {
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
          name              = "streamer"
          image             = "ghcr.io/vimis22/streamer:1.0.14"
          image_pull_policy = "IfNotPresent"

          env {
            name  = "WEBHDFS_URL"
            value = "http://hdfs-namenode:9870"
          }

          env {
            name  = "WEBHDFS_DATANODE_URL"
            value = "http://hdfs-datanode:9864"
          }

          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry:8081"
          }

          env {
            name  = "KAFKA_REST_PROXY_URL"
            value = "http://kafka-rest-proxy:8082"
          }

          env {
            name  = "TIME_MANAGER_URL"
            value = "http://timemanager:8000"
          }
        }
      }
    }
  }
}

# Streamer Service
resource "kubernetes_service_v1" "streamer" {
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


# Hive HTTP Proxy Deployment
resource "kubernetes_deployment_v1" "hive_http_proxy" {
  metadata {
    name      = "hive-http-proxy"
    namespace = var.namespace
    labels = {
      app = "hive-http-proxy"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "hive-http-proxy"
      }
    }

    template {
      metadata {
        labels = {
          app = "hive-http-proxy"
        }
      }

      spec {
        container {
          name              = "hive-http-proxy"
          image             = "ghcr.io/vimis22/hive-http-proxy:1.0.14"
          image_pull_policy = "IfNotPresent"

          env {
            name  = "HIVE_HOST"
            value = "spark-thrift"
          }

          env {
            name  = "HIVE_PORT"
            value = "10000"
          }

          env {
            name  = "HIVE_USERNAME"
            value = "stackable"
          }
        }
      }
    }
  }
}

# Hive HTTP Proxy Service
resource "kubernetes_service_v1" "hive_http_proxy" {
  metadata {
    name      = "hive-http-proxy"
    namespace = var.namespace
    labels = {
      app = "hive-http-proxy"
    }
  }

  spec {
    type = "ClusterIP"

    selector = {
      app = "hive-http-proxy"
    }

    port {
      name        = "http"
      port        = 10001
      target_port = 10001
      protocol    = "TCP"
    }
  }
}

# Dashboard Deployment
resource "kubernetes_deployment_v1" "dashboard" {
  metadata {
    name      = "dashboard"
    namespace = var.namespace
    labels = {
      app = "dashboard"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "dashboard"
      }
    }

    template {
      metadata {
        labels = {
          app = "dashboard"
        }
      }

      spec {
        container {
          name              = "dashboard"
          image             = "ghcr.io/vimis22/dashboard:1.0.14"
          image_pull_policy = "IfNotPresent"

          env {
            name  = "PORT"
            value = "3000"
          }

          env {
            name  = "TIMEMANAGER_URL"
            value = "http://timemanager:8000"
          }

          env {
            name  = "HIVE_HTTP_PROXY_URL"
            value = "http://hive-http-proxy:10001"
          }

          env {
            name  = "KAFKA_UI_URL"
            value = "http://kafka-ui:8080"
          }

          env {
            name  = "KAFKA_CLUSTER_ID"
            value = "kafka-broker"
          }
        }
      }
    }
  }
}

# Dashboard Service
resource "kubernetes_service_v1" "dashboard" {
  metadata {
    name      = "dashboard"
    namespace = var.namespace
    labels = {
      app = "dashboard"
    }
  }
  spec {
    type = "ClusterIP"

    selector = {
      app = "dashboard"
    }

    port {
      name        = "http"
      port        = 3000
      target_port = 3000
      protocol    = "TCP"
    }
  }
}