terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
  }
}

// Create ETL Jobs
resource "kubernetes_job_v1" "bike-weather-data-aggregation" {
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
        init_container {
          name  = "init-container"
          image = "ghcr.io/vimis22/etl:1.0.18"
          command = [
            "cp",
            "-r",
            "/app/jobs/.",
            "/mnt/scripts/"
          ]
          volume_mount {
            name       = "scripts-volume"
            mount_path = "/mnt/scripts"
          }
        }

        container {
          name              = "bike-weather-data-aggregation"
          image             = "oci.stackable.tech/stackable/spark-connect-client:4.0.1-stackable0.0.0-dev"
          image_pull_policy = "IfNotPresent"
          command = [
            "python",
            "/app/jobs/bike-weather-data-aggregation.py",
          ]
          env {
            name  = "SPARK_CONNECT_URL"
            value = "sc://spark-connect-server:15002"
          }
          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry:8081"
          }
          env {
            name  = "KAFKA_BOOTSTRAP"
            value = "kafka-broker:9092"
          }
          env {
            name  = "OUTPUT_TOPIC"
            value = "bike-weather-aggregate"
          }
          env {
            name  = "CHECKPOINT_LOCATION"
            value = "/tmp/bike-weather-aggregate-checkpoint"
          }
          volume_mount {
            name       = "scripts-volume"
            mount_path = "/app/jobs"
          }
        }

        volume {
          name = "scripts-volume"
          empty_dir {
            size_limit = "500Mi"
          }
        }
      }
    }
  }
}

resource "kubernetes_job_v1" "bike-weather-distance" {
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
        init_container {
          name  = "init-container"
          image = "ghcr.io/vimis22/etl:1.0.18"
          command = [
            "cp",
            "-r",
            "/app/jobs/.",
            "/mnt/scripts/"
          ]
          volume_mount {
            name       = "scripts-volume"
            mount_path = "/mnt/scripts"
          }
        }

        container {
          name              = "bike-weather-distance"
          image             = "oci.stackable.tech/stackable/spark-connect-client:4.0.1-stackable0.0.0-dev"
          image_pull_policy = "IfNotPresent"
          command = [
            "python",
            "/app/jobs/bike-weather-distance.py",
          ]
          env {
            name  = "SPARK_CONNECT_URL"
            value = "sc://spark-connect-server:15002"
          }
          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry:8081"
          }
          env {
            name  = "KAFKA_BOOTSTRAP"
            value = "kafka-broker:9092"
          }
          env {
            name  = "CHECKPOINT_LOCATION"
            value = "/tmp/bike-weather-distance-checkpoint"
          }
          volume_mount {
            name       = "scripts-volume"
            mount_path = "/app/jobs"
          }
        }

        volume {
          name = "scripts-volume"
          empty_dir {
            size_limit = "500Mi"
          }
        }
      }
    }
  }
}


resource "kubernetes_job_v1" "data-analysis" {
  metadata {
    name      = "data-analysis"
    namespace = var.namespace
  }
  spec {
    template {
      metadata {}
      spec {
        restart_policy          = "OnFailure"
        active_deadline_seconds = 600
        init_container {
          name  = "init-container"
          image = "ghcr.io/vimis22/etl:1.0.18"
          command = [
            "cp",
            "-r",
            "/app/jobs/.",
            "/mnt/scripts/"
          ]
          volume_mount {
            name       = "scripts-volume"
            mount_path = "/mnt/scripts"
          }
        }

        container {
          name              = "data-analysis"
          image             = "oci.stackable.tech/stackable/spark-connect-client:4.0.1-stackable0.0.0-dev"
          image_pull_policy = "IfNotPresent"
          command = [
            "python",
            "/app/jobs/statistics_etl.py",
          ]

          env {
            name  = "USE_SPARK_CONNECT"
            value = "true"
          }
          
          env {
            name  = "SPARK_CONNECT_URL"
            value = "sc://spark-connect-server.${var.namespace}.svc.cluster.local:15002"
          }
          env {
            name  = "SCHEMA_REGISTRY_URL"
            value = "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
          }
          env {
            name  = "KAFKA_BOOTSTRAP_SERVERS"
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
          }
          env {
            name  = "CHECKPOINT_LOCATION"
            value = "/tmp/data-analysis-checkpoint"
          }
          volume_mount {
            name       = "scripts-volume"
            mount_path = "/app/jobs"
          }
        }

        volume {
          name = "scripts-volume"
          empty_dir {
            size_limit = "500Mi"
          }
        }
      }
    }
  }
}
