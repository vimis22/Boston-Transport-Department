resource "kubernetes_service_v1" "jupyterlab" {
  metadata {
    name      = "jupyterlab"
    namespace = var.namespace
    labels = {
      app = "jupyterlab"
    }
  }

  spec {
    selector = {
      app = "jupyterlab"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }

    type = "ClusterIP" # Changed from NodePort to ClusterIP for internal access, tools/forward-all.py will handle it
  }
}

resource "kubernetes_deployment_v1" "jupyterlab" {
  metadata {
    name      = "jupyterlab"
    namespace = var.namespace
    labels = {
      app = "jupyterlab"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "jupyterlab"
      }
    }

    template {
      metadata {
        labels = {
          app = "jupyterlab"
        }
      }

      spec {
        container {
          name  = "jupyterlab"
          image = "oci.stackable.tech/stackable/spark-connect-client:4.0.1-stackable0.0.0-dev"

          command = ["bash"]
          args = [
            "-c",
            "/stackable/.local/bin/jupyter lab --ServerApp.token='adminadmin' --ServerApp.port=8080 --no-browser --notebook-dir /notebook"
          ]

          env {
            name  = "JUPYTER_PORT"
            value = "8080"
          }
          env {
            name  = "SPARK_REMOTE"
            value = "sc://spark-connect:15002"
          }

          port {
            container_port = 8080
          }

          volume_mount {
            name       = "notebook"
            mount_path = "/notebook"
          }
        }

        volume {
          name = "notebook"
          empty_dir {
            size_limit = "500Mi"
          }
        }
      }
    }
  }
}

