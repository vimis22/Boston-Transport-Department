resource "kubernetes_service_v1" "zookeeper_headless" {
  metadata {
    name      = "zookeeper-headless"
    namespace = var.namespace
    labels = {
      app = "zookeeper"
    }
  }

  spec {
    cluster_ip = "None"
    selector = {
      app = "zookeeper"
    }

    port {
      name = "client"
      port = 2181
    }

    port {
      name = "follower"
      port = 2888
    }

    port {
      name = "election"
      port = 3888
    }
  }
}

resource "kubernetes_service_v1" "zookeeper" {
  metadata {
    name      = "zookeeper"
    namespace = var.namespace
    labels = {
      app = "zookeeper"
    }
  }

  spec {
    selector = {
      app = "zookeeper"
    }

    port {
      name = "client"
      port = 2181
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set_v1" "zookeeper" {
  metadata {
    name      = "zookeeper"
    namespace = var.namespace
    labels = {
      app = "zookeeper"
    }
  }

  spec {
    service_name = kubernetes_service_v1.zookeeper_headless.metadata[0].name
    replicas     = 1

    selector {
      match_labels = {
        app = "zookeeper"
      }
    }

    template {
      metadata {
        labels = {
          app = "zookeeper"
        }
      }

      spec {
        container {
          name  = "zookeeper"
          image = "zookeeper:3.9.3"

          env {
            name  = "ZOO_MY_ID"
            value = "1"
          }
          env {
            name  = "ZOO_SERVERS"
            value = "server.1=0.0.0.0:2888:3888;2181"
          }

          port {
            name           = "client"
            container_port = 2181
          }
          port {
            name           = "follower"
            container_port = 2888
          }
          port {
            name           = "election"
            container_port = 3888
          }

          volume_mount {
            name       = "data"
            mount_path = "/bitnami/zookeeper"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "data"
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
  }
}

