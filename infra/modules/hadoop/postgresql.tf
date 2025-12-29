resource "kubernetes_secret_v1" "hive_postgresql" {
  metadata {
    name      = "hive-postgresql"
    namespace = var.namespace
  }

  data = {
    "password"          = "hive"
    "postgres-password" = "hive"
  }
}

resource "kubernetes_config_map_v1" "hive_postgresql_extended_config" {
  metadata {
    name      = "hive-postgresql-extended-configuration"
    namespace = var.namespace
  }

  data = {
    "override.conf" = "password_encryption=md5"
  }
}

resource "kubernetes_service_v1" "hive_postgresql" {
  metadata {
    name      = "hive-postgresql"
    namespace = var.namespace
  }

  spec {
    selector = {
      "app.kubernetes.io/instance" = "hive-postgresql"
      "app.kubernetes.io/name"     = "postgresql"
    }

    port {
      port        = 5432
      target_port = 5432
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set_v1" "hive_postgresql" {
  metadata {
    name      = "hive-postgresql"
    namespace = var.namespace
  }

  spec {
    service_name = "hive-postgresql"
    replicas     = 1

    selector {
      match_labels = {
        "app.kubernetes.io/instance" = "hive-postgresql"
        "app.kubernetes.io/name"     = "postgresql"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/instance" = "hive-postgresql"
          "app.kubernetes.io/name"     = "postgresql"
        }
      }

      spec {
        container {
          name  = "postgresql"
          image = "bitnamilegacy/postgresql:17.4.0-debian-12-r4"

          env {
            name  = "POSTGRES_USER"
            value = "hive"
          }
          env {
            name = "POSTGRES_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret_v1.hive_postgresql.metadata[0].name
                key  = "password"
              }
            }
          }
          env {
            name  = "POSTGRES_DB"
            value = "hive"
          }
          env {
            name  = "POSTGRESQL_PGAUDIT_LOG_CATALOG"
            value = "off"
          }

          port {
            container_port = 5432
          }

          volume_mount {
            name       = "data"
            mount_path = "/bitnami/postgresql"
          }

          volume_mount {
            name       = "extended-config"
            mount_path = "/bitnami/postgresql/conf/conf.d/"
          }
        }

        volume {
          name = "extended-config"
          config_map {
            name = kubernetes_config_map_v1.hive_postgresql_extended_config.metadata[0].name
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

    persistent_volume_claim_retention_policy {
      when_deleted = "Delete"
    }
  }
}

