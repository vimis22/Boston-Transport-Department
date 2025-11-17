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
    helm = {
      source  = "hashicorp/helm"
      version = ">= 3.0.0"
    }
  }
}

locals {
  zookeeper_cluster_name = "zookeeper-cluster"
  zookeeper_znode_name   = "zookeeper-znode"
  hdfs_cluster_name      = "hdfs-cluster"
  hive_cluster_name      = "hive-cluster"
  kafka_cluster_name     = "kafka-cluster"
}


// OPERATORS
resource "helm_release" "commons-operator" {
  name       = "commons-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "commons-operator"
  wait       = true
}

resource "helm_release" "secret-operator" {
  name       = "secret-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "secret-operator"
  wait       = true
}

resource "helm_release" "listener-operator" {
  name       = "listener-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "listener-operator"
  wait       = true
}

resource "helm_release" "zookeeper-operator" {
  depends_on = [
    helm_release.commons-operator,
    helm_release.secret-operator,
    helm_release.listener-operator
  ]
  name       = "zookeeper-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "zookeeper-operator"
  wait       = true
}

resource "helm_release" "hdfs-operator" {
  depends_on = [
    helm_release.commons-operator,
    helm_release.secret-operator,
    helm_release.listener-operator,
    helm_release.zookeeper-operator
  ]
  name       = "hdfs-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "hdfs-operator"
  wait       = true
}

resource "helm_release" "spark-operator" {
  depends_on = [
    helm_release.commons-operator,
    helm_release.secret-operator,
    helm_release.listener-operator,
    helm_release.zookeeper-operator,
    helm_release.hdfs-operator
  ]
  name       = "spark-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "spark-k8s-operator"
  wait       = true
}

resource "helm_release" "confluent-operator" {
  depends_on = [
    helm_release.commons-operator,
    helm_release.secret-operator,
    helm_release.listener-operator,
    helm_release.zookeeper-operator,
  ]
  name       = "confluent-operator"
  repository = "https://packages.confluent.io/helm"
  version    = "0.1351.24"
  chart      = "confluent-for-kubernetes"
  namespace  = var.namespace
  wait       = true
}

resource "helm_release" "hive-operator" {
  depends_on = [
    helm_release.commons-operator,
    helm_release.secret-operator,
    helm_release.listener-operator,
    helm_release.zookeeper-operator
  ]
  name       = "hive-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "hive-operator"
  wait       = true
}

resource "helm_release" "hive-postgresql" {
  name       = "hive-postgresql"
  repository = "https://charts.bitnami.com/bitnami"
  version    = "16.5.0"
  chart      = "postgresql"
  wait       = true
  namespace  = var.namespace

  values = [
    yamlencode({
      global = {
        security = {
          allowInsecureImages = true
        }
      }
      image = {
        repository = "bitnamilegacy/postgresql"
      }
      volumePermissions = {
        enabled = false
        image = {
          repository = "bitnamilegacy/os-shell"
        }
        securityContext = {
          runAsUser = "auto"
        }
      }
      metrics = {
        image = {
          repository = "bitnamilegacy/postgres-exporter"
        }
      }
      primary = {
        extendedConfiguration = "password_encryption=md5"
      }
      auth = {
        username = "hive"
        password = "hive"
        database = "hive"
      }
    })
  ]
}

// ZOOKEEPER
resource "kubectl_manifest" "zookeeper-cluster" {
  yaml_body = <<YAML
apiVersion: zookeeper.stackable.tech/v1alpha1
kind: ZookeeperCluster
metadata:
  name: ${local.zookeeper_cluster_name}
  namespace: ${var.namespace}
spec:
  image:
    productVersion: "3.9.3"
  servers:
    roleGroups:
      default:
        replicas: 1
YAML
}

resource "kubectl_manifest" "zookeeper-znode" {
  yaml_body = <<YAML
apiVersion: zookeeper.stackable.tech/v1alpha1
kind: ZookeeperZnode
metadata:
  name: ${local.zookeeper_znode_name}
  namespace: ${var.namespace}
spec:
  clusterRef:
    name: ${local.zookeeper_cluster_name}
YAML
}

// HDFS
resource "kubectl_manifest" "hdfs-cluster" {
  depends_on = [
    kubectl_manifest.zookeeper-znode
  ]
  yaml_body = <<YAML
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: ${local.hdfs_cluster_name}
  namespace: ${var.namespace}
spec:
  image:
    productVersion: "3.4.1"
  clusterConfig:
    zookeeperConfigMapName: ${local.zookeeper_znode_name}
    dfsReplication: 1
  nameNodes:
    config:
      listenerClass: "external-stable"
      resources:
        storage:
          data:
            capacity: "5Gi"
    roleGroups:
      default:
        replicas: 2
  dataNodes:
    config:
      listenerClass: "external-unstable"
      resources:
        storage:
          data:
            capacity: "5Gi"
    roleGroups:
      default:
        replicas: 1
  journalNodes:
    config:
      resources:
        storage:
          data:
            capacity: "5Gi"
    roleGroups:
      default:
        replicas: 1
YAML
}


resource "kubernetes_config_map" "hdfs_proxy_config" {
  metadata {
    name      = "hdfs-proxy-config"
    namespace = var.namespace
  }

  data = {
    "nginx.conf" = <<EOF
events {}
http {
  server {
    listen 80;
    server_name _;

    location / {
      proxy_pass http://hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local:9870;

      proxy_set_header Host hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local;

      proxy_redirect http://hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local/ $scheme://$host:9870/;

      proxy_set_header X-Real-IP $remote_addr;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto $scheme;
    }
  }
}
EOF
  }
}

resource "kubernetes_deployment" "hdfs_proxy" {
  depends_on = [
    kubectl_manifest.hdfs-cluster,
    kubernetes_config_map.hdfs_proxy_config
  ]

  metadata {
    name      = "hdfs-proxy"
    namespace = var.namespace
    labels = {
      app = "hdfs-proxy"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "hdfs-proxy"
      }
    }

    template {
      metadata {
        labels = {
          app = "hdfs-proxy"
        }
      }

      spec {
        container {
          name  = "nginx"
          image = "nginx:1.25"
          port {
            container_port = 80
          }

          volume_mount {
            name       = "nginx-config-volume"
            mount_path = "/etc/nginx/nginx.conf"
            sub_path   = "nginx.conf"
          }
        }

        volume {
          name = "nginx-config-volume"
          config_map {
            name = kubernetes_config_map.hdfs_proxy_config.metadata[0].name
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "hdfs_proxy_service" {
  depends_on = [
    kubernetes_deployment.hdfs_proxy
  ]

  metadata {
    name      = "hdfs-proxy-service"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "hdfs-proxy"
    }

    port {
      protocol    = "TCP"
      port        = 80
      target_port = 80
    }

    type = "ClusterIP"
  }
}

# KAFKA
resource "kubectl_manifest" "kafka-cluster" {
  depends_on = [
    kubectl_manifest.zookeeper-znode
  ]
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: Kafka
metadata:
  name: kafka-broker
  namespace: ${var.namespace}
spec:
  replicas: 3
  image:
    application: confluentinc/cp-server:7.9.0
    init: confluentinc/confluent-init-container:3.1.0
  dataVolumeCapacity: 5Gi
  metricReporter:
    enabled: true
  dependencies:
    zookeeper:
      endpoint: zookeeper-cluster-server.${var.namespace}.svc.cluster.local:2282
YAML
}

# KAFKA UI
resource "kubernetes_deployment" "kafka_ui" {
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
            value = "kafka-broker.${var.namespace}.svc.cluster.local:9092"
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

resource "kubernetes_service" "kafka_ui" {
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



# SPARK
resource "kubernetes_config_map" "spark_connect_log_config" {
  metadata {
    name      = "spark-connect-log-config"
    namespace = var.namespace
  }

  data = {
    "log4j2.properties" = <<EOF
appenders = CONSOLE

appender.CONSOLE.type = Console
appender.CONSOLE.name = CONSOLE
appender.CONSOLE.target = SYSTEM_ERR
appender.CONSOLE.layout.type = PatternLayout
appender.CONSOLE.layout.pattern = %d{ISO8601} %p [%t] %c - %m%n
appender.CONSOLE.filter.threshold.type = ThresholdFilter
appender.CONSOLE.filter.threshold.level = DEBUG

rootLogger.level=INFO
rootLogger.appenderRefs = CONSOLE
rootLogger.appenderRef.CONSOLE.ref = CONSOLE
EOF
  }
}

resource "kubectl_manifest" "spark_connect_server" {
  depends_on = [
    kubernetes_config_map.spark_connect_log_config
  ]

  yaml_body = yamlencode({
    apiVersion = "spark.stackable.tech/v1alpha1"
    kind       = "SparkConnectServer"
    metadata = {
      name      = "spark-connect"
      namespace = var.namespace
      labels = {
        "stackable.tech/vendor" = "Stackable"
      }
    }
    spec = {
      image = {
        custom         = "oci.stackable.tech/stackable/spark-connect-client:4.0.1-stackable0.0.0-dev"
        productVersion = "4.0.1"
        pullPolicy     = "IfNotPresent"
      }
      args = []
      server = {
        configOverrides = {
          "spark-defaults.conf" = {
            "spark.jars.ivy"      = "/tmp/ivy2"
            "spark.driver.memory" = "2g"
          }
        }
        podOverrides = {
          spec = {
            containers = [
              {
                name = "spark"
                env = [
                  {
                    name  = "HADOOP_CONF_DIR"
                    value = "/hdfs-config"
                  }
                ]
                volumeMounts = [
                  {
                    name      = "hdfs-discovery-configmap"
                    mountPath = "/hdfs-config"
                  }
                ]
              }
            ]
            volumes = [
              {
                name = "hdfs-discovery-configmap"
                configMap = {
                  name = local.hdfs_cluster_name
                }
              }
            ]
          }
        }
        roleConfig = {
          listenerClass = "external-stable"
        }
        config = {
          resources = {
            memory = {
              limit = "2Gi"
            }
          }
          logging = {
            enableVectorAgent = false
            containers = {
              spark = {
                custom = {
                  configMap = "spark-connect-log-config"
                }
              }
            }
          }
        }
      }
      executor = {
        configOverrides = {
          "spark-defaults.conf" = {
            "spark.executor.instances"        = "4"
            "spark.executor.memory"           = "3g"
            "spark.executor.memoryOverhead"   = "0m"
            "spark.sql.warehouse.dir"         = "/user/hive/warehouse"
            "spark.sql.catalogImplementation" = "hive"
            "spark.jars.packages"             = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.1"
          }
        }
        config = {
          resources = {
            memory = {
              limit = "3Gi"
            }
          }
          logging = {
            enableVectorAgent = false
            containers = {
              spark = {
                custom = {
                  configMap = "spark-connect-log-config"
                }
              }
            }
          }
        }
      }
    }
  })
}

// JUPYTERLAB
resource "kubernetes_deployment" "jupyterlab" {
  metadata {
    name      = "jupyterlab"
    namespace = var.namespace
    labels = {
      app                     = "jupyterlab"
      "stackable.tech/vendor" = "Stackable"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app                     = "jupyterlab"
        "stackable.tech/vendor" = "Stackable"
      }
    }

    template {
      metadata {
        labels = {
          app                     = "jupyterlab"
          "stackable.tech/vendor" = "Stackable"
        }
      }

      spec {
        service_account_name = "default"

        container {
          name              = "jupyterlab"
          image             = "oci.stackable.tech/stackable/spark-connect-client:4.0.1-stackable0.0.0-dev"
          image_pull_policy = "IfNotPresent"

          command = ["bash"]
          args = [
            "-c",
            "/stackable/.local/bin/jupyter lab --ServerApp.token='adminadmin' --ServerApp.port=8080 --no-browser --notebook-dir /notebook"
          ]

          env {
            name  = "JUPYTER_PORT"
            value = "8080"
          }

          port {
            name           = "http"
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

resource "kubernetes_service" "jupyterlab" {
  metadata {
    name      = "jupyterlab"
    namespace = var.namespace
    labels = {
      app                     = "jupyterlab"
      "stackable.tech/vendor" = "Stackable"
    }
  }

  spec {
    type = "NodePort"

    selector = {
      app                     = "jupyterlab"
      "stackable.tech/vendor" = "Stackable"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
  }
}

// HIVE
resource "kubectl_manifest" "hive-credentials" {
  depends_on = [
    helm_release.hive-postgresql
  ]

  yaml_body = <<YAML
apiVersion: v1
kind: Secret
metadata:
  name: hive-credentials
  namespace: ${var.namespace}
type: Opaque
stringData:
  username: hive
  password: hive
YAML
}

resource "kubectl_manifest" "hive-cluster" {
  depends_on = [
    helm_release.hive-operator,
    helm_release.hive-postgresql,
    kubectl_manifest.hive-credentials
  ]

  yaml_body = <<YAML
apiVersion: hive.stackable.tech/v1alpha1
kind: HiveCluster
metadata:
  name: ${local.hive_cluster_name}
  namespace: ${var.namespace}
spec:
  image:
    productVersion: 4.0.1
  clusterConfig:
    database:
      connString: jdbc:postgresql://hive-postgresql:5432/hive
      credentialsSecret: hive-credentials
      dbType: postgres
    hive:
      configMap: ${local.hive_cluster_name}
  metastore:
    roleGroups:
      default:
        replicas: 1
YAML
}

// KAFKA
# resource "kubernetes_deployment" "kafka" {
#   metadata {
#     name      = "broker"
#     namespace = var.namespace
#     labels = {
#       app = "kafka"
#     }
#   }

#   spec {
#     replicas = 1
#     selector {
#       match_labels = {
#         app = "kafka"
#       }
#     }

#     template {
#       metadata {
#         labels = {
#           app = "kafka"
#         }
#       }

#       spec {
#         hostname = "broker"
#         container {
#           name  = "broker"
#           image = "apache/kafka:latest"

#           # External client port (for port-forwarding)
#           port {
#             name           = "kafka-port"
#             container_port = 9092
#           }

#           # Controller port (KRaft)
#           port {
#             name           = "controller-port"
#             container_port = 29093
#           }

#           # Internal broker communication port
#           port {
#             name           = "internal-port"
#             container_port = 29092
#           }

#           env {
#             name  = "KAFKA_BROKER_ID"
#             value = "1"
#           }

#           env {
#             name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
#             value = "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT"
#           }

#           env {
#             name  = "KAFKA_ADVERTISED_LISTENERS"
#             value = "PLAINTEXT://broker:29092,PLAINTEXT_HOST://kafka-broker.${var.namespace}.svc.cluster.local:9092"
#           }

#           env {
#             name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
#             value = "1"
#           }

#           env {
#             name  = "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS"
#             value = "0"
#           }

#           env {
#             name  = "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR"
#             value = "1"
#           }

#           env {
#             name  = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"
#             value = "1"
#           }

#           env {
#             name  = "KAFKA_PROCESS_ROLES"
#             value = "broker,controller"
#           }

#           env {
#             name  = "KAFKA_NODE_ID"
#             value = "1"
#           }

#           env {
#             name  = "KAFKA_CONTROLLER_QUORUM_VOTERS"
#             value = "1@broker:29093"
#           }

#           env {
#             name  = "KAFKA_LISTENERS"
#             value = "PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092"
#           }

#           env {
#             name  = "KAFKA_INTER_BROKER_LISTENER_NAME"
#             value = "PLAINTEXT"
#           }

#           env {
#             name  = "KAFKA_CONTROLLER_LISTENER_NAMES"
#             value = "CONTROLLER"
#           }

#           env {
#             name  = "KAFKA_LOG_DIRS"
#             value = "/tmp/kraft-combined-logs"
#           }

#           env {
#             name  = "CLUSTER_ID"
#             value = "MkU3OEVBNTcwNTJENDM2Qk"
#           }
#         }
#       }
#     }
#   }
# }

# Service to expose Kafka broker within the cluster
# Other resources (Hive, Spark, etc.) connect to this using: broker:29092
# For external access, developers use port-forward to localhost:9092
# resource "kubernetes_service" "kafka" {
#   metadata {
#     name      = "kafka-broker"
#     namespace = var.namespace
#     labels = {
#       app = "kafka"
#     }
#   }

#   spec {
#     selector = {
#       app = "kafka"
#     }

#     # Internal cluster communication (other pods use this)
#     port {
#       name        = "internal"
#       port        = 29092
#       target_port = "internal-port"
#       protocol    = "TCP"
#     }

#     # External port-forward access (developer tools use this)
#     port {
#       name        = "external"
#       port        = 9092
#       target_port = "kafka-port"
#       protocol    = "TCP"
#     }

#     # Controller port for KRaft coordination
#     port {
#       name        = "controller"
#       port        = 29093
#       target_port = "controller-port"
#       protocol    = "TCP"
#     }

#     # ClusterIP service - internal only, no external load balancer
#     type = "ClusterIP"
#   }
# }

# Helper instructions for creating Kafka topics:
# The apache/kafka image has scripts in /opt/kafka/bin/
# To create a topic from within the cluster, use:
# kubectl exec -n ${var.namespace} -it deployment/broker -- /opt/kafka/bin/kafka-topics.sh --create --topic <topic-name> --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
#
# To list topics:
# kubectl exec -n ${var.namespace} -it deployment/broker -- /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server broker:29092
#
# Example: Create the "sparktest" topic:
# kubectl exec -n ${var.namespace} -it deployment/broker -- /opt/kafka/bin/kafka-topics.sh --create --topic sparktest --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
