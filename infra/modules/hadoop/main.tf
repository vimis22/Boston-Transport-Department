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

      proxy_redirect http://hdfs-cluster-namenode-default.bigdata.svc.cluster.local/ $scheme://$host:9870/;

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

        init_container {
          name  = "download-notebook"
          image = "oci.stackable.tech/stackable/spark-connect-client:4.0.1-stackable0.0.0-dev"

          command = ["bash"]
          args = [
            "-c",
            "curl https://raw.githubusercontent.com/stackabletech/demos/main/stacks/jupyterhub-pyspark-hdfs/notebook.ipynb -o /notebook/notebook.ipynb"
          ]

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
