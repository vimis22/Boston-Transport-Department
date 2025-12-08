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

resource "helm_release" "trino-operator" {

  name       = "trino-operator"
  repository = "oci://oci.stackable.tech/sdp-charts"
  version    = "25.7.0"
  chart      = "trino-operator"
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
    configOverrides:
      hdfs-site.xml:
        dfs.permissions.enabled: "false"
    config:
      listenerClass: "external-unstable"
      resources:
        storage:
          data:
            capacity: "5Gi"
    roleGroups:
      default:
        replicas: 2
  dataNodes:
    configOverrides:
      hdfs-site.xml:
        dfs.permissions.enabled: "false"
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
      proxy_pass http://hdfs-cluster-namenode-default-0.hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local:9870;

      proxy_set_header Host hdfs-cluster-namenode-default-0.hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local;

      proxy_redirect http://hdfs-cluster-namenode-default-0.hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local/ $scheme://$host:9870/;

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

# KAFKA CONNECT
resource "kubectl_manifest" "kafka-connect" {
  depends_on = [
    kubectl_manifest.kafka-cluster
  ]
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: Connect
metadata:
  name: connect
  namespace: ${var.namespace}
spec:
  replicas: 1
  image:
    application: confluentinc/cp-server-connect:7.9.0
    init: confluentinc/confluent-init-container:3.1.0
  keyConverterType: io.confluent.connect.avro.AvroConverter
  valueConverterType: io.confluent.connect.avro.AvroConverter
  build:
    type: onDemand
    onDemand:
      plugins:
        locationType: confluentHub
        confluentHub:
          - name: kafka-connect-hdfs
            owner: confluentinc
            version: 10.2.17 # Latest compatible version with hive 3.1.3

  dependencies:
    kafka:
      bootstrapEndpoint: kafka-broker.${var.namespace}.svc.cluster.local:9092
    schemaRegistry:
      url: http://schema-registry.${var.namespace}.svc.cluster.local:8081
  podTemplate:
    podSecurityContext:
      fsGroup: 1000
      runAsUser: 1000
      runAsNonRoot: true
    envVars:
      - name: KAFKA_OPTS
        value: "--add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
YAML
}

## HDFS CONNECTOR
resource "kubectl_manifest" "hdfs_sink_connector" {
  depends_on = [
    kubectl_manifest.kafka-connect,
    kubectl_manifest.hdfs-cluster,
    kubectl_manifest.hive-cluster
  ]

  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: Connector
metadata:
  name: hdfs-sink
  namespace: ${var.namespace}
spec:
  class: "io.confluent.connect.hdfs.HdfsSinkConnector"
  taskMax: 1
  connectClusterRef:
    name: connect
  configs:
    topics: "weather-data, bike-data, taxi-data"
    hdfs.url: "hdfs://hdfs-cluster-namenode-default-0.hdfs-cluster-namenode-default.${var.namespace}.svc.cluster.local:8020"
    flush.size: "3"
    hadoop.conf.dir: "/etc/hadoop/"
    format.class: "io.confluent.connect.hdfs.parquet.ParquetFormat"
    partitioner.class: "io.confluent.connect.storage.partitioner.DefaultPartitioner"
    rotate.interval.ms: "120000"
    hadoop.home: "/opt/hadoop-3.1.3/share/hadoop/common"
    logs.dir: "/tmp"
    hive.integration: "true"
    hive.metastore.uris: "thrift://${local.hive_cluster_name}-metastore.${var.namespace}.svc.cluster.local:9083"
    hive.database: "default"
    confluent.license: ""
    confluent.topic.bootstrap.servers: "kafka-broker.${var.namespace}.svc.cluster.local:9092"
    confluent.topic.replication.factor: "1"
    key.converter: "org.apache.kafka.connect.storage.StringConverter"
    value.converter: "io.confluent.connect.avro.AvroConverter"
    value.converter.schema.registry.url: "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
    schema.compatibility: "BACKWARD"
YAML
}

# SCHEMA REGISTRY
resource "kubectl_manifest" "schema-registry" {
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: SchemaRegistry
metadata:
  name: schema-registry
  namespace: ${var.namespace}
spec:
  replicas: 1
  image:
    application: confluentinc/cp-schema-registry:7.9.0
    init: confluentinc/confluent-init-container:3.1.0
YAML
}

# KAFKA PROXY
resource "kubectl_manifest" "kafka-rest-proxy" {
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: KafkaRestProxy
metadata:
  name: kafkarestproxy
  namespace: ${var.namespace}
spec:
  replicas: 1
  image:
    application: confluentinc/cp-kafka-rest:7.9.0
    init: confluentinc/confluent-init-container:3.1.0
  dependencies:
    schemaRegistry:
      url: http://schema-registry.${var.namespace}.svc.cluster.local:8081
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
            name  = "KAFKA_CLUSTERS_0_SCHEMAREGISTRY"
            value = "http://schema-registry.${var.namespace}.svc.cluster.local:8081"
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
            "spark.jars.ivy"                     = "/tmp/ivy2"
            "spark.driver.memory"                = "2g"
            "spark.sql.catalogImplementation"    = "hive"
            "spark.sql.catalog.hive"             = "org.apache.spark.sql.hive.HiveCatalog"
            "spark.sql.warehouse.dir"            = "/user/hive/warehouse"
            "hive.metastore.uris"                = "thrift://${local.hive_cluster_name}-metastore.${var.namespace}.svc.cluster.local:9083"
            "spark.hadoop.hive.metastore.uris"   = "thrift://${local.hive_cluster_name}-metastore.${var.namespace}.svc.cluster.local:9083"
            "spark.hadoop.hive.metastore.warehouse.dir" = "/user/hive/warehouse"
            "spark.jars.packages"                = "org.apache.spark:spark-hive_2.13:4.0.1"
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
            "spark.executor.instances"        = "1"
            "spark.executor.memory"           = "3g"
            "spark.executor.memoryOverhead"   = "0m"
            "spark.sql.warehouse.dir"         = "/user/hive/warehouse"
            "spark.sql.catalogImplementation" = "hive"
            "spark.sql.catalog.hive"          = "org.apache.spark.sql.hive.HiveCatalog"
            "hive.metastore.uris"             = "thrift://${local.hive_cluster_name}-metastore.${var.namespace}.svc.cluster.local:9083"
            "spark.hadoop.hive.metastore.uris" = "thrift://${local.hive_cluster_name}-metastore.${var.namespace}.svc.cluster.local:9083"
            "spark.hadoop.hive.metastore.warehouse.dir" = "/user/hive/warehouse"
            "spark.jars.packages"             = "org.apache.spark:spark-hive_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.1"
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
    productVersion: 3.1.3 # We cannot upgrade to 4.0.1 yet due to spark compatibility issues: https://github.com/apache/iceberg/issues/12878
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

// SPARK THRIFT SERVER
resource "kubernetes_service" "spark_thrift" {
  metadata {
    name      = "spark-thrift-service"
    namespace = var.namespace
    labels = {
      app = "spark-thrift-server"
    }
  }

  spec {
    cluster_ip = "None"

    selector = {
      app = "spark-thrift-server"
    }

    port {
      name        = "thrift-server-port"
      protocol    = "TCP"
      port        = 10000
      target_port = 10000
    }

    port {
      name        = "http-server-port"
      protocol    = "TCP"
      port        = 10001
      target_port = 10001
    }

    port {
      name        = "spark-driver-port"
      protocol    = "TCP"
      port        = 7078
      target_port = 7078
    }
  }
}

resource "kubernetes_role" "spark_server" {
  metadata {
    name      = "spark-server"
    namespace = var.namespace
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "persistentvolumeclaims", "configmaps", "services"]
    verbs      = ["get", "deletecollection", "create", "list", "watch", "delete"]
  }
}

resource "kubernetes_service_account" "spark" {
  metadata {
    name      = "spark"
    namespace = var.namespace
  }
}

resource "kubernetes_role_binding" "spark" {
  metadata {
    name      = "spark-rolebinding"
    namespace = var.namespace
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.spark.metadata[0].name
    namespace = var.namespace
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.spark_server.metadata[0].name
  }
}
resource "kubernetes_stateful_set" "spark_thrift_server" {
  metadata {
    name      = "spark-thrift-server"
    namespace = var.namespace
    labels = {
      app       = "spark-thrift-server"
      namespace = var.namespace
    }
  }

  spec {
    service_name = kubernetes_service.spark_thrift.metadata[0].name
    replicas     = 1

    selector {
      match_labels = {
        app       = "spark-thrift-server"
        namespace = var.namespace
      }
    }

    template {
      metadata {
        labels = {
          app       = "spark-thrift-server"
          namespace = var.namespace
        }
      }

      spec {
        service_account_name = kubernetes_service_account.spark.metadata[0].name

        container {
          name  = "thrift-server"
          image = "apache/spark:4.0.1"

          env {
            name  = "HADOOP_CONF_DIR"
            value = "/hdfs-config"
          }

          volume_mount {
            name       = "hdfs-discovery-configmap"
            mount_path = "/hdfs-config"
          }

          command = [
            "bash",
            "-c",
            <<-EOT
              set -euo pipefail
              LOG_DIR=/opt/spark/logs
              LOG_FILE="$${LOG_DIR}/spark--org.apache.spark.sql.hive.thriftserver.HiveThriftServer2-1-$${HOSTNAME}.out"
              mkdir -p "$${LOG_DIR}"

              /opt/spark/sbin/start-thriftserver.sh \
              --master k8s://https://kubernetes.default.svc.cluster.local:443 \
              --hiveconf hive.server2.thrift.port=10000 \
              --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
              --hiveconf hive.metastore.uris=thrift://hive-cluster-metastore:9083 \
              --conf spark.hadoop.hive.metastore.uris=thrift://hive-cluster-metastore:9083 \
              --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-cluster-namenode-default-0.hdfs-cluster-namenode-default.bigdata.svc.cluster.local:8020 \
              --conf spark.sql.warehouse.dir=hdfs://hdfs-cluster-namenode-default-0.hdfs-cluster-namenode-default.bigdata.svc.cluster.local:8020/user/hive/warehouse \
              --conf spark.sql.catalogImplementation=hive \
              --conf spark.hadoop.fs.permissions.umask-mode=022 \
              --conf spark.yarn.submit.waitAppCompletion=true \
              --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-cluster-namenode-default-0.hdfs-cluster-namenode-default.bigdata.svc.cluster.local:8020 \
              --proxy-user stackable \
              --conf spark.dynamicAllocation.enabled=true \
              --conf spark.kubernetes.container.image=apache/spark:4.0.1 \
              --conf spark.kubernetes.driver.pod.name=spark-thrift-server-0 \
              --conf spark.kubernetes.executor.request.cores=500m \
              --conf spark.kubernetes.executor.request.memory=1g \
              --conf spark.kubernetes.namespace=${var.namespace} \
              --conf spark.driver.host=spark-thrift-service \
              --conf spark.driver.bindAddress=spark-thrift-server-0 \
              --conf spark.driver.port=7078

              # Wait for log file then stream it
              for i in $(seq 1 30); do
                [ -f "$${LOG_FILE}" ] && break
                sleep 1
              done
              touch "$${LOG_FILE}"
              tail -n 200 -f "$${LOG_FILE}"
            EOT
          ]
        }

        volume {
          name = "hdfs-discovery-configmap"
          config_map {
            name = local.hdfs_cluster_name
          }
        }
      }
    }
  }

  depends_on = [
    kubectl_manifest.hive-cluster,
    kubernetes_service_account.spark,
    kubernetes_role_binding.spark,
    kubernetes_service.spark_thrift,
    kubectl_manifest.hdfs-cluster
  ]
}
