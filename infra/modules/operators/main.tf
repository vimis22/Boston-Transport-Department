terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = ">= 3.0.0"
    }
  }
}


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