terraform {
  required_providers {
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = "1.19.0"
    }
  }
}

# Topics for the project (use kube/confluent KafkaTopic CRD)
resource "kubectl_manifest" "kafka_topic_bike" {
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: bike-topic
  namespace: ${var.namespace}
spec:
  topicName: "bike"
  partitions: 3          # <-- choose number of partitions you want
  replicas: 3            # match Kafka cluster replica count (for availability)
  config:
    "cleanup.policy": "delete"
    "retention.ms": "259200000" # 3 days; adjust as needed
  kafkaClusterRef:
    name: kafka-broker
YAML
}

resource "kubectl_manifest" "kafka_topic_taxi" {
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: taxi-topic
  namespace: ${var.namespace}
spec:
  topicName: "taxi"
  partitions: 3
  replicas: 3
  config:
    "cleanup.policy": "delete"
    "retention.ms": "259200000"
  kafkaClusterRef:
    name: kafka-broker
YAML
}

resource "kubectl_manifest" "kafka_topic_weather" {
  yaml_body = <<YAML
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: weather-topic
  namespace: ${var.namespace}
spec:
  topicName: "weather"
  partitions: 3
  replicas: 3
  config:
    "cleanup.policy": "delete"
    "retention.ms": "259200000"
  kafkaClusterRef:
    name: kafka-broker
YAML
}
