export const env = {
  port: parseInt(process.env.PORT ?? "3000", 10),
  timemanagerUrl: process.env.TIMEMANAGER_URL ?? "http://localhost:8000",
  hiveUrl: process.env.HIVE_HTTP_PROXY_URL ?? "http://localhost:10001",
  kafkaRestUrl: process.env.KAFKA_REST_PROXY_URL ?? "http://localhost:8082",
  kafkaClusterId: process.env.KAFKA_CLUSTER_ID ?? undefined,
  kafkaSampleLimit: parseInt(process.env.KAFKA_SAMPLE_LIMIT ?? "20", 10),
};

export const topics = {
  weather: "weather-data",
  taxi: "taxi-data",
  bike: "bike-data",
};

