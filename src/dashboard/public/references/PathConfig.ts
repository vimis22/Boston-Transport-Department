export const SPEED_PRESETS = [0.5, 1, 2, 5, 10, 50, 100, 300];

export const STREAM_TOPIC_NAMES = ["bike-data", "taxi-data", "weather-data"];

export const QUICK_LINKS = [
  {
    label: "HDFS",
    href: "http://localhost:9870/explorer.html#/",
    description: "NameNode UI & file explorer",
  },
  {
    label: "Kafka UI",
    href: "http://localhost:8083/ui/clusters/kafka-broker/all-topics",
    description: "Topic browser & consumer offsets",
  },
  {
    label: "Spark",
    href: "http://localhost:4040/jobs/",
    description: "Spark application jobs view",
  },
  {
    label: "Jupyter",
    href: "http://localhost:8080/?token=adminadmin",
    description: "Notebook workspace",
  },
];
