export type ClockState = {
  run_id: string;
  current_time: string;
  speed: number;
  last_update: string;
};

export type HiveQueryRequest = {
  statement: string;
};

export type HiveQueryResult = {
  status?: string;
  columns: string[];
  data: Record<string, unknown>[];
  row_count?: number;
};

export type KafkaTopicsResponse = {
  knownTopics: Record<string, string>;
  topics: {
    name: string;
    internal: boolean;
    partitions: number;
    replication: number;
  }[];
};

export type KafkaTopicMetrics = {
  name: string;
  partitions: number;
  replication: number;
  internal: boolean;
};

export type KafkaRecord = {
  topic: string;
  partition: number;
  offset: number;
  key?: unknown;
  value: unknown;
  timestamp?: string;
};

export type KafkaSampleResponse = {
  records: KafkaRecord[];
};

export type DashboardConfig = {
  endpoints: {
    timemanager: string;
    hive: string;
    kafka: string;
  };
  topics: Record<string, string>;
};

