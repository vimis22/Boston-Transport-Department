import { env, topics as topicNames } from "../env";
import { HttpError, fetchJson } from "../http";

type KafkaTopicItem = {
  topic_name: string;
  is_internal?: boolean;
  replication_factor?: number;
  partitions_count?: number;
};

type KafkaTopicResponse = {
  data: KafkaTopicItem[];
};

type KafkaTopicDetail = {
  topic_name: string;
  partitions_count: number;
  replication_factor: number;
  is_internal: boolean;
};

type KafkaRecord = {
  topic: string;
  partition: number;
  offset: number;
  key?: unknown;
  value: unknown;
  timestamp?: string;
};

export class KafkaService {
  private discoveredClusterId: string | undefined;
  private readonly kafkaUiBase = env.kafkaUiUrl.replace(/\/$/, "");
  private readonly sampleConsumers = new Map<
    string,
    {
      consumerBase: string;
      buffer: KafkaRecord[];
      initialized: boolean;
    }
  >();

  async getClusterId(): Promise<string> {
    if (env.kafkaClusterId) return env.kafkaClusterId;
    if (this.discoveredClusterId) return this.discoveredClusterId;

    const data = await fetchJson<Array<{ name: string }>>(
      `${this.kafkaUiBase}/api/clusters`
    );
    const id = data?.[0]?.name;
    if (!id) throw new Error("Kafka cluster_id not found");
    this.discoveredClusterId = id;
    return id;
  }

  async listTopics(): Promise<KafkaTopicItem[]> {
    const clusterId = await this.getClusterId();
    const result = await fetchJson<{ topics: Array<{ name: string; internal: boolean; partitionCount: number; replicationFactor: number }> }>(
      `${this.kafkaUiBase}/api/clusters/${clusterId}/topics`
    );
    return (result.topics ?? []).map(topic => ({
      topic_name: topic.name,
      is_internal: topic.internal,
      partitions_count: topic.partitionCount,
      replication_factor: topic.replicationFactor,
    }));
  }

  async getTopicDetail(topic: string): Promise<KafkaTopicDetail> {
    const clusterId = await this.getClusterId();
    const detail = await fetchJson<{
      name: string;
      partitionCount: number;
      replicationFactor: number;
      internal: boolean;
    }>(`${this.kafkaUiBase}/api/clusters/${clusterId}/topics/${topic}`);

    return {
      topic_name: detail.name,
      partitions_count: detail.partitionCount,
      replication_factor: detail.replicationFactor,
      is_internal: detail.internal,
    };
  }

  async getTopicSample(topic: string, limit = env.kafkaSampleLimit) {
    const url = new URL(
      `${this.kafkaUiBase}/api/clusters/${encodeURIComponent(
        env.kafkaUiClusterId
      )}/topics/${encodeURIComponent(topic)}/messages`
    );
    url.searchParams.set("limit", String(limit));
    url.searchParams.set("page", "0");
    url.searchParams.set("seekDirection", "BACKWARD");
    url.searchParams.set("seekType", "LATEST");
    url.searchParams.set("keySerde", "String");
    url.searchParams.set("valueSerde", "SchemaRegistry");

    const res = await fetch(url.toString(), {
      headers: {
        Accept: "text/event-stream",
      },
    });

    const body = await res.text();
    if (!res.ok) {
      throw new HttpError(
        `Kafka UI sample failed: ${res.status}`,
        res.status,
        body
      );
    }

    const records: KafkaRecord[] = [];

    for (const line of body.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed.startsWith("data:")) continue;
      const jsonPart = trimmed.slice(5).trim();
      if (!jsonPart) continue;

      let payload: any;
      try {
        payload = JSON.parse(jsonPart);
      } catch {
        continue;
      }

      if (payload.type !== "MESSAGE" || !payload.message) continue;
      const msg = payload.message;

      let value: unknown = msg.content;
      if (typeof msg.content === "string") {
        try {
          value = JSON.parse(msg.content);
        } catch {
          // keep raw string if not JSON
        }
      }

      records.push({
        topic,
        partition: msg.partition ?? 0,
        offset: msg.offset ?? 0,
        key: msg.key ?? undefined,
        value,
        timestamp: msg.timestamp ?? undefined,
      });
    }

    // Kafka UI returns newest first (BACKWARD from LATEST); normalize to ascending.
    const sorted = records.sort((a, b) => a.offset - b.offset);
    return sorted.slice(-limit);
  }

  getKnownTopics() {
    return topicNames;
  }

  private async safeFetch(url: string, init: RequestInit) {
    const res = await fetch(url, init);
    if (!res.ok) {
      const detail = await res.text();
      throw new HttpError(
        `Kafka REST error ${res.status}`,
        res.status,
        detail
      );
    }
    return res;
  }
}

