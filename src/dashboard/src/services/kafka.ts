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
  private readonly v3Base = `${env.kafkaRestUrl.replace(/\/$/, "")}/v3`;
  private readonly v2Base = env.kafkaRestUrl.replace(/\/$/, "");
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

    const data = await fetchJson<{ data: { cluster_id: string }[] }>(
      `${this.v3Base}/clusters`
    );
    const id = data.data?.[0]?.cluster_id;
    if (!id) throw new Error("Kafka cluster_id not found");
    this.discoveredClusterId = id;
    return id;
  }

  async listTopics(): Promise<KafkaTopicItem[]> {
    const clusterId = await this.getClusterId();
    const result = await fetchJson<KafkaTopicResponse>(
      `${this.v3Base}/clusters/${clusterId}/topics`
    );
    return result.data ?? [];
  }

  async getTopicDetail(topic: string): Promise<KafkaTopicDetail> {
    const clusterId = await this.getClusterId();
    const detail = await fetchJson<{
      topic_name: string;
      partitions_count: number;
      replication_factor: number;
      is_internal: boolean;
    }>(`${this.v3Base}/clusters/${clusterId}/topics/${topic}`);

    return {
      topic_name: detail.topic_name,
      partitions_count: detail.partitions_count,
      replication_factor: detail.replication_factor,
      is_internal: detail.is_internal,
    };
  }

  async getTopicSample(topic: string, limit = env.kafkaSampleLimit) {
    const headers = {
      "Content-Type": "application/vnd.kafka.v2+json",
      Accept: "application/vnd.kafka.v2+json",
    };
    const maxBuffer = Math.max(limit * 2, 100);

    let state = this.sampleConsumers.get(topic);

    console.log("[KafkaSample] request", {
      topic,
      limit,
      hasState: Boolean(state),
      initialized: state?.initialized ?? false,
      bufferSize: state?.buffer.length ?? 0,
    });

    // Lazily create a long-lived consumer per topic and subscribe once.
    if (!state) {
      // Use a fresh consumer group so we always start from the latest offset
      // and only ever keep a rolling tail of recent messages.
      const group = `dashboard-sample-${topic}-${crypto.randomUUID()}`;
      const creationRes = await this.safeFetch(
        `${this.v2Base}/consumers/${group}`,
        {
          method: "POST",
          headers,
          body: JSON.stringify({
            format: "avro",
            // Start from the current end of the log; we only care about a live tail.
            "auto.offset.reset": "latest",
          }),
        }
      );

      const creationBody = (await creationRes.json()) as {
        base_uri?: string;
        instance_id?: string;
      };

      if (!creationBody.instance_id) {
        throw new HttpError(
          "Kafka consumer creation failed (missing instance_id)",
          500,
          creationBody
        );
      }

      // IMPORTANT: don't use base_uri host (it points to internal DNS that the
      // dashboard can't reach). Always build the instance URL from our own v2Base.
      const consumerBase = `${this.v2Base}/consumers/${group}/instances/${creationBody.instance_id}`;

      console.log("[KafkaSample] created consumer", {
        topic,
        group,
        instanceId: creationBody.instance_id,
        consumerBase,
      });

      await this.safeFetch(`${consumerBase}/subscription`, {
        method: "POST",
        headers,
        body: JSON.stringify({ topics: [topic] }),
      });

      state = {
        consumerBase,
        buffer: [],
        initialized: false,
      };
      this.sampleConsumers.set(topic, state);
    }

    const pollOnce = async () => {
      const recordsRes = await fetch(
        `${state!.consumerBase}/records?timeout=1000&max_bytes=51200`,
        {
          headers: {
            Accept: "application/vnd.kafka.avro.v2+json",
          },
        }
      );

      const recordsBody = await recordsRes.text();
      if (!recordsRes.ok) {
        throw new HttpError(
          `Kafka poll failed: ${recordsRes.status}`,
          recordsRes.status,
          recordsBody
        );
      }

      const batch = (JSON.parse(recordsBody) as KafkaRecord[]) ?? [];
      if (batch.length) {
        const first = batch[0];
        const last = batch[batch.length - 1];
        console.log("[KafkaSample] poll batch", {
          topic,
          batchSize: batch.length,
          firstOffset: first.offset,
          lastOffset: last.offset,
        });
      } else {
        console.log("[KafkaSample] poll empty batch", { topic });
      }
      if (batch.length) {
        state!.buffer.push(...batch);
        if (state!.buffer.length > maxBuffer) {
          state!.buffer = state!.buffer.slice(-maxBuffer);
        }
      }
      return batch;
    };

    if (!state.initialized) {
      console.log("[KafkaSample] first poll for consumer", {
        topic,
        maxBuffer,
      });
      await pollOnce();
      state.initialized = true;
    } else {
      // Subsequent calls just top up with any new records.
      await pollOnce();
    }

    const sorted = [...state.buffer].sort((a, b) => {
      if (a.offset === b.offset) return a.partition - b.partition;
      return a.offset - b.offset;
    });

    const slice = sorted.slice(-limit);

    console.log("[KafkaSample] response slice", {
      topic,
      limit,
      bufferSize: state.buffer.length,
      returned: slice.length,
      firstOffset: slice[0]?.offset,
      lastOffset: slice[slice.length - 1]?.offset,
    });

    return slice;
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

