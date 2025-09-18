import { Elysia } from "elysia";
import { KafkaService } from "../services/kafka";

const service = new KafkaService();

export const kafkaRoutes = new Elysia({ prefix: "/kafka" })
  .get("/topics", async () => {
    const topics = await service.listTopics();
    return {
      knownTopics: service.getKnownTopics(),
      topics: topics.map((t) => ({
        name: t.topic_name,
        internal: Boolean(t.is_internal),
        partitions: t.partitions_count ?? 0,
        replication: t.replication_factor ?? 0,
      })),
    };
  })
  .get("/topics/:topic/metrics", async ({ params }) => {
    const info = await service.getTopicDetail(params.topic);
    return {
      name: info.topic_name,
      partitions: info.partitions_count,
      replication: info.replication_factor,
      internal: info.is_internal,
    };
  })
  .get("/topics/:topic/sample", async ({ params, query }) => {
    const limit = query.limit ? Number(query.limit) : undefined;
    const records = await service.getTopicSample(params.topic, limit);
    return { records };
  });

