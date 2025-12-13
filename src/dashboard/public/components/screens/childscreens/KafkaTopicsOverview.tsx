import React, { useMemo, useState } from "react";
import { useKafkaTopics } from "../../../api/hooks";
import { Card } from "../../props/Card";
import { StatusPill } from "../../props/StatusPill";
import { STREAM_TOPIC_NAMES } from "../../../references/PathConfig";

export function KafkaTopicsOverview() {
  const { data: kafkaTopics, isLoading: topicsLoading } = useKafkaTopics();
  const [topicQuery, setTopicQuery] = useState("");

  const topicStats = useMemo(() => {
    const topics = kafkaTopics?.topics ?? [];
    const internal = topics.filter((t) => t.internal).length;
    const external = topics.filter((t) => !t.internal).length;
    const partitions = topics.reduce((acc, t) => acc + (t.partitions ?? 0), 0);
    const streams = topics.filter((t) =>
      STREAM_TOPIC_NAMES.includes(t.name)
    ).length;

    return {
      total: topics.length,
      internal,
      external,
      partitions,
      streams,
    };
  }, [kafkaTopics?.topics]);

  const filteredTopics = useMemo(() => {
    const topics = kafkaTopics?.topics ?? [];
    const query = topicQuery.trim().toLowerCase();
    if (!query) return topics;
    return topics.filter((t) => t.name.toLowerCase().includes(query));
  }, [kafkaTopics?.topics, topicQuery]);

  return (
    <Card
      title="Kafka Topics Overview"
      className="h-full"
      actions={
        <div className="flex items-center gap-2">
          <input
            type="search"
            value={topicQuery}
            onChange={(e) => setTopicQuery(e.target.value)}
            placeholder="Filter topics"
            className="w-40 rounded-md border border-slate-200 px-3 py-1.5 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400"
            aria-label="Filter Kafka topics"
          />
        </div>
      }
    >
      <div className="space-y-3">
        <div className="grid grid-cols-2 gap-2 rounded-md bg-slate-50 p-3 text-xs font-semibold text-slate-600 sm:grid-cols-4">
          <div>Total: {topicStats.total}</div>
          <div>Partitions: {topicStats.partitions}</div>
          <div>External: {topicStats.external}</div>
          <div>Internal: {topicStats.internal}</div>
        </div>

        {topicsLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((n) => (
              <div
                key={n}
                className="h-14 animate-pulse rounded-md bg-slate-100"
              />
            ))}
          </div>
        ) : filteredTopics.length ? (
          <div className="space-y-2">
            {filteredTopics.map((topic) => (
              <div
                key={topic.name}
                className="grid grid-cols-1 gap-y-2 rounded-md border border-slate-100 px-3 py-2 sm:grid-cols-5 sm:items-center sm:gap-3"
              >
                <div className="sm:col-span-2">
                  <div className="text-sm font-semibold text-slate-800">
                    {topic.name}
                  </div>
                  <div className="text-xs text-slate-500">
                    {topic.partitions} partitions · {topic.replication}x repl
                  </div>
                </div>
                <div className="text-sm text-slate-700">
                  {topic.partitions} partitions
                </div>
                <div className="text-sm text-slate-700">
                  {topic.replication}x replication
                </div>
                <div className="flex items-center justify-end sm:justify-start">
                  <StatusPill
                    label={topic.internal ? "Internal" : "External"}
                    variant={topic.internal ? "info" : "success"}
                  />
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-slate-600">
            No topics match your filter.
          </p>
        )}
      </div>
    </Card>
  );
}
