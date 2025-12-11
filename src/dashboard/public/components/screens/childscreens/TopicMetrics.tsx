import React from "react";
import { useKafkaTopicMetrics } from "../../../api/hooks";
import { Card } from "../../props/Card";

interface TopicMetricsProps {
  selectedTopic: string | undefined;
}

export function TopicMetrics({ selectedTopic }: TopicMetricsProps) {
  const metrics = useKafkaTopicMetrics(selectedTopic);

  return (
    <Card title="Topic Metrics">
      {selectedTopic ? (
        metrics.isLoading ? (
          <p className="text-sm text-slate-600">Loading metrics…</p>
        ) : metrics.data ? (
          <div className="grid grid-cols-2 gap-3 text-sm text-slate-700">
            <div>
              <div className="text-xs uppercase text-slate-500">
                Topic
              </div>
              <div className="font-semibold">{metrics.data.name}</div>
            </div>
            <div>
              <div className="text-xs uppercase text-slate-500">
                Partitions
              </div>
              <div className="font-semibold">
                {metrics.data.partitions}
              </div>
            </div>
            <div>
              <div className="text-xs uppercase text-slate-500">
                Replication
              </div>
              <div className="font-semibold">
                {metrics.data.replication}x
              </div>
            </div>
            <div>
              <div className="text-xs uppercase text-slate-500">
                Type
              </div>
              <div className="font-semibold">
                {metrics.data.internal ? "Internal" : "User"}
              </div>
            </div>
          </div>
        ) : (
          <p className="text-sm text-slate-600">No metrics available.</p>
        )
      ) : (
        <p className="text-sm text-slate-600">Select a topic.</p>
      )}
    </Card>
  );
}
