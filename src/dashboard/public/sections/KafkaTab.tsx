import React, { useMemo, useState } from "react";
import {
  useKafkaTopicMetrics,
  useKafkaTopicSample,
  useKafkaTopics,
} from "../api/hooks";
import { Card } from "../components/Card";

export function KafkaTab() {
  const { data, isLoading, isError } = useKafkaTopics();
  const [selectedTopic, setSelectedTopic] = useState<string | undefined>();
  const metrics = useKafkaTopicMetrics(selectedTopic);
  const sample = useKafkaTopicSample(selectedTopic, 50);

  const records = useMemo(() => {
    const list = sample.data?.records ?? [];
    // Show newest records first, then by partition.
    return [...list].sort((a, b) => {
      if (a.offset === b.offset) return b.partition - a.partition;
      return b.offset - a.offset;
    });
  }, [sample.data]);

  const columns = useMemo(() => {
    const base = ["partition", "offset", "timestamp"];
    const valueKeys = new Set<string>();

    for (const record of records) {
      const value = record.value;
    if (value && typeof value === "object") {
        for (const key of Object.keys(value as Record<string, unknown>)) {
          valueKeys.add(key);
        }
      }
    }

    return [...base, ...Array.from(valueKeys).sort()];
  }, [records]);

  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
      <Card title="Topics" className="lg:col-span-1">
        {isLoading && <p className="text-sm text-slate-600">Loading topics…</p>}
        {isError && (
          <p className="text-sm text-red-600">
            Failed to load topics. Ensure Kafka REST proxy is reachable.
          </p>
        )}
        <div className="space-y-2">
          {data?.topics.map((topic) => (
            <button
              key={topic.name}
              onClick={() => setSelectedTopic(topic.name)}
              className={`flex w-full items-center justify-between rounded border px-3 py-2 text-left text-sm ${
                selectedTopic === topic.name
                  ? "border-slate-400 bg-slate-50"
                  : "border-slate-200 hover:border-slate-300"
              }`}
            >
              <span className="font-semibold text-slate-800">{topic.name}</span>
              <span className="text-xs text-slate-500">
                {topic.partitions}p · {topic.replication}x
              </span>
            </button>
          ))}
          {!data?.topics?.length && (
            <p className="text-sm text-slate-600">No topics discovered.</p>
          )}
        </div>
      </Card>

      <div className="lg:col-span-2 space-y-4">
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

        <Card
          title="Recent Messages (polled)"
          actions={
            selectedTopic ? (
              <span className="text-xs text-slate-500">
                Refreshing every 1s
              </span>
            ) : null
          }
        >
          {selectedTopic ? (
            <div className="overflow-auto">
              <table className="min-w-full text-left text-xs">
                <thead>
                  <tr className="border-b border-slate-200 bg-slate-50">
                    {columns.map((col) => (
                      <th
                        key={col}
                        className="px-3 py-2 font-semibold uppercase tracking-wide text-slate-600"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {records.map((record, idx) => (
                    <tr
                      key={`${record.partition}-${record.offset}-${idx}`}
                      className="border-b border-slate-100 last:border-0"
                    >
                      <td className="px-3 py-2">{record.partition}</td>
                      <td className="px-3 py-2">{record.offset}</td>
                      <td className="px-3 py-2">
                        {record.timestamp ?? "—"}
                      </td>
                      {columns.slice(3).map((key) => {
                        const value =
                          record.value && typeof record.value === "object"
                            ? (record.value as Record<string, unknown>)[key]
                            : undefined;
                        return (
                          <td key={key} className="px-3 py-2">
                            {value !== undefined ? String(value) : ""}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
              {!records.length && (
                <p className="py-3 text-sm text-slate-600">
                  No recent messages yet.
                </p>
              )}
            </div>
          ) : (
            <p className="text-sm text-slate-600">Select a topic.</p>
          )}
        </Card>
      </div>
    </div>
  );
}

