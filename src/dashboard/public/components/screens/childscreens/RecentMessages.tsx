import React, { useMemo } from "react";
import { useKafkaTopicSample } from "../../../api/hooks";
import { Card } from "../../props/Card";

interface RecentMessagesProps {
  selectedTopic: string | undefined;
}

export function RecentMessages({ selectedTopic }: RecentMessagesProps) {
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
  );
}
