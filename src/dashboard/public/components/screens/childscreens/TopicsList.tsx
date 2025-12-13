import React from "react";
import { useKafkaTopics } from "../../../api/hooks";
import { Card } from "../../props/Card";

interface TopicsListProps {
  selectedTopic: string | undefined;
  onTopicSelect: (topicName: string) => void;
}

export function TopicsList({ selectedTopic, onTopicSelect }: TopicsListProps) {
  const { data, isLoading, isError } = useKafkaTopics();

  return (
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
            onClick={() => onTopicSelect(topic.name)}
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
  );
}
