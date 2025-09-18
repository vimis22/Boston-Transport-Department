import React, { useState } from "react";
import { TopicsList } from "../childscreens/TopicsList";
import { TopicMetrics } from "../childscreens/TopicMetrics";
import { RecentMessages } from "../childscreens/RecentMessages";

export function KafkaTab() {
  const [selectedTopic, setSelectedTopic] = useState<string | undefined>();

  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
      <TopicsList
        selectedTopic={selectedTopic}
        onTopicSelect={setSelectedTopic}
      />

      <div className="lg:col-span-2 space-y-4">
        <TopicMetrics selectedTopic={selectedTopic} />
        <RecentMessages selectedTopic={selectedTopic} />
      </div>
    </div>
  );
}
