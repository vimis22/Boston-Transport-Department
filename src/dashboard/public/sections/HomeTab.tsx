import React, { useMemo, useState } from "react";
import {
  useClockState,
  useKafkaTopics,
  useSetClockSpeed,
  useSetClockTime,
} from "../api/hooks";
import { Card } from "../components/Card";

const SPEED_PRESETS = [0.5, 1, 2, 5, 10, 50, 100, 300];

export function HomeTab() {
  const { data: clock, isLoading: clockLoading } = useClockState();
  const setSpeed = useSetClockSpeed();
  const setTime = useSetClockTime();
  const { data: kafkaTopics } = useKafkaTopics();
  const [timeInput, setTimeInput] = useState("");
  const [speedInput, setSpeedInput] = useState("");

  const formattedTime = useMemo(() => {
    if (!clock?.current_time) return "—";
    try {
      // Always display simulation time normalized to ISO 8601.
      return new Date(clock.current_time).toISOString();
    } catch {
      return clock.current_time;
    }
  }, [clock?.current_time]);

  const handleToggle = async () => {
    const nextSpeed = clock?.speed === 0 ? 1 : 0;
    await setSpeed.mutateAsync(nextSpeed);
  };

  const handleSetSpeed = async (value: number) => {
    await setSpeed.mutateAsync(value);
  };

  const handleSetTime = async () => {
    const value = timeInput.trim() || clock?.current_time;
    if (!value) return;
    await setTime.mutateAsync(value);
    setTimeInput("");
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        <Card title="Simulation Clock">
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-600">Run ID</span>
              <span className="text-xs font-mono text-slate-700">
                {clock?.run_id ?? "—"}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-600">Current Time</span>
              <span className="text-sm font-semibold text-slate-800">
                {clockLoading ? "Loading..." : formattedTime}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-600">Speed</span>
              <span className="text-sm font-semibold text-slate-800">
                {clock ? `${clock.speed.toFixed(2)}x` : "—"}
              </span>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={handleToggle}
                className="rounded-md bg-slate-900 px-3 py-2 text-sm font-semibold text-white hover:bg-slate-800"
              >
                {clock?.speed === 0 ? "Play" : "Pause"}
              </button>
              <div className="flex flex-wrap gap-2">
                {SPEED_PRESETS.map((s) => (
                  <button
                    key={s}
                    onClick={() => handleSetSpeed(s)}
                    className="rounded-md border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-800 hover:border-slate-300"
                  >
                    {s}x
                  </button>
                ))}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <label className="text-xs font-medium text-slate-600">
                Custom speed
              </label>
              <input
                type="number"
                step="0.1"
                min="0"
                placeholder={clock ? clock.speed.toString() : "1.0"}
                value={speedInput}
                onChange={(e) => setSpeedInput(e.target.value)}
                className="w-24 rounded-md border border-slate-200 px-2 py-1 text-xs"
              />
              <button
                onClick={async () => {
                  const value = parseFloat(speedInput);
                  if (!Number.isFinite(value) || value < 0) return;
                  await setSpeed.mutateAsync(value);
                  setSpeedInput("");
                }}
                className="rounded-md bg-slate-900 px-3 py-1 text-xs font-semibold text-white hover:bg-slate-800"
              >
                Apply
              </button>
            </div>
            <div className="flex flex-col gap-2">
              <label className="text-xs font-medium text-slate-600">
                Set time (ISO 8601)
              </label>
              <div className="flex gap-2">
                <input
                  type="text"
                  placeholder={clock?.current_time ?? "2018-01-01T00:00:00Z"}
                  value={timeInput}
                  onChange={(e) => setTimeInput(e.target.value)}
                  className="w-full rounded-md border border-slate-200 px-3 py-2 text-sm"
                />
                <button
                  onClick={handleSetTime}
                  className="rounded-md bg-slate-900 px-3 py-2 text-sm font-semibold text-white hover:bg-slate-800"
                >
                  Update
                </button>
              </div>
            </div>
          </div>
        </Card>

        <Card title="Kafka Topics Overview">
          <div className="space-y-3">
            {kafkaTopics?.topics?.length ? (
              kafkaTopics.topics.map((topic) => (
                <div
                  key={topic.name}
                  className="flex items-center justify-between rounded border border-slate-100 px-3 py-2"
                >
                  <div>
                    <div className="text-sm font-semibold text-slate-800">
                      {topic.name}
                    </div>
                    <div className="text-xs text-slate-500">
                      {topic.partitions} partitions · {topic.replication}x repl
                    </div>
                  </div>
                  <span className="text-xs uppercase text-slate-500">
                    {topic.internal ? "internal" : "external"}
                  </span>
                </div>
              ))
            ) : (
              <p className="text-sm text-slate-600">No topics found yet.</p>
            )}
          </div>
        </Card>
      </div>
    </div>
  );
}

