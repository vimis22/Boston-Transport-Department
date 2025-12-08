import React, { useMemo, useState } from "react";
import clsx from "clsx";
import {
  useClockState,
  useKafkaTopics,
  useSetClockSpeed,
  useSetClockTime,
} from "../api/hooks";
import { Card } from "../components/Card";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";

const SPEED_PRESETS = [0.5, 1, 2, 5, 10, 50, 100, 300];
const STREAM_TOPIC_NAMES = ["bike-data", "taxi-data", "weather-data"];

export function HomeTab() {
  const { data: clock, isLoading: clockLoading } = useClockState();
  const setSpeed = useSetClockSpeed();
  const setTime = useSetClockTime();
  const { data: kafkaTopics, isLoading: topicsLoading } = useKafkaTopics();

  const [timeInput, setTimeInput] = useState("");
  const [speedInput, setSpeedInput] = useState("");
  const [topicQuery, setTopicQuery] = useState("");
  const [speedError, setSpeedError] = useState("");
  const [timeError, setTimeError] = useState("");

  const formattedTime = useMemo(() => {
    if (!clock?.current_time) return "—";
    try {
      return new Date(clock.current_time).toISOString();
    } catch {
      return clock.current_time;
    }
  }, [clock?.current_time]);

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

  const isPaused = (clock?.speed ?? 0) === 0;

  const handleToggle = async () => {
    setSpeedError("");
    setTimeError("");
    const nextSpeed = isPaused ? 1 : 0;
    await setSpeed.mutateAsync(nextSpeed);
  };

  const handleSetSpeed = async (value: number) => {
    setSpeedError("");
    await setSpeed.mutateAsync(value);
  };

  const handleApplyCustomSpeed = async () => {
    const value = parseFloat(speedInput);
    if (!Number.isFinite(value) || value < 0) {
      setSpeedError("Enter a non-negative number.");
      return;
    }
    setSpeedError("");
    await setSpeed.mutateAsync(value);
    setSpeedInput("");
  };

  const handleSetTime = async () => {
    const value = timeInput.trim() || clock?.current_time;
    if (!value) return;
    try {
      new Date(value).toISOString();
    } catch {
      setTimeError("Use ISO 8601, e.g., 2018-08-10T12:36:09Z");
      return;
    }
    setTimeError("");
    await setTime.mutateAsync(value);
    setTimeInput("");
  };

  return (
    <div className="mx-auto max-w-6xl space-y-6 px-4 py-6">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Home
          </p>
          <h1 className="text-2xl font-semibold text-slate-900">
            Boston Transport Dashboard
          </h1>
          <p className="text-sm text-slate-600">
            Timemanager controls, Kafka visibility, and live simulation
            telemetry in one place.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <StatusPill
            label={isPaused ? "Paused" : "Running"}
            variant={isPaused ? "warning" : "success"}
          />
          <div className="text-xs text-slate-500">
            {clockLoading
              ? "Clock loading..."
              : `Speed ${clock ? clock.speed.toFixed(2) : "—"}x`}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Simulation Time"
          value={clockLoading ? "Loading..." : formattedTime}
          hint="Normalized ISO 8601"
        />
        <StatCard
          label="Speed"
          value={clock ? `${clock.speed.toFixed(2)}x` : "—"}
          hint="Active playback rate"
        />
        <StatCard
          label="Kafka Topics"
          value={topicsLoading ? "Loading..." : topicStats.total}
          hint={`${topicStats.external} external · ${topicStats.internal} internal`}
        />
        <StatCard
          label="Active Feeds"
          value={topicStats.streams}
          hint="bike, taxi, weather"
        />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card title="Simulation Clock" className="h-full">
          <div className="space-y-4">
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-md bg-slate-50 px-3 py-2">
              <div className="flex items-center gap-2">
                <StatusPill
                  label={isPaused ? "Paused" : "Running"}
                  variant={isPaused ? "warning" : "success"}
                />
                <div className="text-sm font-semibold text-slate-800">
                  {clock ? `${clock.speed.toFixed(2)}x` : "—"}
                </div>
              </div>
              <div className="text-xs font-mono text-slate-600">
                {clock?.run_id ? `Run ${clock.run_id}` : "Run ID unavailable"}
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="text-sm font-semibold text-slate-800">
                  Current Time
                </div>
                <div className="text-sm text-slate-700">
                  {clockLoading ? "Loading..." : formattedTime}
                </div>
              </div>
              <div className="flex items-center justify-between">
                <div className="text-sm font-semibold text-slate-800">
                  Current Speed
                </div>
                <div className="text-sm text-slate-700">
                  {clock ? `${clock.speed.toFixed(2)}x` : "—"}
                </div>
              </div>
            </div>

            <div className="space-y-3 rounded-md border border-slate-100 p-3">
              <div className="flex items-center justify-between">
                <div className="text-sm font-semibold text-slate-800">
                  Playback
                </div>
                <button
                  onClick={handleToggle}
                  className="rounded-md bg-slate-900 px-3 py-2 text-sm font-semibold text-white transition hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-slate-400"
                >
                  {isPaused ? "Play" : "Pause"}
                </button>
              </div>

              <div className="space-y-2">
                <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                  Speed Presets
                </div>
                <div className="flex flex-wrap gap-2">
                  {SPEED_PRESETS.map((s) => {
                    const isActive = clock ? Math.abs(clock.speed - s) < 0.001 : false;
                    return (
                      <button
                        key={s}
                        onClick={() => handleSetSpeed(s)}
                        className={clsx(
                          "rounded-md border px-3 py-1 text-xs font-semibold transition focus:outline-none focus:ring-2 focus:ring-slate-400",
                          isActive
                            ? "border-slate-900 bg-slate-900 text-white"
                            : "border-slate-200 bg-white text-slate-800 hover:border-slate-300"
                        )}
                      >
                        {s}x
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                  Custom speed
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <input
                    type="number"
                    step="0.1"
                    min="0"
                    placeholder={clock ? clock.speed.toString() : "1.0"}
                    value={speedInput}
                    onChange={(e) => setSpeedInput(e.target.value)}
                    className="w-28 rounded-md border border-slate-200 px-2 py-1 text-xs focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400"
                    aria-label="Custom speed"
                  />
                  <button
                    onClick={handleApplyCustomSpeed}
                    className="rounded-md bg-slate-900 px-3 py-1 text-xs font-semibold text-white transition hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-slate-400"
                  >
                    Apply
                  </button>
                </div>
                {speedError && (
                  <p className="text-xs text-rose-600" role="alert">
                    {speedError}
                  </p>
                )}
              </div>
            </div>

            <div className="space-y-2 rounded-md border border-slate-100 p-3">
              <div className="flex items-center justify-between">
                <div className="text-sm font-semibold text-slate-800">
                  Set Time (ISO 8601)
                </div>
                <span className="text-xs text-slate-500">
                  Example: 2018-08-10T12:36:09Z
                </span>
              </div>
              <div className="flex flex-col gap-2 sm:flex-row">
                <input
                  type="text"
                  placeholder={clock?.current_time ?? "2018-08-10T12:36:09Z"}
                  value={timeInput}
                  onChange={(e) => setTimeInput(e.target.value)}
                  className="w-full rounded-md border border-slate-200 px-3 py-2 text-sm focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400"
                  aria-label="Set simulation time"
                />
                <button
                  onClick={handleSetTime}
                  className="rounded-md bg-slate-900 px-3 py-2 text-sm font-semibold text-white transition hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-slate-400"
                >
                  Update
                </button>
              </div>
              {timeError && (
                <p className="text-xs text-rose-600" role="alert">
                  {timeError}
                </p>
              )}
            </div>
          </div>
        </Card>

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
      </div>
    </div>
  );
}

