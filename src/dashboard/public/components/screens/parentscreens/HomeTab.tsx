import React from "react";
import { useClockState } from "../../../api/hooks";
import { StatusPill } from "../../props/StatusPill";
import { QuickLinks } from "../childscreens/QuickLinks";
import { SimulationClock } from "../childscreens/SimulationClock";
import { KafkaTopicsOverview } from "../childscreens/KafkaTopicsOverview";

export function HomeTab() {
  const { data: clock, isLoading: clockLoading } = useClockState();

  const isPaused = (clock?.speed ?? 0) === 0;

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

      <QuickLinks />

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <SimulationClock />
        <KafkaTopicsOverview />
      </div>
    </div>
  );
}
