import React from "react";
import { LiveBikeTripRate } from "../childscreens/LiveBikeTripRate";
import { LiveTaxiTripRate } from "../childscreens/LiveTaxiTripRate";
import { LiveWeatherChart } from "../childscreens/LiveWeatherChart";
import { useClockState } from "../../../api/hooks";

export function LiveChartsTab() {
  const { data: clockState, isLoading: clockLoading } = useClockState();

  const simulationTime = clockState?.current_time
    ? new Date(clockState.current_time).toLocaleString()
    : "—";

  const speed = clockState?.speed ?? 0;

  return (
    <div className="space-y-6">
      {/* Section header */}
      <div className="border-b border-slate-200 pb-4">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">Live Analytics</h2>
            <p className="mt-1 text-sm text-slate-500">
              Real-time streaming data from Kafka topics with live aggregation
            </p>
          </div>
          
          {/* Simulation clock status */}
          <div className="flex flex-wrap items-center gap-2">
            {clockLoading ? (
              <div className="flex items-center gap-2 rounded-lg bg-slate-100 px-3 py-2 text-xs text-slate-500">
                <span className="inline-block h-3 w-3 animate-spin rounded-full border-2 border-slate-300 border-t-blue-500" />
                Connecting...
              </div>
            ) : (
              <>
                <div className="flex items-center gap-2 rounded-lg bg-indigo-50 px-3 py-2 text-xs">
                  <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-indigo-500" />
                  <span className="font-medium text-indigo-700">Simulation:</span>
                  <span className="text-indigo-600">{simulationTime}</span>
                </div>
                <div className={`rounded-lg px-3 py-2 text-xs ${
                  speed > 0 
                    ? "bg-emerald-50 text-emerald-700" 
                    : "bg-amber-50 text-amber-700"
                }`}>
                  <span className="font-medium">Speed:</span>{" "}
                  <span>{speed > 0 ? `${speed}x` : "Paused"}</span>
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      {/* Info banner when paused */}
      {speed === 0 && !clockLoading && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-4">
          <div className="flex items-start gap-3">
            <span className="text-2xl">⏸️</span>
            <div>
              <h3 className="font-semibold text-amber-800">Simulation Paused</h3>
              <p className="text-sm text-amber-700">
                The data streamers are not currently producing events. Go to the{" "}
                <span className="font-medium">Home</span> tab to adjust the simulation speed,
                or the charts will update once streaming resumes.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Main charts grid */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <LiveBikeTripRate />
        <LiveTaxiTripRate />
      </div>

      {/* Weather chart full width */}
      <div className="grid grid-cols-1">
        <LiveWeatherChart />
      </div>

      {/* Help text */}
      <div className="rounded-lg bg-slate-50 p-4 text-xs text-slate-600">
        <h4 className="mb-1 font-semibold text-slate-700">How it works</h4>
        <p>
          These charts poll Kafka topics every second and aggregate incoming messages by offset 
          to avoid double-counting. Each chart shows a rolling window of the last ~60 seconds 
          of events grouped into 3-5 second buckets. The aggregation happens client-side using 
          the message offset as a unique identifier per partition.
        </p>
      </div>
    </div>
  );
}

