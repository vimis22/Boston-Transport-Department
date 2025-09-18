import React, { useMemo } from "react";
import { Card } from "../../props/Card";
import { useHistoricalHiveQuery } from "../../../api/hooks";

const QUERY = `
SELECT
  ROUND(mean_speed, 2) as mean_speed,
  ROUND(median_speed, 2) as median_speed,
  ROUND(stddev_speed, 2) as stddev_speed,
  ROUND(min_speed, 2) as min_speed,
  ROUND(max_speed, 2) as max_speed,
  ROUND(mean_temp, 2) as mean_temp,
  ROUND(mean_distance, 2) as mean_distance,
  ROUND(mean_duration, 2) as mean_duration,
  total_trips
FROM weather_transport_statistics_overall
ORDER BY total_trips DESC
LIMIT 1
`;

type OverallStatsData = {
  mean_speed: number;
  median_speed: number;
  stddev_speed: number;
  min_speed: number;
  max_speed: number;
  mean_temp: number;
  mean_distance: number;
  mean_duration: number;
  total_trips: number;
};

const FALLBACK_DATA: OverallStatsData = {
  mean_speed: 11.5,
  median_speed: 11.2,
  stddev_speed: 2.3,
  min_speed: 3.5,
  max_speed: 35.2,
  mean_temp: 18.5,
  mean_distance: 1850,
  mean_duration: 580,
  total_trips: 12450,
};

export function OverallStatistics() {
  const { data, isLoading, isError } = useHistoricalHiveQuery(QUERY);

  const statsData = useMemo(() => {
    if (!data?.data?.length) {
      return isLoading ? FALLBACK_DATA : null;
    }

    const row = data.data[0];
    return {
      mean_speed: Number(row.mean_speed ?? 0),
      median_speed: Number(row.median_speed ?? 0),
      stddev_speed: Number(row.stddev_speed ?? 0),
      min_speed: Number(row.min_speed ?? 0),
      max_speed: Number(row.max_speed ?? 0),
      mean_temp: Number(row.mean_temp ?? 0),
      mean_distance: Number(row.mean_distance ?? 0),
      mean_duration: Number(row.mean_duration ?? 0),
      total_trips: Number(row.total_trips ?? 0),
    };
  }, [data, isLoading]);

  return (
    <Card title="Overall Trip Statistics">
      {isLoading && (
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-blue-600" />
          Querying Hive...
        </div>
      )}
      {isError && (
        <p className="text-sm text-amber-600">
          ⚠ Could not load data. Showing sample values.
        </p>
      )}
      <p className="mb-3 text-xs text-slate-500">
        Descriptive statistics across all bike trips in dataset
      </p>
      {!isLoading && !statsData && (
        <div className="flex h-48 items-center justify-center text-sm text-slate-400">
          No data available
        </div>
      )}
      {statsData && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
          {/* Speed Statistics */}
          <div className="rounded-lg bg-blue-50 p-3">
            <div className="text-xs font-medium text-blue-700">Mean Speed</div>
            <div className="mt-1 text-xl font-bold text-blue-900">
              {statsData.mean_speed.toFixed(1)} <span className="text-sm">km/h</span>
            </div>
          </div>

          <div className="rounded-lg bg-blue-50 p-3">
            <div className="text-xs font-medium text-blue-700">Median Speed</div>
            <div className="mt-1 text-xl font-bold text-blue-900">
              {statsData.median_speed.toFixed(1)} <span className="text-sm">km/h</span>
            </div>
          </div>

          <div className="rounded-lg bg-blue-50 p-3">
            <div className="text-xs font-medium text-blue-700">Std Dev Speed</div>
            <div className="mt-1 text-xl font-bold text-blue-900">
              {statsData.stddev_speed.toFixed(1)} <span className="text-sm">km/h</span>
            </div>
          </div>

          {/* Distance & Duration */}
          <div className="rounded-lg bg-emerald-50 p-3">
            <div className="text-xs font-medium text-emerald-700">Mean Distance</div>
            <div className="mt-1 text-xl font-bold text-emerald-900">
              {(statsData.mean_distance / 1000).toFixed(2)} <span className="text-sm">km</span>
            </div>
          </div>

          <div className="rounded-lg bg-emerald-50 p-3">
            <div className="text-xs font-medium text-emerald-700">Mean Duration</div>
            <div className="mt-1 text-xl font-bold text-emerald-900">
              {(statsData.mean_duration / 60).toFixed(1)} <span className="text-sm">min</span>
            </div>
          </div>

          {/* Temperature */}
          <div className="rounded-lg bg-amber-50 p-3">
            <div className="text-xs font-medium text-amber-700">Mean Temp</div>
            <div className="mt-1 text-xl font-bold text-amber-900">
              {statsData.mean_temp.toFixed(1)} <span className="text-sm">°C</span>
            </div>
          </div>

          {/* Speed Range */}
          <div className="rounded-lg bg-slate-100 p-3">
            <div className="text-xs font-medium text-slate-700">Min Speed</div>
            <div className="mt-1 text-lg font-bold text-slate-900">
              {statsData.min_speed.toFixed(1)} <span className="text-sm">km/h</span>
            </div>
          </div>

          <div className="rounded-lg bg-slate-100 p-3">
            <div className="text-xs font-medium text-slate-700">Max Speed</div>
            <div className="mt-1 text-lg font-bold text-slate-900">
              {statsData.max_speed.toFixed(1)} <span className="text-sm">km/h</span>
            </div>
          </div>

          {/* Total Trips */}
          <div className="rounded-lg bg-purple-50 p-3">
            <div className="text-xs font-medium text-purple-700">Total Trips</div>
            <div className="mt-1 text-xl font-bold text-purple-900">
              {statsData.total_trips.toLocaleString()}
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}
