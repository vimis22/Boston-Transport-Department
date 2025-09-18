import React, { useMemo } from "react";
import { Card } from "../../props/Card";
import {
  BarChart,
  Bar,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  CartesianGrid,
  ReferenceLine,
} from "recharts";
import { useHistoricalHiveQuery } from "../../../api/hooks";
import { useDateRangeFilter } from "../parentscreens/HistoricalTab";

const buildQuery = (dateFilter: string) => `
SELECT 
  FLOOR(distance_meters / 500) * 500 as distance_bucket,
  COUNT(*) as trip_count,
  ROUND(AVG(tripduration) / 60, 1) as avg_duration_min
FROM bike_weather_distance
WHERE distance_meters > 0 AND distance_meters < 8000${dateFilter}
GROUP BY FLOOR(distance_meters / 500) * 500
ORDER BY distance_bucket
`;

type DistanceData = {
  distance_bucket: number;
  trip_count: number;
  avg_duration_min: number;
};

const FALLBACK_DATA: DistanceData[] = [
  { distance_bucket: 500, trip_count: 180, avg_duration_min: 5.2 },
  { distance_bucket: 1000, trip_count: 420, avg_duration_min: 8.5 },
  { distance_bucket: 1500, trip_count: 580, avg_duration_min: 11.3 },
  { distance_bucket: 2000, trip_count: 520, avg_duration_min: 14.1 },
  { distance_bucket: 2500, trip_count: 380, avg_duration_min: 16.8 },
  { distance_bucket: 3000, trip_count: 290, avg_duration_min: 19.5 },
  { distance_bucket: 3500, trip_count: 180, avg_duration_min: 22.4 },
  { distance_bucket: 4000, trip_count: 95, avg_duration_min: 25.2 },
  { distance_bucket: 4500, trip_count: 45, avg_duration_min: 28.1 },
];

export function TripDistanceDistribution() {
  const { dateFilter } = useDateRangeFilter();
  const query = useMemo(() => buildQuery(dateFilter), [dateFilter]);
  const { data, isLoading, isError } = useHistoricalHiveQuery(query);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return isLoading ? FALLBACK_DATA : [];
    }

    return data.data.map((row) => ({
      distance_bucket: Number(row.distance_bucket ?? 0),
      trip_count: Number(row.trip_count ?? 0),
      avg_duration_min: Number(row.avg_duration_min ?? 0),
    }));
  }, [data, isLoading]);

  const avgDistance = useMemo(() => {
    if (!chartData.length) return 0;
    const totalTrips = chartData.reduce((sum, d) => sum + d.trip_count, 0);
    const weightedSum = chartData.reduce(
      (sum, d) => sum + d.distance_bucket * d.trip_count,
      0
    );
    return totalTrips > 0 ? weightedSum / totalTrips : 0;
  }, [chartData]);

  const formatDistance = (meters: number): string => {
    if (meters >= 1000) return `${(meters / 1000).toFixed(1)}km`;
    return `${meters}m`;
  };

  return (
    <Card title="Trip Distance Distribution">
      {isLoading && (
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-cyan-600" />
          Querying Hive...
        </div>
      )}
      {isError && (
        <p className="text-sm text-amber-600">
          ⚠ Could not load data. Showing sample values.
        </p>
      )}
      <p className="mb-3 text-xs text-slate-500">
        Most trips are between 1-3km • Average: {formatDistance(avgDistance)}
      </p>
      {!isLoading && chartData.length === 0 && (
        <div className="flex h-72 items-center justify-center text-sm text-slate-400">
          No data available for selected date range
        </div>
      )}
      <div style={{ width: "100%", height: chartData.length === 0 && !isLoading ? 0 : 288 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
            <defs>
              <linearGradient id="distanceGradient" x1="0" y1="0" x2="1" y2="0">
                <stop offset="0%" stopColor="#06b6d4" />
                <stop offset="100%" stopColor="#0891b2" />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
            <XAxis
              dataKey="distance_bucket"
              tickFormatter={formatDistance}
              tick={{ fontSize: 10, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              label={{
                value: "Distance",
                position: "insideBottom",
                offset: -10,
                fontSize: 11,
                fill: "#64748b",
              }}
            />
            <YAxis
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#f8fafc",
                border: "1px solid #e2e8f0",
                borderRadius: "8px",
                fontSize: "12px",
              }}
              formatter={(value: number, name: string) => {
                if (name === "trip_count") return [value.toLocaleString(), "Trips"];
                return [value, name];
              }}
              labelFormatter={(label) => `${formatDistance(Number(label))} - ${formatDistance(Number(label) + 500)}`}
            />
            <ReferenceLine
              x={avgDistance}
              stroke="#f43f5e"
              strokeDasharray="5 5"
              strokeWidth={2}
              label={{
                value: "Avg",
                position: "top",
                fontSize: 10,
                fill: "#f43f5e",
              }}
            />
            <Bar
              dataKey="trip_count"
              fill="url(#distanceGradient)"
              radius={[4, 4, 0, 0]}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

