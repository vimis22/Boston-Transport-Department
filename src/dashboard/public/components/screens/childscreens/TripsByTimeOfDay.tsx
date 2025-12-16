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
  Cell,
} from "recharts";
import { useHistoricalHiveQuery } from "../../../api/hooks";
import { useDateRangeFilter } from "../parentscreens/HistoricalTab";

const buildQuery = (dateFilter: string) => `
SELECT 
  HOUR(starttime) as hour_of_day,
  COUNT(*) as trip_count,
  ROUND(AVG(tripduration) / 60, 1) as avg_duration_min
FROM bike_weather_distance
WHERE average_speed_kmh > 0${dateFilter}
GROUP BY HOUR(starttime)
ORDER BY hour_of_day
`;

type TimeData = {
  hour_of_day: number;
  trip_count: number;
  avg_duration_min: number;
};

const FALLBACK_DATA: TimeData[] = Array.from({ length: 24 }, (_, i) => ({
  hour_of_day: i,
  trip_count: Math.round(50 + 200 * Math.sin((i - 6) * Math.PI / 12) ** 2 * (i >= 6 && i <= 22 ? 1 : 0.3)),
  avg_duration_min: 10 + Math.random() * 5,
}));

// Color scale from cool (night) to warm (day)
const getBarColor = (hour: number): string => {
  if (hour >= 6 && hour < 10) return "#f97316"; // Morning rush - orange
  if (hour >= 10 && hour < 16) return "#eab308"; // Midday - yellow
  if (hour >= 16 && hour < 20) return "#ef4444"; // Evening rush - red
  return "#6366f1"; // Night - indigo
};

export function TripsByTimeOfDay() {
  const { dateFilter } = useDateRangeFilter();
  const query = useMemo(() => buildQuery(dateFilter), [dateFilter]);
  const { data, isLoading, isError } = useHistoricalHiveQuery(query);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return isLoading ? FALLBACK_DATA : [];
    }

    const result = data.data.map((row) => ({
      hour_of_day: Number(row.hour_of_day ?? 0),
      trip_count: Number(row.trip_count ?? 0),
      avg_duration_min: Number(row.avg_duration_min ?? 0),
    }));

    // Sort by hour
    return result.sort((a, b) => a.hour_of_day - b.hour_of_day);
  }, [data, isLoading]);

  const formatHour = (hour: number): string => {
    if (hour === 0) return "12am";
    if (hour === 12) return "12pm";
    return hour < 12 ? `${hour}am` : `${hour - 12}pm`;
  };

  return (
    <Card title="Trips by Hour of Day">
      {isLoading && (
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-orange-600" />
          Querying Hive...
        </div>
      )}
      {isError && (
        <p className="text-sm text-amber-600">
          ⚠ Could not load data. Showing sample values.
        </p>
      )}
      <div className="mb-3 flex flex-wrap gap-3 text-xs text-slate-500">
        <span className="flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-orange-500" />
          Morning Rush (6-10)
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-yellow-500" />
          Midday (10-16)
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-red-500" />
          Evening Rush (16-20)
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-2 w-2 rounded-full bg-indigo-500" />
          Night
        </span>
      </div>
      {!isLoading && chartData.length === 0 && (
        <div className="flex h-72 items-center justify-center text-sm text-slate-400">
          No data available for selected date range
        </div>
      )}
      <div style={{ width: "100%", height: chartData.length === 0 && !isLoading ? 0 : 288 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
            <XAxis
              dataKey="hour_of_day"
              tickFormatter={formatHour}
              tick={{ fontSize: 10, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              interval={2}
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
              labelFormatter={(label) => formatHour(Number(label))}
            />
            <Bar dataKey="trip_count" radius={[4, 4, 0, 0]}>
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={getBarColor(entry.hour_of_day)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

