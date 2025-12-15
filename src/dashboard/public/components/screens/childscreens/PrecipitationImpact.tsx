import React, { useMemo } from "react";
import { Card } from "../../props/Card";
import {
  ComposedChart,
  Bar,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend,
} from "recharts";
import { useHistoricalHiveQuery } from "../../../api/hooks";
import { useDateRangeFilter } from "../parentscreens/HistoricalTab";

const buildQuery = (dateFilter: string) => `
SELECT 
  CASE 
    WHEN precip_daily_mm = 0 THEN 'None'
    WHEN precip_daily_mm < 1 THEN 'Light'
    WHEN precip_daily_mm < 5 THEN 'Moderate'
    ELSE 'Heavy'
  END as precip_level,
  COUNT(*) as trip_count,
  ROUND(AVG(average_speed_kmh), 2) as avg_speed,
  ROUND(AVG(tripduration) / 60, 1) as avg_duration_min
FROM bike_weather_distance
WHERE average_speed_kmh > 0 AND average_speed_kmh < 50${dateFilter}
GROUP BY 
  CASE 
    WHEN precip_daily_mm = 0 THEN 'None'
    WHEN precip_daily_mm < 1 THEN 'Light'
    WHEN precip_daily_mm < 5 THEN 'Moderate'
    ELSE 'Heavy'
  END
ORDER BY 
  CASE precip_level
    WHEN 'None' THEN 1
    WHEN 'Light' THEN 2
    WHEN 'Moderate' THEN 3
    WHEN 'Heavy' THEN 4
  END
`;

type PrecipData = {
  precip_level: string;
  trip_count: number;
  avg_speed: number;
  avg_duration_min: number;
};

const FALLBACK_DATA: PrecipData[] = [
  { precip_level: "None", trip_count: 850, avg_speed: 11.8, avg_duration_min: 12.5 },
  { precip_level: "Light", trip_count: 420, avg_speed: 10.9, avg_duration_min: 13.2 },
  { precip_level: "Moderate", trip_count: 180, avg_speed: 9.8, avg_duration_min: 14.8 },
  { precip_level: "Heavy", trip_count: 45, avg_speed: 8.5, avg_duration_min: 16.1 },
];

export function PrecipitationImpact() {
  const { dateFilter } = useDateRangeFilter();
  const query = useMemo(() => buildQuery(dateFilter), [dateFilter]);
  const { data, isLoading, isError } = useHistoricalHiveQuery(query);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return isLoading ? FALLBACK_DATA : [];
    }

    // Sort the data by precipitation level
    const levelOrder = { None: 0, Light: 1, Moderate: 2, Heavy: 3 };
    return data.data
      .map((row) => ({
        precip_level: String(row.precip_level ?? "Unknown"),
        trip_count: Number(row.trip_count ?? 0),
        avg_speed: Number(row.avg_speed ?? 0),
        avg_duration_min: Number(row.avg_duration_min ?? 0),
      }))
      .sort(
        (a, b) =>
          (levelOrder[a.precip_level as keyof typeof levelOrder] ?? 99) -
          (levelOrder[b.precip_level as keyof typeof levelOrder] ?? 99)
      );
  }, [data, isLoading]);

  return (
    <Card title="Precipitation Impact on Cycling">
      {isLoading && (
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-violet-600" />
          Querying Hive...
        </div>
      )}
      {isError && (
        <p className="text-sm text-amber-600">
          ⚠ Could not load data. Showing sample values.
        </p>
      )}
      <p className="mb-2 text-xs text-slate-500">
        Trip volume vs average speed by precipitation level
      </p>
      <div className="mb-3 flex flex-wrap gap-2 text-xs">
        <span className="rounded bg-slate-100 px-2 py-0.5 text-slate-600">
          <strong>None:</strong> 0mm
        </span>
        <span className="rounded bg-blue-50 px-2 py-0.5 text-blue-600">
          <strong>Light:</strong> &lt;1mm
        </span>
        <span className="rounded bg-violet-50 px-2 py-0.5 text-violet-600">
          <strong>Moderate:</strong> 1-5mm
        </span>
        <span className="rounded bg-indigo-50 px-2 py-0.5 text-indigo-600">
          <strong>Heavy:</strong> &gt;5mm
        </span>
      </div>
      {!isLoading && chartData.length === 0 && (
        <div className="flex h-72 items-center justify-center text-sm text-slate-400">
          No data available for selected date range
        </div>
      )}
      <div style={{ width: "100%", height: chartData.length === 0 && !isLoading ? 0 : 288 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData} margin={{ top: 10, right: 30, bottom: 20, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="precip_level"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
            />
            <YAxis
              yAxisId="left"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              label={{
                value: "Trip Count",
                angle: -90,
                position: "insideLeft",
                fontSize: 11,
                fill: "#64748b",
              }}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              domain={[0, "dataMax + 2"]}
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              label={{
                value: "Speed (km/h)",
                angle: 90,
                position: "insideRight",
                fontSize: 11,
                fill: "#64748b",
              }}
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
                if (name === "avg_speed") return [`${value.toFixed(1)} km/h`, "Avg Speed"];
                return [value, name];
              }}
            />
            <Legend
              wrapperStyle={{ fontSize: "11px", paddingTop: "10px" }}
              formatter={(value) => {
                if (value === "trip_count") return "Trip Count";
                if (value === "avg_speed") return "Avg Speed";
                return value;
              }}
            />
            <Bar
              yAxisId="left"
              dataKey="trip_count"
              fill="#8b5cf6"
              fillOpacity={0.8}
              radius={[4, 4, 0, 0]}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="avg_speed"
              stroke="#f59e0b"
              strokeWidth={3}
              dot={{ fill: "#f59e0b", strokeWidth: 2, r: 5 }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

