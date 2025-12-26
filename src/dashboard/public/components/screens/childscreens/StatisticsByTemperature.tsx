import React, { useMemo } from "react";
import { Card } from "../../props/Card";
import {
  LineChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend,
} from "recharts";
import { useHistoricalHiveQuery } from "../../../api/hooks";

const QUERY = `
SELECT
  temp_bucket,
  ROUND(mean_speed, 2) as mean_speed,
  ROUND(median_speed, 2) as median_speed,
  ROUND(mean_distance, 2) as mean_distance,
  ROUND(avg_temp_in_bucket, 2) as avg_temp,
  sample_size
FROM weather_transport_statistics_by_temperature
ORDER BY
  CASE temp_bucket
    WHEN 'Below 0°C' THEN 1
    WHEN '0-10°C' THEN 2
    WHEN '10-20°C' THEN 3
    WHEN '20-30°C' THEN 4
    WHEN 'Above 30°C' THEN 5
  END
`;

type TemperatureStats = {
  temp_bucket: string;
  mean_speed: number;
  median_speed: number;
  mean_distance: number;
  avg_temp: number;
  sample_size: number;
};

const FALLBACK_DATA: TemperatureStats[] = [
  {
    temp_bucket: "Below 0°C",
    mean_speed: 9.5,
    median_speed: 9.2,
    mean_distance: 1620,
    avg_temp: -2.5,
    sample_size: 850,
  },
  {
    temp_bucket: "0-10°C",
    mean_speed: 10.8,
    median_speed: 10.5,
    mean_distance: 1750,
    avg_temp: 6.2,
    sample_size: 3200,
  },
  {
    temp_bucket: "10-20°C",
    mean_speed: 11.9,
    median_speed: 11.6,
    mean_distance: 1890,
    avg_temp: 15.5,
    sample_size: 5400,
  },
  {
    temp_bucket: "20-30°C",
    mean_speed: 11.2,
    median_speed: 10.9,
    mean_distance: 1820,
    avg_temp: 24.2,
    sample_size: 2800,
  },
  {
    temp_bucket: "Above 30°C",
    mean_speed: 9.8,
    median_speed: 9.5,
    mean_distance: 1680,
    avg_temp: 32.5,
    sample_size: 200,
  },
];

export function StatisticsByTemperature() {
  const { data, isLoading, isError } = useHistoricalHiveQuery(QUERY);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return isLoading ? FALLBACK_DATA : [];
    }

    return data.data.map((row) => ({
      temp_bucket: String(row.temp_bucket ?? "unknown"),
      mean_speed: Number(row.mean_speed ?? 0),
      median_speed: Number(row.median_speed ?? 0),
      mean_distance: Number(row.mean_distance ?? 0),
      avg_temp: Number(row.avg_temp ?? 0),
      sample_size: Number(row.sample_size ?? 0),
    }));
  }, [data, isLoading]);

  return (
    <Card title="Statistics by Temperature Range">
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
        How cycling speed and distance vary across temperature ranges
      </p>
      {!isLoading && chartData.length === 0 && (
        <div className="flex h-72 items-center justify-center text-sm text-slate-400">
          No data available
        </div>
      )}
      <div
        style={{
          width: "100%",
          height: chartData.length === 0 && !isLoading ? 0 : 288,
        }}
      >
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={chartData}
            margin={{ top: 10, right: 20, bottom: 20, left: 10 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="temp_bucket"
              tick={{ fontSize: 10, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              angle={-15}
              textAnchor="end"
              height={60}
            />
            <YAxis
              yAxisId="left"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              label={{
                value: "Speed (km/h)",
                angle: -90,
                position: "insideLeft",
                style: { fontSize: 11, fill: "#64748b" },
              }}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              label={{
                value: "Distance (km)",
                angle: 90,
                position: "insideRight",
                style: { fontSize: 11, fill: "#64748b" },
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
                if (name === "mean_speed" || name === "median_speed") {
                  return [`${value.toFixed(1)} km/h`, name === "mean_speed" ? "Mean Speed" : "Median Speed"];
                }
                if (name === "mean_distance") {
                  return [`${(value / 1000).toFixed(2)} km`, "Mean Distance"];
                }
                if (name === "avg_temp") {
                  return [`${value.toFixed(1)}°C`, "Avg Temp"];
                }
                if (name === "sample_size") {
                  return [value.toLocaleString(), "Sample Size"];
                }
                return [value, name];
              }}
            />
            <Legend
              wrapperStyle={{ fontSize: "11px" }}
              iconType="line"
              iconSize={14}
            />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="mean_speed"
              stroke="#3b82f6"
              strokeWidth={2}
              name="mean_speed"
              dot={{ fill: "#3b82f6", r: 4 }}
              activeDot={{ r: 6 }}
            />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="median_speed"
              stroke="#10b981"
              strokeWidth={2}
              name="median_speed"
              dot={{ fill: "#10b981", r: 4 }}
              activeDot={{ r: 6 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="mean_distance"
              stroke="#f59e0b"
              strokeWidth={2}
              name="mean_distance"
              dot={{ fill: "#f59e0b", r: 4 }}
              activeDot={{ r: 6}}
              strokeDasharray="5 5"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
