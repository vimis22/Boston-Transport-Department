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
  Legend,
} from "recharts";
import { useHistoricalHiveQuery } from "../../../api/hooks";

const QUERY = `
SELECT
  weather_condition,
  ROUND(mean_speed, 2) as mean_speed,
  ROUND(median_speed, 2) as median_speed,
  ROUND(stddev_speed, 2) as stddev_speed,
  ROUND(mean_distance, 2) as mean_distance,
  ROUND(mean_temp, 2) as mean_temp,
  sample_size
FROM weather_transport_statistics_by_condition
ORDER BY
  CASE weather_condition
    WHEN 'good' THEN 1
    WHEN 'fair' THEN 2
    WHEN 'poor' THEN 3
  END
`;

type WeatherConditionStats = {
  weather_condition: string;
  mean_speed: number;
  median_speed: number;
  stddev_speed: number;
  mean_distance: number;
  mean_temp: number;
  sample_size: number;
};

const FALLBACK_DATA: WeatherConditionStats[] = [
  {
    weather_condition: "good",
    mean_speed: 12.3,
    median_speed: 12.0,
    stddev_speed: 2.1,
    mean_distance: 1950,
    mean_temp: 20.5,
    sample_size: 5200,
  },
  {
    weather_condition: "fair",
    mean_speed: 11.5,
    median_speed: 11.2,
    stddev_speed: 2.4,
    mean_distance: 1820,
    mean_temp: 15.2,
    sample_size: 4850,
  },
  {
    weather_condition: "poor",
    mean_speed: 10.2,
    median_speed: 9.8,
    stddev_speed: 2.8,
    mean_distance: 1650,
    mean_temp: 8.5,
    sample_size: 2400,
  },
];

export function StatisticsByWeatherCondition() {
  const { data, isLoading, isError } = useHistoricalHiveQuery(QUERY);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return isLoading ? FALLBACK_DATA : [];
    }

    return data.data.map((row) => ({
      weather_condition: String(row.weather_condition ?? "unknown"),
      mean_speed: Number(row.mean_speed ?? 0),
      median_speed: Number(row.median_speed ?? 0),
      stddev_speed: Number(row.stddev_speed ?? 0),
      mean_distance: Number(row.mean_distance ?? 0),
      mean_temp: Number(row.mean_temp ?? 0),
      sample_size: Number(row.sample_size ?? 0),
    }));
  }, [data, isLoading]);

  return (
    <Card title="Statistics by Weather Condition">
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
        Comparing mean and median speeds across good, fair, and poor weather
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
          <BarChart
            data={chartData}
            margin={{ top: 10, right: 20, bottom: 20, left: 10 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="weather_condition"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
            />
            <YAxis
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
            <Tooltip
              contentStyle={{
                backgroundColor: "#f8fafc",
                border: "1px solid #e2e8f0",
                borderRadius: "8px",
                fontSize: "12px",
              }}
              formatter={(value: number, name: string) => {
                if (name === "mean_speed") return [`${value.toFixed(1)} km/h`, "Mean Speed"];
                if (name === "median_speed") return [`${value.toFixed(1)} km/h`, "Median Speed"];
                if (name === "stddev_speed") return [`${value.toFixed(1)} km/h`, "Std Dev"];
                if (name === "mean_distance") return [`${(value / 1000).toFixed(2)} km`, "Mean Distance"];
                if (name === "mean_temp") return [`${value.toFixed(1)}°C`, "Mean Temp"];
                if (name === "sample_size") return [value.toLocaleString(), "Sample Size"];
                return [value, name];
              }}
            />
            <Legend
              wrapperStyle={{ fontSize: "11px" }}
              iconType="rect"
              iconSize={10}
            />
            <Bar
              dataKey="mean_speed"
              fill="#3b82f6"
              name="Mean Speed"
              radius={[4, 4, 0, 0]}
            />
            <Bar
              dataKey="median_speed"
              fill="#10b981"
              name="Median Speed"
              radius={[4, 4, 0, 0]}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
