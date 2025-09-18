import React, { useMemo } from "react";
import { Card } from "../../props/Card";
import {
  ScatterChart,
  Scatter,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  CartesianGrid,
  ZAxis,
} from "recharts";
import { useHistoricalHiveQuery } from "../../../api/hooks";
import { useDateRangeFilter } from "../parentscreens/HistoricalTab";

const buildQuery = (dateFilter: string) => `
SELECT 
  ROUND(temp_c, 0) as temperature,
  ROUND(AVG(average_speed_kmh), 2) as avg_speed,
  COUNT(*) as trip_count,
  ROUND(AVG(precip_daily_mm), 2) as avg_precip
FROM bike_weather_distance
WHERE average_speed_kmh > 0 AND average_speed_kmh < 50${dateFilter}
GROUP BY ROUND(temp_c, 0)
ORDER BY temperature
`;

type WeatherSpeedData = {
  temperature: number;
  avg_speed: number;
  trip_count: number;
  avg_precip: number;
};

const FALLBACK_DATA: WeatherSpeedData[] = [
  { temperature: 15, avg_speed: 10.5, trip_count: 120, avg_precip: 0.2 },
  { temperature: 18, avg_speed: 11.2, trip_count: 250, avg_precip: 0.1 },
  { temperature: 20, avg_speed: 11.8, trip_count: 380, avg_precip: 0.3 },
  { temperature: 22, avg_speed: 12.1, trip_count: 420, avg_precip: 0.0 },
  { temperature: 25, avg_speed: 11.5, trip_count: 350, avg_precip: 0.0 },
];

export function BikeVsWeather() {
  const { dateFilter } = useDateRangeFilter();
  const query = useMemo(() => buildQuery(dateFilter), [dateFilter]);
  const { data, isLoading, isError } = useHistoricalHiveQuery(query);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      // Only use fallback if query hasn't completed yet (loading state)
      return isLoading ? FALLBACK_DATA : [];
    }

    return data.data.map((row) => ({
      temperature: Number(row.temperature ?? 0),
      avg_speed: Number(row.avg_speed ?? 0),
      trip_count: Number(row.trip_count ?? 0),
      avg_precip: Number(row.avg_precip ?? 0),
    }));
  }, [data, isLoading]);

  return (
    <Card title="Temperature vs Average Speed">
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
        Bubble size indicates trip count at each temperature
      </p>
      {!isLoading && chartData.length === 0 && (
        <div className="flex h-72 items-center justify-center text-sm text-slate-400">
          No data available for selected date range
        </div>
      )}
      <div style={{ width: "100%", height: chartData.length === 0 && !isLoading ? 0 : 288 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="temperature"
              name="Temperature"
              unit="°C"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
            />
            <YAxis
              dataKey="avg_speed"
              name="Avg Speed"
              unit=" km/h"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              domain={["auto", "auto"]}
            />
            <ZAxis
              dataKey="trip_count"
              range={[60, 400]}
              name="Trip Count"
            />
            <Tooltip
              cursor={{ strokeDasharray: "3 3" }}
              contentStyle={{
                backgroundColor: "#f8fafc",
                border: "1px solid #e2e8f0",
                borderRadius: "8px",
                fontSize: "12px",
              }}
              formatter={(value: number, name: string) => {
                if (name === "Avg Speed") return [`${value.toFixed(1)} km/h`, name];
                if (name === "Temperature") return [`${value}°C`, name];
                return [value.toLocaleString(), name];
              }}
            />
            <Scatter
              data={chartData}
              fill="#3b82f6"
              fillOpacity={0.7}
              stroke="#1e40af"
              strokeWidth={1}
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
