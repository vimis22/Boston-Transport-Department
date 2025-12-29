import React, { useMemo } from "react";
import { Card } from "../../props/Card";
import {
  AreaChart,
  Area,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  CartesianGrid,
} from "recharts";
import { useHistoricalHiveQuery } from "../../../api/hooks";
import { useDateRangeFilter } from "../parentscreens/HistoricalTab";

const buildQuery = (dateFilter: string) => `
SELECT 
  FLOOR(average_speed_kmh / 2) * 2 as speed_bucket,
  COUNT(*) as trip_count
FROM bike_weather_distance
WHERE average_speed_kmh > 0 AND average_speed_kmh < 30${dateFilter}
GROUP BY FLOOR(average_speed_kmh / 2) * 2
ORDER BY speed_bucket
`;

type SpeedData = {
  speed_bucket: number;
  trip_count: number;
};

const FALLBACK_DATA: SpeedData[] = [
  { speed_bucket: 4, trip_count: 45 },
  { speed_bucket: 6, trip_count: 120 },
  { speed_bucket: 8, trip_count: 280 },
  { speed_bucket: 10, trip_count: 450 },
  { speed_bucket: 12, trip_count: 520 },
  { speed_bucket: 14, trip_count: 380 },
  { speed_bucket: 16, trip_count: 220 },
  { speed_bucket: 18, trip_count: 90 },
  { speed_bucket: 20, trip_count: 35 },
];

export function TripSpeedDistribution() {
  const { dateFilter } = useDateRangeFilter();
  const query = useMemo(() => buildQuery(dateFilter), [dateFilter]);
  const { data, isLoading, isError } = useHistoricalHiveQuery(query);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return isLoading ? FALLBACK_DATA : [];
    }

    return data.data.map((row) => ({
      speed_bucket: Number(row.speed_bucket ?? 0),
      trip_count: Number(row.trip_count ?? 0),
    }));
  }, [data, isLoading]);

  return (
    <Card title="Trip Speed Distribution">
      {isLoading && (
        <div className="flex items-center gap-2 text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-emerald-600" />
          Querying Hive...
        </div>
      )}
      {isError && (
        <p className="text-sm text-amber-600">
          ⚠ Could not load data. Showing sample values.
        </p>
      )}
      <p className="mb-3 text-xs text-slate-500">
        Distribution of average cycling speeds (km/h)
      </p>
      {!isLoading && chartData.length === 0 && (
        <div className="flex h-72 items-center justify-center text-sm text-slate-400">
          No data available for selected date range
        </div>
      )}
      <div style={{ width: "100%", height: chartData.length === 0 && !isLoading ? 0 : 288 }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 10, right: 20, bottom: 20, left: 10 }}>
            <defs>
              <linearGradient id="speedGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#10b981" stopOpacity={0.8} />
                <stop offset="95%" stopColor="#10b981" stopOpacity={0.1} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="speed_bucket"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickLine={{ stroke: "#cbd5e1" }}
              axisLine={{ stroke: "#cbd5e1" }}
              label={{
                value: "Speed (km/h)",
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
              label={{
                value: "Trips",
                angle: -90,
                position: "insideLeft",
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
              formatter={(value: number) => [value.toLocaleString(), "Trip Count"]}
              labelFormatter={(label) => `${label}-${Number(label) + 2} km/h`}
            />
            <Area
              type="monotone"
              dataKey="trip_count"
              stroke="#059669"
              strokeWidth={2}
              fill="url(#speedGradient)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

