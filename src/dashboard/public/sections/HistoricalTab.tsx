import React, { useMemo } from "react";
import { useHistoricalHiveQuery } from "../api/hooks";
import { Card } from "../components/Card";
import {
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  CartesianGrid,
  BarChart,
  Bar,
} from "recharts";

const DEFAULT_QUERY =
  "SELECT hour, COUNT(*) as trips FROM taxi_data_raw GROUP BY hour ORDER BY hour LIMIT 24";

export function HistoricalTab() {
  const { data, isLoading, isError } = useHistoricalHiveQuery(DEFAULT_QUERY);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return [
        { hour: 0, trips: 0 },
        { hour: 6, trips: 12 },
        { hour: 12, trips: 28 },
        { hour: 18, trips: 35 },
        { hour: 23, trips: 10 },
      ];
    }
    return data.data.map((row) => ({
      hour: (row as Record<string, unknown>).hour,
      trips: Number((row as Record<string, unknown>).trips ?? 0),
    }));
  }, [data]);

  return (
    <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
      <Card title="Taxi Trips by Hour (Hive)">
        {isLoading && <p className="text-sm text-slate-600">Loading…</p>}
        {isError && (
          <p className="text-sm text-red-600">
            Failed to query Hive. Showing fallback data.
          </p>
        )}
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="hour" />
              <YAxis />
              <Tooltip />
              <Line
                type="monotone"
                dataKey="trips"
                stroke="#1e40af"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Card>

      <Card title="Bike vs Weather (placeholder)">
        <p className="mb-3 text-sm text-slate-600">
          Placeholder chart wired for future ETL outputs; swap the data source
          to new Kafka topics when ready.
        </p>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={[
                { label: "Clear", bikes: 30 },
                { label: "Cloudy", bikes: 22 },
                { label: "Rain", bikes: 10 },
              ]}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="label" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="bikes" fill="#0f172a" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Card>
    </div>
  );
}

