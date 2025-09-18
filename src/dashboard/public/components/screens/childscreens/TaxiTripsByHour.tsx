import React, { useMemo } from "react";
import { useHistoricalHiveQuery } from "../../../api/hooks";
import { Card } from "../../props/Card";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis, CartesianGrid } from "recharts";
import { HISTORICAL_DEFAULT_QUERY, HISTORICAL_FALLBACK_CHART_DATA } from "../../../references/MockData";

export function TaxiTripsByHour() {
  const { data, isLoading, isError } = useHistoricalHiveQuery(HISTORICAL_DEFAULT_QUERY);

  const chartData = useMemo(() => {
    if (!data?.data?.length) {
      return HISTORICAL_FALLBACK_CHART_DATA;
    }
    return data.data.map((row) => ({
      hour: (row as Record<string, unknown>).hour,
      trips: Number((row as Record<string, unknown>).trips ?? 0),
    }));
  }, [data]);

  return (
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
  );
}
