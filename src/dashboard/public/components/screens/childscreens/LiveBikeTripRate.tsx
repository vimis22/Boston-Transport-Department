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
import { useLiveKafkaAggregation } from "../../../api/hooks";

type BikeRecord = {
  tripduration: number;
  starttime: string;
  stoptime: string;
  start_station_name: string;
  end_station_name: string;
};

type BikeAggregation = {
  count: number;
  avgDuration: number;
  stations: Set<string>;
};

export function LiveBikeTripRate() {
  const { data, totalCount, isLoading, reset, latestRecords } = useLiveKafkaAggregation<BikeRecord, BikeAggregation>({
    topic: "bike-data",
    bucketSizeMs: 3000,
    maxBuckets: 20,
    aggregate: (records) => {
      const avgDuration = records.length > 0
        ? records.reduce((sum, r) => sum + (r.tripduration || 0), 0) / records.length / 60
        : 0;
      const stations = new Set<string>();
      records.forEach(r => {
        if (r.start_station_name) stations.add(r.start_station_name);
        if (r.end_station_name) stations.add(r.end_station_name);
      });
      return { count: records.length, avgDuration, stations };
    },
  });

  const chartData = useMemo(() => {
    return data.map((d) => ({
      time: d.time,
      trips: d.count,
      avgDuration: d.avgDuration?.toFixed(1) ?? 0,
    }));
  }, [data]);

  const recentTrip = useMemo(() => {
    if (!latestRecords.length) return null;
    const latest = latestRecords[latestRecords.length - 1]?.value as BikeRecord | undefined;
    return latest;
  }, [latestRecords]);

  return (
    <Card
      title="Live Bike Trips"
      actions={
        <div className="flex items-center gap-3">
          <span className="flex items-center gap-1.5 text-xs">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-emerald-500" />
            <span className="text-slate-500">Streaming</span>
          </span>
          <button
            onClick={reset}
            className="rounded bg-slate-100 px-2 py-1 text-xs font-medium text-slate-600 hover:bg-slate-200"
          >
            Reset
          </button>
        </div>
      }
    >
      {/* Stats row */}
      <div className="mb-4 grid grid-cols-3 gap-3">
        <div className="rounded-lg bg-emerald-50 p-3">
          <p className="text-xs font-medium text-emerald-700">Total Trips</p>
          <p className="text-2xl font-bold text-emerald-600">{totalCount.toLocaleString()}</p>
        </div>
        <div className="rounded-lg bg-blue-50 p-3">
          <p className="text-xs font-medium text-blue-700">Current Rate</p>
          <p className="text-2xl font-bold text-blue-600">
            {chartData.length > 0 ? chartData[chartData.length - 1].trips : 0}
            <span className="text-sm font-normal">/3s</span>
          </p>
        </div>
        <div className="rounded-lg bg-amber-50 p-3">
          <p className="text-xs font-medium text-amber-700">Avg Duration</p>
          <p className="text-2xl font-bold text-amber-600">
            {chartData.length > 0 ? chartData[chartData.length - 1].avgDuration : 0}
            <span className="text-sm font-normal"> min</span>
          </p>
        </div>
      </div>

      {/* Recent trip info */}
      {recentTrip && (
        <div className="mb-3 rounded-lg border border-slate-200 bg-slate-50 p-2 text-xs">
          <span className="font-medium text-slate-600">Latest:</span>{" "}
          <span className="text-slate-700">
            {recentTrip.start_station_name ?? "Unknown"} → {recentTrip.end_station_name ?? "Unknown"}
          </span>
        </div>
      )}

      {isLoading && data.length === 0 && (
        <div className="flex h-48 items-center justify-center text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-emerald-600" />
          <span className="ml-2">Waiting for data...</span>
        </div>
      )}

      <div style={{ width: "100%", height: data.length === 0 ? 0 : 200 }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 10, right: 10, bottom: 0, left: -10 }}>
            <defs>
              <linearGradient id="bikeGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#10b981" stopOpacity={0.4} />
                <stop offset="95%" stopColor="#10b981" stopOpacity={0.05} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 10, fill: "#64748b" }}
              tickLine={false}
              axisLine={{ stroke: "#cbd5e1" }}
            />
            <YAxis
              tick={{ fontSize: 10, fill: "#64748b" }}
              tickLine={false}
              axisLine={false}
              width={30}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#f8fafc",
                border: "1px solid #e2e8f0",
                borderRadius: "8px",
                fontSize: "12px",
              }}
              formatter={(value: number, name: string) => {
                if (name === "trips") return [value, "Trips"];
                return [value, name];
              }}
            />
            <Area
              type="monotone"
              dataKey="trips"
              stroke="#10b981"
              strokeWidth={2}
              fill="url(#bikeGradient)"
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

