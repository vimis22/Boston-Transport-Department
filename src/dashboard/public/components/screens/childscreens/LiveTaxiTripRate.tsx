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
  BarChart,
  Bar,
  Cell,
} from "recharts";
import { useLiveKafkaAggregation } from "../../../api/hooks";

type TaxiRecord = {
  id: string;
  timestamp: string;
  hour: number;
  source: string;
  destination: string;
  cab_type: string;
};

type TaxiAggregation = {
  count: number;
  cabTypes: Record<string, number>;
  routes: Map<string, number>;
};

const CAB_COLORS: Record<string, string> = {
  Uber: "#000000",
  Lyft: "#ff00bf",
  Yellow: "#f59e0b",
  default: "#6366f1",
};

export function LiveTaxiTripRate() {
  const { data, totalCount, isLoading, reset, latestRecords } = useLiveKafkaAggregation<TaxiRecord, TaxiAggregation>({
    topic: "taxi-data",
    bucketSizeMs: 3000,
    maxBuckets: 20,
    aggregate: (records) => {
      const cabTypes: Record<string, number> = {};
      const routes = new Map<string, number>();
      records.forEach((r) => {
        const type = r.cab_type || "Unknown";
        cabTypes[type] = (cabTypes[type] || 0) + 1;
        
        const route = `${r.source ?? "?"} → ${r.destination ?? "?"}`;
        routes.set(route, (routes.get(route) || 0) + 1);
      });
      return { count: records.length, cabTypes, routes };
    },
  });

  const chartData = useMemo(() => {
    return data.map((d) => ({
      time: d.time,
      trips: d.count,
    }));
  }, [data]);

  // Aggregate cab types across all buckets for pie-like breakdown
  const cabTypeBreakdown = useMemo(() => {
    const totals: Record<string, number> = {};
    data.forEach((d) => {
      if (d.cabTypes) {
        Object.entries(d.cabTypes).forEach(([type, count]) => {
          totals[type] = (totals[type] || 0) + count;
        });
      }
    });
    return Object.entries(totals)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);
  }, [data]);

  const recentTrip = useMemo(() => {
    if (!latestRecords.length) return null;
    const latest = latestRecords[latestRecords.length - 1]?.value as TaxiRecord | undefined;
    return latest;
  }, [latestRecords]);

  return (
    <Card
      title="Live Taxi Trips"
      actions={
        <div className="flex items-center gap-3">
          <span className="flex items-center gap-1.5 text-xs">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-violet-500" />
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
        <div className="rounded-lg bg-violet-50 p-3">
          <p className="text-xs font-medium text-violet-700">Total Trips</p>
          <p className="text-2xl font-bold text-violet-600">{totalCount.toLocaleString()}</p>
        </div>
        <div className="rounded-lg bg-slate-100 p-3">
          <p className="text-xs font-medium text-slate-700">Current Rate</p>
          <p className="text-2xl font-bold text-slate-600">
            {chartData.length > 0 ? chartData[chartData.length - 1].trips : 0}
            <span className="text-sm font-normal">/3s</span>
          </p>
        </div>
        <div className="rounded-lg bg-pink-50 p-3">
          <p className="text-xs font-medium text-pink-700">Cab Types</p>
          <p className="text-2xl font-bold text-pink-600">{cabTypeBreakdown.length}</p>
        </div>
      </div>

      {/* Cab type breakdown */}
      {cabTypeBreakdown.length > 0 && (
        <div className="mb-3 flex flex-wrap gap-2">
          {cabTypeBreakdown.map(({ name, value }) => (
            <span
              key={name}
              className="inline-flex items-center gap-1 rounded-full px-2 py-1 text-xs font-medium"
              style={{
                backgroundColor: `${CAB_COLORS[name] || CAB_COLORS.default}15`,
                color: CAB_COLORS[name] || CAB_COLORS.default,
              }}
            >
              <span
                className="h-1.5 w-1.5 rounded-full"
                style={{ backgroundColor: CAB_COLORS[name] || CAB_COLORS.default }}
              />
              {name}: {value}
            </span>
          ))}
        </div>
      )}

      {/* Recent trip info */}
      {recentTrip && (
        <div className="mb-3 rounded-lg border border-slate-200 bg-slate-50 p-2 text-xs">
          <span className="font-medium text-slate-600">Latest:</span>{" "}
          <span className="text-slate-700">
            {recentTrip.source ?? "Unknown"} → {recentTrip.destination ?? "Unknown"}
          </span>
          <span
            className="ml-2 rounded px-1.5 py-0.5 text-[10px] font-semibold"
            style={{
              backgroundColor: `${CAB_COLORS[recentTrip.cab_type] || CAB_COLORS.default}20`,
              color: CAB_COLORS[recentTrip.cab_type] || CAB_COLORS.default,
            }}
          >
            {recentTrip.cab_type || "Unknown"}
          </span>
        </div>
      )}

      {isLoading && data.length === 0 && (
        <div className="flex h-48 items-center justify-center text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-violet-600" />
          <span className="ml-2">Waiting for data...</span>
        </div>
      )}

      <div style={{ width: "100%", height: data.length === 0 ? 0 : 200 }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 10, right: 10, bottom: 0, left: -10 }}>
            <defs>
              <linearGradient id="taxiGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.4} />
                <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0.05} />
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
              stroke="#8b5cf6"
              strokeWidth={2}
              fill="url(#taxiGradient)"
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}

