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
} from "recharts";
import { useLiveKafkaAggregation } from "../../../api/hooks";

type WeatherRecord = {
  station_id: number;
  observation_date: string;
  station_name: string;
  dry_bulb_temperature_celsius: number;
  dew_point_temperature_celsius: number;
  precip_daily_mm: number;
  wind_observation: string;
  sky_condition_observation: string;
};

type WeatherAggregation = {
  count: number;
  avgTemp: number;
  avgPrecip: number;
  latestTemp: number | null;
  latestPrecip: number | null;
};

export function LiveWeatherChart() {
  const { data, totalCount, isLoading, reset, latestRecords } = useLiveKafkaAggregation<WeatherRecord, WeatherAggregation>({
    topic: "weather-data",
    bucketSizeMs: 5000,
    maxBuckets: 20,
    aggregate: (records) => {
      const temps = records.map(r => r.dry_bulb_temperature_celsius).filter(t => typeof t === 'number' && !isNaN(t));
      const precips = records.map(r => r.precip_daily_mm).filter(p => typeof p === 'number' && !isNaN(p));
      
      return {
        count: records.length,
        avgTemp: temps.length > 0 ? temps.reduce((a, b) => a + b, 0) / temps.length : 0,
        avgPrecip: precips.length > 0 ? precips.reduce((a, b) => a + b, 0) / precips.length : 0,
        latestTemp: temps.length > 0 ? temps[temps.length - 1] : null,
        latestPrecip: precips.length > 0 ? precips[precips.length - 1] : null,
      };
    },
  });

  const chartData = useMemo(() => {
    return data.map((d) => ({
      time: d.time,
      observations: d.count,
      temperature: d.avgTemp ? parseFloat(d.avgTemp.toFixed(1)) : null,
      precipitation: d.avgPrecip ? parseFloat(d.avgPrecip.toFixed(2)) : null,
    }));
  }, [data]);

  const latestWeather = useMemo(() => {
    if (!latestRecords.length) return null;
    const latest = latestRecords[latestRecords.length - 1]?.value as WeatherRecord | undefined;
    return latest;
  }, [latestRecords]);

  // Get current temperature for display
  const currentTemp = useMemo(() => {
    for (let i = data.length - 1; i >= 0; i--) {
      if (data[i].avgTemp !== null && data[i].avgTemp !== 0) {
        return data[i].avgTemp;
      }
    }
    return null;
  }, [data]);

  const currentPrecip = useMemo(() => {
    for (let i = data.length - 1; i >= 0; i--) {
      if (data[i].avgPrecip !== null) {
        return data[i].avgPrecip;
      }
    }
    return null;
  }, [data]);

  // Weather condition icon
  const getWeatherIcon = (skyCondition?: string): string => {
    if (!skyCondition) return "🌤️";
    const lower = skyCondition.toLowerCase();
    if (lower.includes("clear") || lower.includes("clr")) return "☀️";
    if (lower.includes("few") || lower.includes("sct")) return "⛅";
    if (lower.includes("bkn") || lower.includes("ovc")) return "☁️";
    if (lower.includes("rain") || lower.includes("ra")) return "🌧️";
    return "🌤️";
  };

  return (
    <Card
      title="Live Weather Observations"
      actions={
        <div className="flex items-center gap-3">
          <span className="flex items-center gap-1.5 text-xs">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-sky-500" />
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
        <div className="rounded-lg bg-sky-50 p-3">
          <p className="text-xs font-medium text-sky-700">Observations</p>
          <p className="text-2xl font-bold text-sky-600">{totalCount.toLocaleString()}</p>
        </div>
        <div className="rounded-lg bg-orange-50 p-3">
          <p className="text-xs font-medium text-orange-700">Temperature</p>
          <p className="text-2xl font-bold text-orange-600">
            {currentTemp !== null ? currentTemp.toFixed(1) : "—"}
            <span className="text-sm font-normal">°C</span>
          </p>
        </div>
        <div className="rounded-lg bg-blue-50 p-3">
          <p className="text-xs font-medium text-blue-700">Precipitation</p>
          <p className="text-2xl font-bold text-blue-600">
            {currentPrecip !== null ? currentPrecip.toFixed(1) : "—"}
            <span className="text-sm font-normal"> mm</span>
          </p>
        </div>
      </div>

      {/* Latest observation info */}
      {latestWeather && (
        <div className="mb-3 rounded-lg border border-slate-200 bg-gradient-to-r from-sky-50 to-blue-50 p-3">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs font-medium text-slate-500">Latest Observation</p>
              <p className="text-sm font-semibold text-slate-700">
                {latestWeather.station_name || `Station ${latestWeather.station_id}`}
              </p>
            </div>
            <div className="text-right">
              {/* <span className="text-3xl">{getWeatherIcon(latestWeather.sky_condition_observation)}</span> */}
              <p className="text-xs text-slate-500">{latestWeather.observation_date}</p>
            </div>
          </div>
        </div>
      )}

      {isLoading && data.length === 0 && (
        <div className="flex h-48 items-center justify-center text-sm text-slate-500">
          <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-slate-300 border-t-sky-600" />
          <span className="ml-2">Waiting for data...</span>
        </div>
      )}

      <div style={{ width: "100%", height: data.length === 0 ? 0 : 200 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData} margin={{ top: 10, right: 10, bottom: 0, left: -10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 10, fill: "#64748b" }}
              tickLine={false}
              axisLine={{ stroke: "#cbd5e1" }}
            />
            <YAxis
              yAxisId="temp"
              tick={{ fontSize: 10, fill: "#ea580c" }}
              tickLine={false}
              axisLine={false}
              width={35}
              tickFormatter={(v) => `${v}°`}
            />
            <YAxis
              yAxisId="precip"
              orientation="right"
              tick={{ fontSize: 10, fill: "#0284c7" }}
              tickLine={false}
              axisLine={false}
              width={35}
              tickFormatter={(v) => `${v}mm`}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "#f8fafc",
                border: "1px solid #e2e8f0",
                borderRadius: "8px",
                fontSize: "12px",
              }}
              formatter={(value: number | null, name: string) => {
                if (value === null) return ["—", name];
                if (name === "temperature") return [`${value}°C`, "Temperature"];
                if (name === "precipitation") return [`${value}mm`, "Precipitation"];
                if (name === "observations") return [value, "Observations"];
                return [value, name];
              }}
            />
            <Line
              yAxisId="temp"
              type="monotone"
              dataKey="temperature"
              stroke="#ea580c"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
              connectNulls
            />
            <Line
              yAxisId="precip"
              type="monotone"
              dataKey="precipitation"
              stroke="#0284c7"
              strokeWidth={2}
              dot={false}
              isAnimationActive={false}
              connectNulls
              strokeDasharray="5 5"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      
      {/* Legend */}
      <div className="mt-2 flex justify-center gap-4 text-xs">
        <span className="flex items-center gap-1">
          <span className="h-0.5 w-4 bg-orange-500" />
          Temperature
        </span>
        <span className="flex items-center gap-1">
          <span className="h-0.5 w-4 bg-sky-500" style={{ borderStyle: "dashed" }} />
          Precipitation
        </span>
      </div>
    </Card>
  );
}

