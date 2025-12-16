import React, { useMemo, useState, createContext, useContext } from "react";
import { BikeVsWeather } from "../childscreens/BikeVsWeather";
import { TripSpeedDistribution } from "../childscreens/TripSpeedDistribution";
import { PrecipitationImpact } from "../childscreens/PrecipitationImpact";
import { TripsByTimeOfDay } from "../childscreens/TripsByTimeOfDay";
import { TripDistanceDistribution } from "../childscreens/TripDistanceHeatmap";
import { useHistoricalHiveQuery } from "../../../api/hooks";

// Date range context for child components
type DateRangeContextType = {
  dateFilter: string;
  isFiltered: boolean;
};

export const DateRangeContext = createContext<DateRangeContextType>({
  dateFilter: "",
  isFiltered: false,
});

export function useDateRangeFilter() {
  return useContext(DateRangeContext);
}

const TIME_RANGE_QUERY = `
SELECT 
  MIN(starttime) as min_time,
  MAX(stoptime) as max_time,
  COUNT(*) as total_trips
FROM bike_weather_distance
WHERE average_speed_kmh > 0
`;

export function HistoricalTab() {
  const { data, isLoading } = useHistoricalHiveQuery(TIME_RANGE_QUERY);
  
  // Date range filter state
  const [startDate, setStartDate] = useState<string>("");
  const [endDate, setEndDate] = useState<string>("");
  const [isEditing, setIsEditing] = useState(false);
  const [isFilterActive, setIsFilterActive] = useState(false);

  const timeRange = useMemo(() => {
    if (!data?.data?.length) return null;
    const row = data.data[0];
    return {
      minTime: row.min_time ? String(row.min_time).replace(" ", "T") : null,
      maxTime: row.max_time ? String(row.max_time).replace(" ", "T") : null,
      totalTrips: Number(row.total_trips ?? 0),
    };
  }, [data]);

  // Build date filter SQL clause
  const dateFilterContext = useMemo<DateRangeContextType>(() => {
    if (!isFilterActive || (!startDate && !endDate)) {
      return { dateFilter: "", isFiltered: false };
    }
    
    const conditions: string[] = [];
    if (startDate) {
      conditions.push(`starttime >= '${startDate.replace("T", " ")}'`);
    }
    if (endDate) {
      conditions.push(`stoptime <= '${endDate.replace("T", " ")}'`);
    }
    
    return {
      dateFilter: conditions.length > 0 ? ` AND ${conditions.join(" AND ")}` : "",
      isFiltered: true,
    };
  }, [startDate, endDate, isFilterActive]);

  const handleApply = () => {
    if (startDate || endDate) {
      setIsFilterActive(true);
    }
    setIsEditing(false);
  };

  const handleReset = () => {
    setStartDate("");
    setEndDate("");
    setIsFilterActive(false);
    setIsEditing(false);
  };

  const displayStart = isFilterActive && startDate ? startDate : timeRange?.minTime ?? "";
  const displayEnd = isFilterActive && endDate ? endDate : timeRange?.maxTime ?? "";

  const handleStartEditing = () => {
    // Pre-fill with current display values when entering edit mode
    if (!startDate) setStartDate(timeRange?.minTime ?? "");
    if (!endDate) setEndDate(timeRange?.maxTime ?? "");
    setIsEditing(true);
  };

  return (
    <DateRangeContext.Provider value={dateFilterContext}>
      <div className="space-y-6">
        {/* Section header */}
        <div className="border-b border-slate-200 pb-4">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-lg font-semibold text-slate-800">Historical Analytics</h2>
              <p className="mt-1 text-sm text-slate-500">
                Bike trip patterns and weather correlations from Hive data warehouse
              </p>
            </div>
            
            {/* Time range / filter controls */}
            <div className="flex flex-wrap items-center gap-2">
              {isLoading ? (
                <div className="flex items-center gap-2 rounded-lg bg-slate-100 px-3 py-2 text-xs text-slate-500">
                  <span className="inline-block h-3 w-3 animate-spin rounded-full border-2 border-slate-300 border-t-blue-500" />
                  Loading...
                </div>
              ) : isEditing ? (
                <div className="flex items-center gap-2 rounded-lg bg-blue-50 px-3 py-1.5">
                  <span className="text-xs font-medium text-blue-700">Period:</span>
                  <input
                    type="text"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    className="w-40 rounded border border-blue-200 bg-white px-2 py-1 text-xs text-blue-600 focus:border-blue-400 focus:outline-none"
                  />
                  <span className="text-xs text-blue-600">—</span>
                  <input
                    type="text"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className="w-40 rounded border border-blue-200 bg-white px-2 py-1 text-xs text-blue-600 focus:border-blue-400 focus:outline-none"
                  />
                  <button
                    onClick={handleApply}
                    style={{ backgroundColor: "#2563eb", color: "white" }}
                    className="rounded px-3 py-1 text-xs font-semibold shadow-sm hover:opacity-90"
                  >
                    Apply
                  </button>
                  <button
                    onClick={handleReset}
                    className="rounded bg-slate-200 px-2 py-1 text-xs font-medium text-slate-600 hover:bg-slate-300"
                  >
                    Reset
                  </button>
                </div>
              ) : (
                <button
                  onClick={handleStartEditing}
                  className="flex items-center gap-1 rounded-lg bg-blue-50 px-3 py-2 text-xs transition hover:bg-blue-100"
                >
                  <span className="font-medium text-blue-700">
                    {isFilterActive ? "Filtered:" : "Period:"}
                  </span>
                  <span className="text-blue-600">{displayStart}</span>
                  <span className="text-blue-400">—</span>
                  <span className="text-blue-600">{displayEnd}</span>
                  {isFilterActive && (
                    <span className="ml-1 rounded-full bg-amber-100 px-1.5 py-0.5 text-[10px] font-medium text-amber-700">
                      Filtered
                    </span>
                  )}
                </button>
              )}
              
              {timeRange && !isEditing && (
                <div className="rounded-lg bg-emerald-50 px-3 py-2 text-xs">
                  <span className="font-medium text-emerald-700">Trips:</span>{" "}
                  <span className="text-emerald-600">{timeRange.totalTrips.toLocaleString()}</span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Time-based charts */}
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <TripsByTimeOfDay />
          <PrecipitationImpact />
        </div>

        {/* Weather impact charts */}
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <BikeVsWeather />
          <TripDistanceDistribution />
        </div>

        {/* Distribution charts */}
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <TripSpeedDistribution />
        </div>
      </div>
    </DateRangeContext.Provider>
  );
}
