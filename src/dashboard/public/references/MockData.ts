// Mock data and default queries for the dashboard
// Ensures separation of concerns by keeping data separate from UI components

// ============================================================================
// HISTORICAL TAB DATA
// ============================================================================

// export const HISTORICAL_DEFAULT_QUERY =
//   "SELECT hour, COUNT(*) as trips FROM taxi_data_raw GROUP BY hour ORDER BY hour LIMIT 24";

export const HISTORICAL_FALLBACK_CHART_DATA = [
  { hour: 0, trips: 0 },
  { hour: 6, trips: 12 },
  { hour: 12, trips: 28 },
  { hour: 18, trips: 35 },
  { hour: 23, trips: 10 },
];

export const BIKE_WEATHER_PLACEHOLDER_DATA = [
  { label: "Clear", bikes: 30 },
  { label: "Cloudy", bikes: 22 },
  { label: "Rain", bikes: 10 },
];

// ============================================================================
// HIVE TAB DATA
// ============================================================================

export const HIVE_DEFAULT_QUERY = "SELECT * FROM bike_data LIMIT 10";

export const HIVE_QUICK_QUERIES: { label: string; statement: string }[] = [
  { label: "Show databases", statement: "SHOW DATABASES;" },
  { label: "Show tables", statement: "SHOW TABLES;" },
  { label: "Describe bike_data", statement: "DESCRIBE FORMATTED bike_data;" },
  { label: "Describe weather_data", statement: "DESCRIBE FORMATTED weather_data;" },
  { label: "Preview bike data", statement: "SELECT * FROM bike_data LIMIT 20;" },
  { label: "Preview weather data", statement: "SELECT * FROM weather_data LIMIT 20;" },
];
