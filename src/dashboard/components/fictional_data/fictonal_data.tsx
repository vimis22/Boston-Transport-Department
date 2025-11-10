// Centralized fictional/mock datasets for the dashboard charts.
// Replace these with real fetch(...) calls or data loaders as needed.

export const weatherData = [
  { time: '08:00', tempC: 10, windKph: 5 },
  { time: '10:00', tempC: 14, windKph: 9 },
  { time: '12:00', tempC: 18, windKph: 11 },
  { time: '14:00', tempC: 19, windKph: 14 },
  { time: '16:00', tempC: 17, windKph: 12 },
  { time: '18:00', tempC: 14, windKph: 7 },
]

export const taxiData = [
  { hour: '08', trips: 120 },
  { hour: '09', trips: 150 },
  { hour: '10', trips: 180 },
  { hour: '11', trips: 160 },
  { hour: '12', trips: 210 },
  { hour: '13', trips: 230 },
]

export const bikeData = [
  { hour: '08', rides: 40 },
  { hour: '09', rides: 52 },
  { hour: '10', rides: 68 },
  { hour: '11', rides: 61 },
  { hour: '12', rides: 75 },
  { hour: '13', rides: 79 },
]
