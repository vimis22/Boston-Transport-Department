import React from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { weatherData } from '../fictional_data/fictonal_data'
import { styles, type Colors } from '../styling/GlobalStyles'

type Props = {
  colors: Colors
  cardStyle: React.CSSProperties
}

export default function WeatherOverview({ colors, cardStyle }: Props) {
  return (
    <div style={cardStyle}>
      <h2 style={styles.heading}>Weather Overview</h2>
      <p style={styles.getSubtitle(colors)}>
        Source: weather-data.py (sample). Replace with real API/endpoint.
      </p>
      <div style={styles.chartContainer}>
        <ResponsiveContainer>
          <LineChart data={weatherData} margin={styles.chartMargins}>
            <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" />
            <XAxis dataKey="time" stroke={colors.subtext} />
            <YAxis yAxisId="left" stroke={colors.subtext} />
            <YAxis yAxisId="right" orientation="right" stroke={colors.subtext} />
            <Tooltip />
            <Legend />
            <Line yAxisId="left" type="monotone" dataKey="tempC" stroke={colors.primary} strokeWidth={2} dot={false} name="Temp (°C)" />
            <Line yAxisId="right" type="monotone" dataKey="windKph" stroke={colors.accent} strokeWidth={2} dot={false} name="Wind (kph)" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
