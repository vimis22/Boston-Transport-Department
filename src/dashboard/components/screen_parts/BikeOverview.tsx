import React from 'react'
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { bikeData } from '../fictional_data/fictonal_data'
import { styles, type Colors } from '../styling/GlobalStyles'

type Props = {
  colors: Colors
  cardStyle: React.CSSProperties
}

export default function BikeOverview({ colors, cardStyle }: Props) {
  return (
    <div style={cardStyle}>
      <h2 style={styles.heading}>Bike Rides per Hour</h2>
      <p style={styles.getSubtitle(colors)}>
        Source: bike-data.py (sample). Replace with real API/endpoint.
      </p>
      <div style={styles.chartContainer}>
        <ResponsiveContainer>
          <AreaChart data={bikeData} margin={styles.chartMargins}>
            <defs>
              <linearGradient id="colorRides" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={colors.accent} stopOpacity={0.5} />
                <stop offset="95%" stopColor={colors.accent} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" />
            <XAxis dataKey="hour" stroke={colors.subtext} />
            <YAxis stroke={colors.subtext} />
            <Tooltip />
            <Area type="monotone" dataKey="rides" stroke={colors.accent} fillOpacity={1} fill="url(#colorRides)" name="Rides" />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

