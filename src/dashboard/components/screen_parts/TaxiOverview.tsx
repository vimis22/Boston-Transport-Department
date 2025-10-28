import React from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { taxiData } from '../fictional_data/fictonal_data'
import { styles, type Colors } from '../styling/GlobalStyles'

type Props = {
  colors: Colors
  cardStyle: React.CSSProperties
}

export default function TaxiOverview({ colors, cardStyle }: Props) {
  return (
    <div style={cardStyle}>
      <h2 style={styles.heading}>Taxi Trips per Hour</h2>
      <p style={styles.getSubtitle(colors)}>
        Source: taxi-data.py (sample). Replace with real API/endpoint.
      </p>
      <div style={styles.chartContainer}>
        <ResponsiveContainer>
          <BarChart data={taxiData} margin={styles.chartMargins}>
            <CartesianGrid stroke={colors.grid} strokeDasharray="3 3" />
            <XAxis dataKey="hour" stroke={colors.subtext} />
            <YAxis stroke={colors.subtext} />
            <Tooltip />
            <Legend />
            <Bar dataKey="trips" fill={colors.primary} name="Trips" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

