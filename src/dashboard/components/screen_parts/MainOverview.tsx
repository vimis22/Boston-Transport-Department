import React from 'react'
import WeatherOverview from './WeatherOverview'
import TaxiOverview from '../screen_parts/TaxiOverview'
import BikeOverview from '../screen_parts/BikeOverview'
import { styles, type Colors } from '../styling/GlobalStyles'

type Props = {
  colors: Colors
  theme: 'light' | 'dark'
}

export default function MainOverview({ colors, theme }: Props) {
    const cardStyle = styles.getCardStyle(theme)

  return (
    <section style={styles.sectionContainer}>
      <div>
        <WeatherOverview colors={colors} cardStyle={cardStyle} />
      </div>
      <div>
        <TaxiOverview colors={colors} cardStyle={cardStyle} />
      </div>
      <div>
        <BikeOverview colors={colors} cardStyle={cardStyle} />
      </div>
    </section>
  )
}
