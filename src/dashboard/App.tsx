import React, { useMemo, useState } from 'react'
import Navbar from './components/props/Navbar'
import { Button } from './components/props/Buttons'
import MainOverview from './components/screen_parts/MainOverview'

export default function App() {
  const [theme, setTheme] = useState<'light' | 'dark'>('light')

  const colors = useMemo(() => (
    theme === 'light'
      ? {
          bg: '#f7fafc',
          card: '#ffffff',
          text: '#1a202c',
          subtext: '#4a5568',
          grid: '#e2e8f0',
          primary: '#2563eb',
          accent: '#16a34a',
        }
      : {
          bg: '#0f172a',
          card: '#111827',
          text: '#e5e7eb',
          subtext: '#9ca3af',
          grid: '#374151',
          primary: '#60a5fa',
          accent: '#34d399',
        }
  ), [theme])

  const containerStyle: React.CSSProperties = {
    minHeight: '100vh',
    background: colors.bg,
    color: colors.text,
    fontFamily: 'Inter, system-ui, Avenir, Helvetica, Arial, sans-serif',
  }


  return (
    <div style={containerStyle}>
      <Navbar title="Boston Transport Dashboard" subtitle="Visualizing live and historical transit and weather data" rightContent={
          <Button onClick={() => setTheme(t => (t === 'light' ? 'dark' : 'light'))}>
            Toggle {theme === 'light' ? 'Dark' : 'Light'} Mode
          </Button>
        }
      />

      <main style={{ padding: 24, maxWidth: 1200, margin: '0 auto' }}>
        <MainOverview colors={colors} theme={theme} />
      </main>

      <footer style={{ padding: 24, textAlign: 'center', color: colors.subtext }}>
        <small>
          To connect real data, replace mock arrays with fetch calls to your Python service
          (Flask/FastAPI) or to files produced by your Spark/ETL jobs.
        </small>
      </footer>
    </div>
  )
}
