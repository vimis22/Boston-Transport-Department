import React from 'react'

interface NavbarProps {
  title: string
  subtitle?: string
  rightContent?: React.ReactNode
}

export default function Navbar({ title, subtitle, rightContent }: NavbarProps) {
  return (
    <header
      style={{
        position: 'sticky',
        top: 0,
        zIndex: 10,
        background: 'linear-gradient(90deg, #0ea5e9 0%, #2563eb 100%)',
        color: 'white',
        padding: '16px 24px',
        boxShadow: '0 2px 6px rgba(0,0,0,0.15)'
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', maxWidth: 1200, margin: '0 auto', gap: 12 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 20, lineHeight: 1.2 }}>{title}</h1>
          {subtitle && (
            <p style={{ margin: 0, opacity: 0.9, fontSize: 13 }}>{subtitle}</p>
          )}
        </div>
        {rightContent}
      </div>
    </header>
  )
}
