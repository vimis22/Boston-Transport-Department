import React from 'react'

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'ghost'
}

export function Button({ variant = 'primary', style, children, ...props }: ButtonProps) {
  const base: React.CSSProperties = {
    border: '1px solid transparent',
    padding: '8px 12px',
    borderRadius: 8,
    cursor: 'pointer',
    fontSize: 14,
    fontWeight: 600,
    transition: 'all 120ms ease-in-out',
  }

  const variants: Record<string, React.CSSProperties> = {
    primary: { background: '#111827', color: 'white' },
    secondary: { background: '#2563eb', color: 'white' },
    ghost: { background: 'transparent', color: '#111827', borderColor: '#d1d5db' },
  }

  const hover: React.CSSProperties =
    variant === 'primary' ? { filter: 'brightness(1.05)' } :
    variant === 'secondary' ? { filter: 'brightness(1.05)' } :
    { background: '#f3f4f6' }

  return (
    <button
      {...props}
      style={{ ...base, ...variants[variant], ...style }}
      onMouseEnter={(e) => Object.assign((e.currentTarget as HTMLButtonElement).style, hover)}
      onMouseLeave={(e) => Object.assign((e.currentTarget as HTMLButtonElement).style, { filter: '', background: variants[variant].background as string || 'transparent' })}
    >
      {children}
    </button>
  )
}
