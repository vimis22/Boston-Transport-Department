import React from 'react';

export type Theme = 'light' | 'dark'

export type Colors = {
    bg: string;
    card: string;
    text: string;
    subtext: string;
    grid: string;
    primary: string;
    accent: string;
}

export const lightColors: Colors = {
    bg: '#f7fafc',
    card: '#ffffff',
    text: '#1a202c',
    subtext: '#4a5568',
    grid: '#e2e8f0',
    primary: '#2563eb',
    accent: '#16a34a',
}

export const darkColors: Colors = {
    bg: '#0f172a',
    card: '#111827',
    text: '#e5e7eb',
    subtext: '#9ca3af',
    grid: '#374151',
    primary: '#60a5fa',
    accent: '#34d399',
}

export function getColors(theme: Theme): Colors {
    return theme === 'light' ? lightColors : darkColors
}

export const styles = {
    getCardStyle: (theme: Theme): React.CSSProperties => {
        const colors = getColors(theme)
        return {
            background: colors.card,
            borderRadius: 12,
            padding: 16,
            boxShadow: theme === 'light' ? '0 1px 3px rgba(0,0,0,0.1)' : '0 1px 3px rgba(0,0,0,0.4)',
            border: theme === 'light' ? '1px solid #e5e7eb' : '1px solid #1f2937',
        }
    },

    sectionContainer: {
        display: 'grid',
        gridTemplateColumns: '1fr',
        gap: 16,
    } as React.CSSProperties,

    heading: {
        margin: '0 0 8px 0',
    } as React.CSSProperties,

    getSubtitle: (colors: Colors): React.CSSProperties => ({
        margin: '0 0 16px 0',
        color: colors.subtext,
    }),

    chartContainer: {
        width: '100%',
        height: 280,
    } as React.CSSProperties,

    chartMargins: {
        top: 10,
        right: 20,
        left: 0,
        bottom: 0,
    },
}
