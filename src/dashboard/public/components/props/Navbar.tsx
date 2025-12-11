import React from 'react';

export type TabKey = "home" | "hive" | "kafka" | "historical";

export interface Tab {
  key: TabKey;
  label: string;
}

interface NavbarProps {
  title: string;
  subtitle: string;
  tabs: Tab[];
  activeTab: TabKey;
  onTabChange: (tab: TabKey) => void;
}

export function Navbar({ title, subtitle, tabs, activeTab, onTabChange }: NavbarProps) {
  return (
    <header className="border-b border-slate-200 bg-white">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-4">
        <div>
          <h1 className="text-lg font-bold text-slate-900">{title}</h1>
          <p className="text-sm text-slate-600">{subtitle}</p>
        </div>
        <nav className="flex gap-2">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              onClick={() => onTabChange(tab.key)}
              className={`rounded-md px-3 py-2 text-sm font-semibold ${
                activeTab === tab.key
                  ? "bg-slate-900 text-white"
                  : "text-slate-700 hover:bg-slate-100"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>
    </header>
  );
}
