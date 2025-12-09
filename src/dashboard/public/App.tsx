import React, { useEffect, useState } from "react";
import { HomeTab } from "./sections/HomeTab";
import { HiveTab } from "./sections/HiveTab";
import { KafkaTab } from "./sections/KafkaTab";
import { HistoricalTab } from "./sections/HistoricalTab";

type TabKey = "home" | "hive" | "kafka" | "historical";
const TAB_STORAGE_KEY = "btd-active-tab";

const TABS: { key: TabKey; label: string }[] = [
  { key: "home", label: "Home" },
  { key: "hive", label: "Hive Query" },
  { key: "kafka", label: "Kafka" },
  { key: "historical", label: "Historical Charts" },
];

export default function App() {
  const [tab, setTab] = useState<TabKey>(() => {
    if (typeof window === "undefined") return "home";
    const saved = window.localStorage.getItem(TAB_STORAGE_KEY);
    return (saved as TabKey) ?? "home";
  });

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(TAB_STORAGE_KEY, tab);
  }, [tab]);

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-4">
          <div>
            <h1 className="text-lg font-bold text-slate-900">
              Boston Transport Dashboard
            </h1>
            <p className="text-sm text-slate-600">
              Timemanager, Hive, and Kafka controls
            </p>
          </div>
          <nav className="flex gap-2">
            {TABS.map((t) => (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={`rounded-md px-3 py-2 text-sm font-semibold ${
                  tab === t.key
                    ? "bg-slate-900 text-white"
                    : "text-slate-700 hover:bg-slate-100"
                }`}
              >
                {t.label}
              </button>
            ))}
          </nav>
        </div>
      </header>

      <main className="mx-auto max-w-6xl px-4 py-6">
        {tab === "home" && <HomeTab />}
        {tab === "hive" && <HiveTab />}
        {tab === "kafka" && <KafkaTab />}
        {tab === "historical" && <HistoricalTab />}
      </main>
    </div>
  );
}

