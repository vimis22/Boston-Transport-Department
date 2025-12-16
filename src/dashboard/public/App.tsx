import React, { useEffect, useState } from "react";
import { Navbar, TabKey } from "./components/props/Navbar";
import { HomeTab } from "./components/screens/parentscreens/HomeTab";
import { HiveTab } from "./components/screens/parentscreens/HiveTab";
import { KafkaTab } from "./components/screens/parentscreens/KafkaTab";
import { HistoricalTab } from "./components/screens/parentscreens/HistoricalTab";
import { LiveChartsTab } from "./components/screens/parentscreens/LiveChartsTab";
import { TABS, TAB_STORAGE_KEY } from "./references/TabConfig";

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
      <Navbar
        title="Boston Transport Dashboard"
        subtitle="Timemanager, Hive, and Kafka controls"
        tabs={TABS}
        activeTab={tab}
        onTabChange={setTab}
      />

      <main className="mx-auto max-w-6xl px-4 py-6">
        {tab === "home" && <HomeTab />}
        {tab === "hive" && <HiveTab />}
        {tab === "kafka" && <KafkaTab />}
        {tab === "live" && <LiveChartsTab />}
        {tab === "historical" && <HistoricalTab />}
      </main>
    </div>
  );
}
