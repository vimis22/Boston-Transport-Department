import { Tab } from "../components/props/Navbar";

export const TABS: Tab[] = [
  { key: "home", label: "Home" },
  { key: "hive", label: "Hive Query" },
  { key: "kafka", label: "Kafka" },
  { key: "historical", label: "Historical Charts" },
];

export const TAB_STORAGE_KEY = "btd-active-tab";
