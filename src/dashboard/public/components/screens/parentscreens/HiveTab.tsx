import React from "react";
import { HiveSQLEditor } from "../childscreens/HiveSQLEditor";
import { HiveResults } from "../childscreens/HiveResults";

export function HiveTab() {
  return (
    <div className="space-y-4">
      <HiveSQLEditor />
      <HiveResults />
    </div>
  );
}
