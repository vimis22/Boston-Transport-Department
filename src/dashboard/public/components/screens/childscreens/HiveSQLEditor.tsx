import React, { useState } from "react";
import { useHiveQuery } from "../../../api/hooks";
import { Card } from "../../props/Card";
import { HIVE_DEFAULT_QUERY, HIVE_QUICK_QUERIES } from "../../../references/MockData";

interface HiveSQLEditorProps {
  onQueryExecuted?: () => void;
}

export function HiveSQLEditor({ onQueryExecuted }: HiveSQLEditorProps) {
  const [statement, setStatement] = useState(HIVE_DEFAULT_QUERY);
  const hiveQuery = useHiveQuery();

  const handleRun = () => {
    hiveQuery.mutate({ statement });
    onQueryExecuted?.();
  };

  return (
    <Card title="Hive SQL">
      <div className="space-y-3">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            Quick queries
          </span>
          {HIVE_QUICK_QUERIES.map((quick) => (
            <button
              key={quick.label}
              type="button"
              onClick={() => setStatement(quick.statement)}
              className="rounded-md border border-slate-200 px-3 py-1 text-xs font-semibold text-slate-800 transition hover:border-slate-300 hover:bg-slate-50 focus:outline-none focus:ring-2 focus:ring-slate-400"
              title="Insert query into editor"
            >
              {quick.label}
            </button>
          ))}
        </div>
        <textarea
          value={statement}
          onChange={(e) => setStatement(e.target.value)}
          rows={6}
          className="w-full rounded-md border border-slate-200 px-3 py-2 text-sm font-mono text-slate-800"
        />
        <div className="flex items-center gap-3">
          <button
            onClick={handleRun}
            className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:opacity-60"
            disabled={hiveQuery.isPending}
          >
            {hiveQuery.isPending ? "Running..." : "Run query"}
          </button>
          {hiveQuery.isError && (
            <span className="text-sm text-red-600">
              {(hiveQuery.error as Error)?.message}
            </span>
          )}
        </div>
      </div>
    </Card>
  );
}
