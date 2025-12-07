import React, { useMemo, useState } from "react";
import { useHiveQuery } from "../api/hooks";
import { Card } from "../components/Card";

const DEFAULT_QUERY = "SELECT * FROM bike_data_raw LIMIT 10";

export function HiveTab() {
  const [statement, setStatement] = useState(DEFAULT_QUERY);
  const hiveQuery = useHiveQuery();

  const handleRun = () => {
    hiveQuery.mutate({ statement });
  };

  const columns = hiveQuery.data?.columns ?? [];
  const rows = hiveQuery.data?.data ?? [];

  const renderedRows = useMemo(() => {
    if (!rows.length) return [];
    return rows.map((row, idx) => (
      <tr key={idx} className="border-b border-slate-100 last:border-0">
        {columns.map((col) => (
          <td key={col} className="px-3 py-2 text-xs text-slate-700">
            {String((row as Record<string, unknown>)[col] ?? "")}
          </td>
        ))}
      </tr>
    ));
  }, [rows, columns]);

  return (
    <div className="space-y-4">
      <Card title="Hive SQL">
        <div className="space-y-3">
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

      <Card
        title="Results"
        actions={
          hiveQuery.data ? (
            <span className="text-xs text-slate-500">
              {hiveQuery.data.row_count ?? rows.length} rows
            </span>
          ) : null
        }
      >
        <div className="overflow-auto">
          <table className="min-w-full text-left">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50">
                {columns.map((col) => (
                  <th
                    key={col}
                    className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-600"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>{renderedRows}</tbody>
          </table>
          {!rows.length && (
            <p className="py-4 text-sm text-slate-600">No results yet.</p>
          )}
        </div>
      </Card>
    </div>
  );
}

