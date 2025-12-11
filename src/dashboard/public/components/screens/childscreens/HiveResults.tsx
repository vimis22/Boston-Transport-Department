import React, { useMemo } from "react";
import { useHiveQuery } from "../../../api/hooks";
import { Card } from "../../props/Card";

export function HiveResults() {
  const hiveQuery = useHiveQuery();

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
  );
}
