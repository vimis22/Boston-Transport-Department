import React from "react";
import clsx from "clsx";

type Props = {
  label: string;
  value: React.ReactNode;
  hint?: React.ReactNode;
  icon?: React.ReactNode;
  className?: string;
};

export function StatCard({ label, value, hint, icon, className }: Props) {
  return (
    <div
      className={clsx(
        "rounded-lg border border-slate-200 bg-white px-4 py-3 shadow-sm",
        "flex items-center gap-3",
        className
      )}
    >
      {icon && (
        <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-slate-50 text-slate-600">
          {icon}
        </div>
      )}
      <div className="min-w-0">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
          {label}
        </div>
        <div className="truncate text-lg font-semibold text-slate-900">
          {value}
        </div>
        {hint && <div className="text-xs text-slate-500">{hint}</div>}
      </div>
    </div>
  );
}


