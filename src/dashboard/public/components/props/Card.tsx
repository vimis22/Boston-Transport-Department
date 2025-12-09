import React from "react";
import clsx from "clsx";

type Props = {
  title?: string;
  className?: string;
  children: React.ReactNode;
  actions?: React.ReactNode;
};

export function Card({ title, children, className, actions }: Props) {
  return (
    <div
      className={clsx(
        "rounded-lg border border-slate-200 bg-white shadow-sm p-4",
        className
      )}
    >
      {(title || actions) && (
        <div className="mb-3 flex items-center justify-between gap-3">
          {title && <h3 className="text-sm font-semibold text-slate-700">{title}</h3>}
          {actions}
        </div>
      )}
      {children}
    </div>
  );
}

