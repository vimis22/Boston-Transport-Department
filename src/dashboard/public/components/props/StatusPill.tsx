import React from "react";
import clsx from "clsx";

type Variant = "success" | "warning" | "info" | "neutral" | "danger";

type Props = {
  label: string;
  variant?: Variant;
  className?: string;
};

const VARIANT_STYLES: Record<Variant, string> = {
  success: "border-emerald-200 bg-emerald-50 text-emerald-700",
  warning: "border-amber-200 bg-amber-50 text-amber-700",
  info: "border-sky-200 bg-sky-50 text-sky-700",
  neutral: "border-slate-200 bg-slate-50 text-slate-700",
  danger: "border-rose-200 bg-rose-50 text-rose-700",
};

export function StatusPill({ label, variant = "neutral", className }: Props) {
  return (
    <span
      className={clsx(
        "inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold",
        VARIANT_STYLES[variant],
        className
      )}
      aria-label={label}
    >
      {label}
    </span>
  );
}


