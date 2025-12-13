import React from "react";
import { Card } from "../../props/Card";
import { QUICK_LINKS } from "../../../references/PathConfig";

export function QuickLinks() {
  return (
    <Card title="Quick Links">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {QUICK_LINKS.map((link) => (
          <a
            key={link.href}
            href={link.href}
            target="_blank"
            rel="noreferrer"
            className="flex flex-col gap-1 rounded-md border border-slate-200 px-3 py-3 transition hover:-translate-y-0.5 hover:border-slate-300 hover:shadow-sm focus:outline-none focus:ring-2 focus:ring-slate-400"
          >
            <span className="text-sm font-semibold text-slate-800">
              {link.label}
            </span>
            <span className="text-xs text-slate-500">{link.description}</span>
          </a>
        ))}
      </div>
    </Card>
  );
}
