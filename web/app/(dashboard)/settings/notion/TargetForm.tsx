"use client";

import { useMemo, useState } from "react";
import type { NotionSourceDef } from "@/lib/notion-targets";
import { upsertTarget } from "./actions";

export function TargetForm({ sources }: { sources: NotionSourceDef[] }) {
  const [sourceName, setSourceName] = useState(sources[0]?.name ?? "");
  const source = useMemo(
    () => sources.find((s) => s.name === sourceName) ?? sources[0],
    [sources, sourceName],
  );
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [columnMap, setColumnMap] = useState<Record<string, string>>(() =>
    Object.fromEntries(source.columns.map((c) => [c.key, c.label])),
  );

  function onSourceChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const next = sources.find((s) => s.name === e.target.value);
    setSourceName(e.target.value);
    setFilters({});
    setColumnMap(Object.fromEntries((next?.columns ?? []).map((c) => [c.key, c.label])));
  }

  function toggleCol(key: string) {
    setColumnMap((prev) => {
      const copy = { ...prev };
      if (key in copy) delete copy[key];
      else copy[key] = source.columns.find((c) => c.key === key)?.label ?? key;
      return copy;
    });
  }

  return (
    <form action={upsertTarget} className="space-y-4 text-sm">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <label className="block">
          <span className="text-muted">Source mart</span>
          <select
            name="source_table"
            value={sourceName}
            onChange={onSourceChange}
            className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5"
          >
            {sources.map((s) => (
              <option key={s.name} value={s.name}>
                {s.label}
              </option>
            ))}
          </select>
        </label>
        <label className="block">
          <span className="text-muted">Notion database ID</span>
          <input
            name="notion_database_id"
            required
            placeholder="e.g. 1234abcd5678ef90…"
            className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5 font-mono"
          />
        </label>
      </div>

      {source.filters.length > 0 && (
        <div>
          <div className="text-xs uppercase text-muted mb-1">Filters</div>
          <div className="flex flex-wrap gap-3">
            {source.filters.map((f) => (
              <label key={f.key} className="text-sm">
                {f.label}:{" "}
                <select
                  value={filters[f.key] || ""}
                  onChange={(e) => setFilters((p) => ({ ...p, [f.key]: e.target.value }))}
                  className="bg-bg border border-border rounded px-2 py-1"
                >
                  <option value="">— any —</option>
                  {f.values.map((v) => (
                    <option key={v} value={v}>
                      {v}
                    </option>
                  ))}
                </select>
              </label>
            ))}
          </div>
        </div>
      )}

      <div>
        <div className="text-xs uppercase text-muted mb-1">Columns to push</div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-y-1 gap-x-4">
          {source.columns.map((c) => {
            const included = c.key in columnMap;
            return (
              <div key={c.key} className="flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={included}
                  onChange={() => toggleCol(c.key)}
                  id={`col-${c.key}`}
                />
                <label htmlFor={`col-${c.key}`} className="flex-1 flex items-center gap-2">
                  <span className="w-48 font-mono text-xs text-muted">{c.key}</span>
                  <input
                    type="text"
                    value={columnMap[c.key] ?? ""}
                    onChange={(e) =>
                      setColumnMap((p) => ({ ...p, [c.key]: e.target.value }))
                    }
                    disabled={!included}
                    className="flex-1 bg-bg border border-border rounded px-2 py-1 text-xs"
                    placeholder="Notion property name"
                  />
                </label>
              </div>
            );
          })}
        </div>
      </div>

      <label className="flex items-center gap-2">
        <input type="checkbox" name="enabled" defaultChecked />
        <span>Enabled</span>
      </label>

      <input type="hidden" name="filter_json" value={JSON.stringify(filters)} />
      <input type="hidden" name="column_map_json" value={JSON.stringify(columnMap)} />
      <button type="submit" className="bg-accent text-white rounded px-3 py-1.5 font-medium">
        Add target
      </button>
    </form>
  );
}
