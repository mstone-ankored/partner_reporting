"use client";

import { useTransition } from "react";
import type { NotionTargetRow } from "@/lib/notion-targets";
import { deleteTarget } from "./actions";

export function TargetRow({ target }: { target: NotionTargetRow }) {
  const [pending, start] = useTransition();
  return (
    <tr className="border-b border-border/50">
      <td className="py-2 pr-3 font-mono text-xs">{target.source_table}</td>
      <td className="py-2 pr-3 font-mono text-xs">{target.notion_database_id}</td>
      <td className="py-2 pr-3">{target.enabled ? "on" : "off"}</td>
      <td className="py-2 pr-3 text-xs">
        {target.last_synced_at ? (
          <>
            {new Date(target.last_synced_at).toLocaleString()}{" "}
            <span className={target.last_sync_status === "ok" ? "text-good" : "text-bad"}>
              ({target.last_sync_status})
            </span>
          </>
        ) : (
          <span className="text-muted">never</span>
        )}
        {target.last_sync_message ? (
          <div className="text-muted">{target.last_sync_message}</div>
        ) : null}
      </td>
      <td className="py-2 pr-3 text-right">
        <button
          onClick={() => {
            if (!confirm("Delete this sync target?")) return;
            start(() => {
              void deleteTarget(target.id);
            });
          }}
          disabled={pending}
          className="text-xs text-bad hover:underline disabled:opacity-50"
        >
          Delete
        </button>
      </td>
    </tr>
  );
}
