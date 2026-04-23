"use client";

import { useState, useTransition } from "react";
import type { PartnerWithCustomerCount, CustomerCountEntry } from "@/lib/partners-info";
import { fmtInt } from "@/components/ui";
import { upsertCustomerCount, deleteCustomerCount } from "./actions";

// Format a Date as YYYY-MM-DD in UTC (the <input type="date"> format).
function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

export function PartnerRow({
  partner,
  history,
}: {
  partner: PartnerWithCustomerCount;
  history: CustomerCountEntry[];
}) {
  const [open, setOpen] = useState(false);
  const [pending, start] = useTransition();

  return (
    <>
      <tr className="border-b border-border/50 hover:bg-bg">
        <td className="py-2 pr-3 font-medium">{partner.partner_name}</td>
        <td className="py-2 pr-3 text-xs text-muted">{partner.partner_domain || "—"}</td>
        <td className="py-2 pr-3 text-xs">{partner.partner_tier || "—"}</td>
        <td className="py-2 pr-3 text-xs">{partner.partner_start_date || "—"}</td>
        <td className="py-2 pr-3 text-right">
          {partner.latest_customer_count != null ? fmtInt(partner.latest_customer_count) : "—"}
        </td>
        <td className="py-2 pr-3 text-xs text-muted">{partner.latest_as_of_date || "—"}</td>
        <td className="py-2 pr-3 text-right">
          <button
            onClick={() => setOpen((v) => !v)}
            className="text-xs text-accent hover:underline"
          >
            {open ? "Hide" : "Edit"}
          </button>
        </td>
      </tr>
      {open ? (
        <tr className="border-b border-border/50 bg-bg">
          <td colSpan={7} className="p-4">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <form
                action={(fd) => {
                  start(() => {
                    void upsertCustomerCount(fd).then(() => setOpen(false));
                  });
                }}
                className="space-y-2"
              >
                <input type="hidden" name="partner_id" value={partner.partner_id} />
                <div className="text-xs font-semibold text-muted uppercase tracking-wide">
                  Add / update customer count
                </div>
                <div className="flex gap-2">
                  <label className="flex flex-col text-xs gap-1 flex-1">
                    <span className="text-muted">As-of date</span>
                    <input
                      type="date"
                      name="as_of_date"
                      required
                      defaultValue={todayIso()}
                      className="rounded border border-border bg-panel px-2 py-1 text-sm"
                    />
                  </label>
                  <label className="flex flex-col text-xs gap-1 flex-1">
                    <span className="text-muted">Total customer count</span>
                    <input
                      type="number"
                      name="total_customer_count"
                      required
                      min={0}
                      step={1}
                      placeholder="e.g. 1250"
                      className="rounded border border-border bg-panel px-2 py-1 text-sm"
                    />
                  </label>
                </div>
                <label className="flex flex-col text-xs gap-1">
                  <span className="text-muted">Notes (optional)</span>
                  <input
                    type="text"
                    name="notes"
                    maxLength={500}
                    placeholder="Source / context"
                    className="rounded border border-border bg-panel px-2 py-1 text-sm"
                  />
                </label>
                <div className="flex justify-end gap-2 pt-1">
                  <button
                    type="button"
                    onClick={() => setOpen(false)}
                    className="text-xs px-3 py-1.5 rounded border border-border text-muted hover:text-text"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    disabled={pending}
                    className="text-xs px-3 py-1.5 rounded bg-accent text-white disabled:opacity-50"
                  >
                    {pending ? "Saving…" : "Save"}
                  </button>
                </div>
                <div className="text-xs text-muted">
                  Saving overwrites the entry for the same as-of date. Used by
                  the penetration mart as{" "}
                  <code>our_customers / total_customer_count</code>.
                </div>
              </form>

              <div>
                <div className="text-xs font-semibold text-muted uppercase tracking-wide mb-2">
                  History
                </div>
                {history.length === 0 ? (
                  <div className="text-xs text-muted">No entries yet.</div>
                ) : (
                  <table className="w-full text-xs">
                    <thead className="text-muted">
                      <tr className="border-b border-border">
                        <th className="text-left py-1">As of</th>
                        <th className="text-right py-1">Count</th>
                        <th className="text-left py-1">Notes</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {history.map((h) => (
                        <tr key={h.id} className="border-b border-border/50">
                          <td className="py-1">{h.as_of_date}</td>
                          <td className="py-1 text-right">{fmtInt(h.total_customer_count)}</td>
                          <td className="py-1 text-muted">{h.notes || "—"}</td>
                          <td className="py-1 text-right">
                            <button
                              onClick={() => {
                                if (!confirm(`Delete entry for ${h.as_of_date}?`)) return;
                                start(() => {
                                  void deleteCustomerCount(h.id);
                                });
                              }}
                              disabled={pending}
                              className="text-bad hover:underline disabled:opacity-50"
                            >
                              Delete
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}
