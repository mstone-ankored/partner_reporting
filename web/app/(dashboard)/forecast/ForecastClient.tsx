"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { buildForecast, defaultAssumptions, HistoryPoint } from "@/lib/forecast";
import { Panel, KpiCard, fmtInt, fmtMoney, fmtPct } from "@/components/ui";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";

type Props = {
  partners: { partner_id: string; partner_name: string }[];
  selectedPartner: string;
  history: HistoryPoint[];
};

export function ForecastClient({ partners, selectedPartner, history }: Props) {
  const router = useRouter();
  const [a, setA] = useState(() => defaultAssumptions(history));

  const forecast = useMemo(() => buildForecast(history, a), [history, a]);
  const lastHistMonth = history.at(-1)?.month ?? "";

  const totals = forecast
    .filter((r) => r.kind === "forecast")
    .reduce(
      (s, r) => ({
        leads: s.leads + r.leads,
        deals: s.deals + r.deals_won,
        revenue: s.revenue + r.revenue,
      }),
      { leads: 0, deals: 0, revenue: 0 },
    );

  function onPartnerChange(e: React.ChangeEvent<HTMLSelectElement>) {
    const v = e.target.value;
    const q = v ? `?partner=${encodeURIComponent(v)}` : "";
    router.replace(`/forecast${q}`);
  }

  return (
    <>
      <div className="flex items-center gap-3 mb-4">
        <label className="text-sm">
          Scope:{" "}
          <select
            value={selectedPartner}
            onChange={onPartnerChange}
            className="bg-panel border border-border rounded px-2 py-1"
          >
            <option value="">All partners</option>
            {partners.map((p) => (
              <option key={p.partner_id} value={p.partner_id}>
                {p.partner_name}
              </option>
            ))}
          </select>
        </label>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-4">
        <Panel title="Assumptions" className="lg:col-span-1">
          <div className="space-y-3 text-sm">
            <NumField label="Horizon (months)" value={a.horizonMonths} step={1} min={1} max={36}
              onChange={(v) => setA((s) => ({ ...s, horizonMonths: v }))} />
            <NumField label="Trailing months fit" value={a.trailingMonths} step={1} min={1} max={36}
              onChange={(v) => setA((s) => ({ ...s, trailingMonths: v }))} />
            <NumField label="Growth % / month" value={a.growthPctPerMonth} step={0.5} min={-20} max={50}
              onChange={(v) => setA((s) => ({ ...s, growthPctPerMonth: v }))} />
            <NumField label="Lead → won rate" value={a.leadToWonRate} step={0.005} min={0} max={1}
              onChange={(v) => setA((s) => ({ ...s, leadToWonRate: v }))} />
            <NumField label="Avg deal size ($)" value={a.avgDealSize} step={500} min={0}
              onChange={(v) => setA((s) => ({ ...s, avgDealSize: v }))} />
          </div>
        </Panel>

        <Panel title="Projected totals (forecast window)" className="lg:col-span-2">
          <div className="grid grid-cols-3 gap-3">
            <KpiCard label="Leads" value={fmtInt(Math.round(totals.leads))} />
            <KpiCard label="Deals won" value={fmtInt(Math.round(totals.deals))} sub={`@ ${fmtPct(a.leadToWonRate, 2)}`} />
            <KpiCard label="Revenue" value={fmtMoney(totals.revenue)} sub={`@ ${fmtMoney(a.avgDealSize)} avg`} />
          </div>
          <div className="mt-4" style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
              <LineChart
                data={forecast.map((r) => ({
                  month: r.month.slice(0, 7),
                  leads: r.kind === "history" ? r.leads : null,
                  leads_forecast: r.kind === "forecast" ? r.leads : null,
                  revenue: r.kind === "history" ? r.revenue : null,
                  revenue_forecast: r.kind === "forecast" ? r.revenue : null,
                }))}
                margin={{ top: 8, right: 16, left: 0, bottom: 0 }}
              >
                <CartesianGrid stroke="#1f242b" />
                <XAxis dataKey="month" stroke="#8a94a3" fontSize={11} />
                <YAxis yAxisId="l" stroke="#8a94a3" fontSize={11} />
                <YAxis yAxisId="r" orientation="right" stroke="#8a94a3" fontSize={11} />
                <Tooltip contentStyle={{ background: "#111418", border: "1px solid #1f242b" }} />
                <Legend />
                <ReferenceLine x={lastHistMonth.slice(0, 7)} stroke="#8a94a3" strokeDasharray="3 3" yAxisId="l" />
                <Line yAxisId="l" type="monotone" dataKey="leads" name="Leads (hist)" stroke="#21c07a" dot={false} strokeWidth={2} />
                <Line yAxisId="l" type="monotone" dataKey="leads_forecast" name="Leads (fcst)" stroke="#21c07a" strokeDasharray="5 5" dot={false} strokeWidth={2} />
                <Line yAxisId="r" type="monotone" dataKey="revenue" name="Revenue (hist)" stroke="#4f8cff" dot={false} strokeWidth={2} />
                <Line yAxisId="r" type="monotone" dataKey="revenue_forecast" name="Revenue (fcst)" stroke="#4f8cff" strokeDasharray="5 5" dot={false} strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Panel>
      </div>

      <Panel title="Month-by-month">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-muted text-xs uppercase">
              <tr className="border-b border-border">
                <th className="text-left py-2 pr-3">Month</th>
                <th className="text-left py-2 pr-3">Kind</th>
                <th className="text-right py-2 pr-3">Leads</th>
                <th className="text-right py-2 pr-3">Deals won</th>
                <th className="text-right py-2 pr-3">Revenue</th>
              </tr>
            </thead>
            <tbody>
              {forecast.map((r) => (
                <tr key={r.month} className="border-b border-border/50">
                  <td className="py-2 pr-3">{r.month.slice(0, 7)}</td>
                  <td className="py-2 pr-3 text-xs">
                    <span className={r.kind === "forecast" ? "text-warn" : "text-muted"}>
                      {r.kind}
                    </span>
                  </td>
                  <td className="py-2 pr-3 text-right">{fmtInt(Math.round(r.leads))}</td>
                  <td className="py-2 pr-3 text-right">{fmtInt(Math.round(r.deals_won))}</td>
                  <td className="py-2 pr-3 text-right">{fmtMoney(r.revenue)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </>
  );
}

function NumField({
  label,
  value,
  onChange,
  step,
  min,
  max,
}: {
  label: string;
  value: number;
  onChange: (v: number) => void;
  step?: number;
  min?: number;
  max?: number;
}) {
  return (
    <label className="flex items-center justify-between gap-3">
      <span className="text-muted">{label}</span>
      <input
        type="number"
        value={value}
        step={step}
        min={min}
        max={max}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-32 bg-bg border border-border rounded px-2 py-1 text-right"
      />
    </label>
  );
}
