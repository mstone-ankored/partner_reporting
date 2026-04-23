"use client";

import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
  BarChart, Bar, Legend, ScatterChart, Scatter, ZAxis,
} from "recharts";

const numberFmt = (v: unknown) =>
  typeof v === "number" && !isNaN(v) ? v.toLocaleString("en-US") : String(v ?? "");

const currencyFmt = (v: unknown) =>
  typeof v === "number" && !isNaN(v)
    ? v.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 })
    : String(v ?? "");

export function TrendLine({
  data,
  xKey,
  series,
  height = 280,
  yFormat = "number",
}: {
  data: Record<string, unknown>[];
  xKey: string;
  series: { key: string; label: string; color: string }[];
  height?: number;
  yFormat?: "number" | "currency";
}) {
  const tick = yFormat === "currency" ? currencyFmt : numberFmt;
  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid stroke="#1f242b" />
          <XAxis dataKey={xKey} stroke="#8a94a3" fontSize={11} />
          <YAxis stroke="#8a94a3" fontSize={11} tickFormatter={tick} width={72} />
          <Tooltip
            contentStyle={{ background: "#111418", border: "1px solid #1f242b" }}
            formatter={(v: number) => tick(v)}
          />
          <Legend />
          {series.map((s) => (
            <Line key={s.key} type="monotone" dataKey={s.key} name={s.label} stroke={s.color} dot={false} strokeWidth={2} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export function HBar({
  data,
  xKey,
  yKey,
  height = 280,
  color = "#4f8cff",
  xFormat = "currency",
}: {
  data: Record<string, unknown>[];
  xKey: string;
  yKey: string;
  height?: number;
  color?: string;
  xFormat?: "number" | "currency";
}) {
  const tick = xFormat === "currency" ? currencyFmt : numberFmt;
  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <BarChart data={data} layout="vertical" margin={{ top: 8, right: 16, left: 8, bottom: 0 }}>
          <CartesianGrid stroke="#1f242b" />
          <XAxis type="number" stroke="#8a94a3" fontSize={11} tickFormatter={tick} />
          <YAxis type="category" dataKey={yKey} stroke="#8a94a3" fontSize={11} width={140} />
          <Tooltip
            contentStyle={{ background: "#111418", border: "1px solid #1f242b" }}
            formatter={(v: number) => tick(v)}
          />
          <Bar dataKey={xKey} fill={color} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function QuadrantScatter({
  data,
  height = 360,
}: {
  data: { partner_name: string; total_leads: number; lead_to_won_rate: number; revenue_closed_won: number }[];
  height?: number;
}) {
  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <ScatterChart margin={{ top: 8, right: 16, left: 8, bottom: 0 }}>
          <CartesianGrid stroke="#1f242b" />
          <XAxis type="number" dataKey="total_leads" name="Leads" stroke="#8a94a3" fontSize={11} />
          <YAxis type="number" dataKey="lead_to_won_rate" name="Lead → won" stroke="#8a94a3" fontSize={11} tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
          <ZAxis type="number" dataKey="revenue_closed_won" name="Revenue" range={[50, 400]} />
          <Tooltip
            contentStyle={{ background: "#111418", border: "1px solid #1f242b" }}
            formatter={(v: number, k: string) => (k === "lead_to_won_rate" ? `${(v * 100).toFixed(1)}%` : v)}
            labelFormatter={() => ""}
          />
          <Scatter data={data} fill="#4f8cff" />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}
