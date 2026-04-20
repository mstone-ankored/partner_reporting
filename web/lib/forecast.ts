export type HistoryPoint = {
  month: string; // YYYY-MM-01
  leads: number;
  deals_won: number;
  revenue: number;
};

export type ForecastAssumptions = {
  horizonMonths: number;         // how many months out
  trailingMonths: number;        // how many months back to fit the baseline
  growthPctPerMonth: number;     // applied on top of the baseline trend
  leadToWonRate: number;         // override lead→won rate
  avgDealSize: number;           // override average deal size
  seasonality?: number[];        // 12 monthly multipliers, Jan..Dec; optional
};

function addMonth(iso: string, k: number): string {
  const [y, m] = iso.split("-").map(Number);
  const d = new Date(Date.UTC(y, (m - 1) + k, 1));
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-01`;
}

function mean(xs: number[]): number {
  if (!xs.length) return 0;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

export type ForecastPoint = HistoryPoint & { kind: "history" | "forecast" };

export function buildForecast(
  history: HistoryPoint[],
  a: ForecastAssumptions,
): ForecastPoint[] {
  const sorted = history.slice().sort((a, b) => a.month.localeCompare(b.month));
  const recent = sorted.slice(-Math.max(1, a.trailingMonths));
  const baseLeads = Math.max(0, mean(recent.map((r) => r.leads)));

  const out: ForecastPoint[] = sorted.map((p) => ({ ...p, kind: "history" }));
  const last = sorted.at(-1)?.month ?? "2024-01-01";

  for (let i = 1; i <= a.horizonMonths; i++) {
    const month = addMonth(last, i);
    const growthMult = Math.pow(1 + a.growthPctPerMonth / 100, i);
    const seasonMult = a.seasonality?.[(new Date(month).getUTCMonth())] ?? 1;
    const leads = baseLeads * growthMult * seasonMult;
    const deals_won = leads * a.leadToWonRate;
    const revenue = deals_won * a.avgDealSize;
    out.push({ month, leads, deals_won, revenue, kind: "forecast" });
  }
  return out;
}

export function defaultAssumptions(history: HistoryPoint[]): ForecastAssumptions {
  const recent = history.slice(-6);
  const leads = recent.reduce((s, r) => s + r.leads, 0);
  const won = recent.reduce((s, r) => s + r.deals_won, 0);
  const rev = recent.reduce((s, r) => s + r.revenue, 0);
  return {
    horizonMonths: 6,
    trailingMonths: 6,
    growthPctPerMonth: 3,
    leadToWonRate: leads > 0 ? won / leads : 0.05,
    avgDealSize: won > 0 ? rev / won : 10000,
  };
}
