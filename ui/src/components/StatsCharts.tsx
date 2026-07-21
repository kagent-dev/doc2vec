import { useMemo } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { ConfigStats } from '../api';
import { formatDuration } from '../lib/format';

// Status palette (dataviz reference): reserved status colors, never reused as series
const STATUS_COLORS: Record<string, string> = {
  succeeded: 'var(--status-good)',
  failed: 'var(--status-critical)',
  skipped: 'var(--status-warning)',
  canceled: 'var(--text-muted)',
};
const STACK_ORDER = ['succeeded', 'failed', 'skipped', 'canceled'] as const;

function StatTile({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="rounded-lg border border-edge bg-surface px-4 py-3">
      <p className="text-xs uppercase tracking-wide text-ink-muted">{label}</p>
      <p className="mt-1 text-2xl font-semibold">{value}</p>
      {sub && <p className="mt-0.5 text-xs text-ink-muted">{sub}</p>}
    </div>
  );
}

function ChartTooltip({ active, payload, label, unit }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-md border border-edge bg-surface px-3 py-2 text-xs shadow-sm">
      <p className="mb-1 font-medium text-ink-secondary">{label}</p>
      {payload.map((entry: any) => (
        <p key={entry.dataKey} className="flex items-center gap-2 text-ink-secondary">
          <span className="inline-block h-2 w-2 rounded-full" style={{ background: entry.color }} />
          {entry.name}: <span className="font-medium text-ink">{unit === 's' ? formatDuration(entry.value) : entry.value}</span>
        </p>
      ))}
    </div>
  );
}

export default function StatsCharts({ stats }: { stats: ConfigStats }) {
  const dailyData = useMemo(() => {
    const byDay = new Map<string, Record<string, number | string>>();
    for (const row of stats.daily) {
      const entry = byDay.get(row.day) ?? { day: row.day.slice(5) };
      entry[row.status] = row.count;
      byDay.set(row.day, entry);
    }
    return [...byDay.values()];
  }, [stats.daily]);

  const durationData = useMemo(
    () =>
      stats.recentDurations.map(r => ({
        id: `#${r.id}`,
        duration: Math.round(r.duration_s * 10) / 10,
      })),
    [stats.recentDurations]
  );

  const successRate = stats.totals.total > 0
    ? Math.round((stats.totals.succeeded / Math.max(stats.totals.total - stats.totals.skipped, 1)) * 100)
    : null;
  const avgDuration = stats.recentDurations.length > 0
    ? stats.recentDurations.reduce((acc, r) => acc + r.duration_s, 0) / stats.recentDurations.length
    : null;

  const axisTick = { fill: 'var(--text-muted)', fontSize: 11 };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatTile label="Runs" value={String(stats.totals.total)} />
        <StatTile
          label="Success rate"
          value={successRate === null ? '—' : `${successRate}%`}
          sub={stats.totals.failed > 0 ? `${stats.totals.failed} failed` : undefined}
        />
        <StatTile label="Skipped" value={String(stats.totals.skipped)} sub="overlapping schedule" />
        <StatTile label="Avg duration" value={avgDuration === null ? '—' : formatDuration(avgDuration)} sub="last 50 runs" />
      </div>

      <div className="rounded-lg border border-edge bg-surface p-4">
        <h3 className="mb-3 text-sm font-semibold text-ink-secondary">Runs per day</h3>
        {dailyData.length === 0 ? (
          <p className="py-10 text-center text-sm text-ink-muted">No runs in this period.</p>
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={dailyData} margin={{ top: 4, right: 8, left: -18, bottom: 0 }}>
              <CartesianGrid vertical={false} stroke="var(--gridline)" />
              <XAxis dataKey="day" tick={axisTick} axisLine={{ stroke: 'var(--baseline)' }} tickLine={false} />
              <YAxis tick={axisTick} axisLine={false} tickLine={false} allowDecimals={false} />
              <Tooltip content={<ChartTooltip />} cursor={{ fill: 'var(--gridline)', opacity: 0.4 }} />
              <Legend wrapperStyle={{ fontSize: 12, color: 'var(--text-secondary)' }} iconSize={8} />
              {STACK_ORDER.map((status, i) => (
                <Bar
                  key={status}
                  dataKey={status}
                  name={status}
                  stackId="runs"
                  fill={STATUS_COLORS[status]}
                  stroke="var(--surface-1)"
                  strokeWidth={1}
                  maxBarSize={28}
                  radius={i === STACK_ORDER.length - 1 ? [3, 3, 0, 0] : undefined}
                  isAnimationActive={false}
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      <div className="rounded-lg border border-edge bg-surface p-4">
        <h3 className="mb-3 text-sm font-semibold text-ink-secondary">Run duration (last {durationData.length} runs)</h3>
        {durationData.length === 0 ? (
          <p className="py-10 text-center text-sm text-ink-muted">No finished runs yet.</p>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={durationData} margin={{ top: 4, right: 8, left: -10, bottom: 0 }}>
              <CartesianGrid vertical={false} stroke="var(--gridline)" />
              <XAxis dataKey="id" tick={axisTick} axisLine={{ stroke: 'var(--baseline)' }} tickLine={false} />
              <YAxis
                tick={axisTick}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v: number) => formatDuration(v)}
              />
              <Tooltip content={<ChartTooltip unit="s" />} cursor={{ stroke: 'var(--baseline)' }} />
              <Line
                type="monotone"
                dataKey="duration"
                name="duration"
                stroke="var(--series-1)"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, strokeWidth: 2, stroke: 'var(--surface-1)' }}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
