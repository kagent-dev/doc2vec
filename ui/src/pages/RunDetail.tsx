import { useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { api, ApiError, SourceRunStats } from '../api';
import { formatDuration, formatTime, runDuration } from '../lib/format';
import LogViewer from '../components/LogViewer';
import RunStatusBadge from '../components/RunStatusBadge';

type SourceSortKey = 'source' | 'type' | 'version' | 'duration' | 'changes' | 'result';

function sourceSortValue(source: SourceRunStats, key: SourceSortKey): string | number {
  switch (key) {
    case 'source': return source.product_name;
    case 'type': return source.type;
    case 'version': return source.version ?? '';
    case 'duration': return source.duration_ms;
    case 'changes': {
      const c = source.counters;
      return c ? c.items_new + c.items_updated + c.items_deleted : -1;
    }
    case 'result': return source.ok ? 1 : 0;
  }
}

export default function RunDetail() {
  const { id } = useParams();
  const runId = Number(id);
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const { data: run, error } = useQuery({
    queryKey: ['runs', runId],
    queryFn: () => api.run(runId),
    refetchInterval: query =>
      query.state.data && ['queued', 'running'].includes(query.state.data.status) ? 3000 : false,
  });

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(runId),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['runs', runId] }),
  });

  const [sort, setSort] = useState<{ key: SourceSortKey; dir: 1 | -1 } | null>(null);
  // Keep each source's original position: it's the key of the per-source detail
  // route, which must not change when the table is re-sorted.
  const sortedSources = useMemo(() => {
    const rows = (run?.stats?.sources ?? []).map((source, index) => ({ source, index }));
    if (!sort) return rows;
    return [...rows].sort((a, b) => {
      const va = sourceSortValue(a.source, sort.key);
      const vb = sourceSortValue(b.source, sort.key);
      const cmp = typeof va === 'string' && typeof vb === 'string' ? va.localeCompare(vb) : Number(va) - Number(vb);
      return cmp * sort.dir;
    });
  }, [run, sort]);

  if (error) return <p className="text-critical">{(error as ApiError).message}</p>;
  if (!run) return <p className="text-ink-muted">Loading…</p>;

  const active = run.status === 'queued' || run.status === 'running';

  // Clicking a header sorts ascending, again descending, a third time restores config order
  const sortHeader = (label: string, key: SourceSortKey) => (
    <th className="px-4 py-2 font-medium">
      <button
        onClick={() => setSort(prev =>
          prev?.key !== key ? { key, dir: 1 } : prev.dir === 1 ? { key, dir: -1 } : null
        )}
        className={`inline-flex items-center gap-1 uppercase tracking-wide transition hover:text-ink-secondary ${
          sort?.key === key ? 'text-ink-secondary' : ''
        }`}
      >
        {label}
        <span className="text-[10px]">{sort?.key === key ? (sort.dir === 1 ? '▲' : '▼') : ''}</span>
      </button>
    </th>
  );

  return (
    <div className="space-y-6">
      <div>
        <Link to={`/configs/${run.config_id}`} className="text-sm text-ink-muted hover:text-accent">
          ← {run.config_name ?? `Config ${run.config_id}`}
        </Link>
        <div className="mt-2 flex flex-wrap items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight">Run #{run.id}</h1>
          <RunStatusBadge status={run.status} />
          {active && (
            <button
              onClick={() => cancel.mutate()}
              disabled={cancel.isPending}
              className="ml-auto rounded-md border border-critical/40 px-3.5 py-1.5 text-sm font-medium text-critical transition hover:bg-critical/10 disabled:opacity-40"
            >
              Cancel run
            </button>
          )}
        </div>
        {run.error && <p className="mt-2 text-sm text-critical">{run.error}</p>}
        {cancel.error && <p className="mt-2 text-sm text-critical">{(cancel.error as ApiError).message}</p>}
        <dl className="mt-3 flex flex-wrap gap-x-8 gap-y-2 text-sm">
          {[
            ['Trigger', run.requested_sources?.length ? `${run.trigger} (partial)` : run.trigger],
            ['Queued', formatTime(run.queued_at)],
            ['Started', formatTime(run.started_at)],
            ['Duration', active ? '…' : runDuration(run.started_at, run.finished_at)],
            ['Exit code', run.exit_code === null ? '—' : String(run.exit_code)],
            ['Warnings', String(run.stats?.warn_count ?? 0)],
            ['Errors', String(run.stats?.error_count ?? 0)],
          ].map(([label, value]) => (
            <div key={label}>
              <dt className="text-xs uppercase tracking-wide text-ink-muted">{label}</dt>
              <dd className="mt-0.5 text-ink-secondary">{value}</dd>
            </div>
          ))}
        </dl>
        {run.requested_sources && run.requested_sources.length > 0 && (
          <p className="mt-2 text-sm text-ink-secondary">
            <span className="text-xs uppercase tracking-wide text-ink-muted">Selected sources</span>{' '}
            <span className="ml-1 inline-flex flex-wrap gap-1.5 align-middle">
              {run.requested_sources.map(source => (
                <span key={source.index} className="rounded-full border border-edge bg-page px-2 py-0.5 text-xs">
                  {source.product_name}
                  <span className="ml-1 text-ink-muted">{source.type}{source.version ? ` · ${source.version}` : ''}</span>
                </span>
              ))}
            </span>
          </p>
        )}
      </div>

      {sortedSources.length > 0 && (
        <div className="overflow-x-auto rounded-lg border border-edge bg-surface">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-edge text-left text-xs uppercase tracking-wide text-ink-muted">
                {sortHeader('Source', 'source')}
                {sortHeader('Type', 'type')}
                {sortHeader('Version', 'version')}
                {sortHeader('Duration', 'duration')}
                {sortHeader('Changes', 'changes')}
                {sortHeader('Result', 'result')}
                <th className="px-4 py-2" />
              </tr>
            </thead>
            <tbody>
              {sortedSources.map(({ source, index }) => (
                <tr
                  key={`${source.product_name}-${source.type}-${index}`}
                  onClick={() => navigate(`/runs/${run.id}/sources/${index}`)}
                  className="cursor-pointer border-b border-edge/60 transition last:border-0 hover:bg-edge/20"
                >
                  <td className="px-4 py-2 font-medium">{source.product_name}</td>
                  <td className="px-4 py-2 text-ink-secondary">{source.type}</td>
                  <td className="px-4 py-2 text-ink-secondary">{source.version}</td>
                  <td className="px-4 py-2 text-ink-secondary">{formatDuration(source.duration_ms / 1000)}</td>
                  <td className="px-4 py-2 text-ink-secondary">
                    {source.counters ? (
                      <span className="whitespace-nowrap tabular-nums">
                        <span className={source.counters.items_new > 0 ? 'text-good-text' : ''}>+{source.counters.items_new}</span>
                        {' '}
                        <span>~{source.counters.items_updated}</span>
                        {' '}
                        <span className={source.counters.items_deleted > 0 ? 'text-critical' : ''}>−{source.counters.items_deleted}</span>
                        <span className="ml-1 text-xs text-ink-muted">{source.counters.items_kind}</span>
                      </span>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="px-4 py-2">
                    {source.ok ? (
                      <span className="text-good-text">✓ ok</span>
                    ) : (
                      <span className="text-critical" title={source.error}>✕ {source.error ?? 'failed'}</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-ink-muted">›</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div>
        <h2 className="mb-2 text-sm font-semibold text-ink-secondary">Logs</h2>
        <LogViewer runId={runId} isActive={active} />
      </div>
    </div>
  );
}
