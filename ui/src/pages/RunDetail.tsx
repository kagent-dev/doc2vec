import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import { api, ApiError } from '../api';
import { formatDuration, formatTime, runDuration } from '../lib/format';
import LogViewer from '../components/LogViewer';
import RunStatusBadge from '../components/RunStatusBadge';

export default function RunDetail() {
  const { id } = useParams();
  const runId = Number(id);
  const queryClient = useQueryClient();

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

  if (error) return <p className="text-critical">{(error as ApiError).message}</p>;
  if (!run) return <p className="text-ink-muted">Loading…</p>;

  const active = run.status === 'queued' || run.status === 'running';
  const sources = run.stats?.sources ?? [];

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
            ['Trigger', run.trigger],
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
      </div>

      {sources.length > 0 && (
        <div className="overflow-x-auto rounded-lg border border-edge bg-surface">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-edge text-left text-xs uppercase tracking-wide text-ink-muted">
                <th className="px-4 py-2 font-medium">Source</th>
                <th className="px-4 py-2 font-medium">Type</th>
                <th className="px-4 py-2 font-medium">Version</th>
                <th className="px-4 py-2 font-medium">Duration</th>
                <th className="px-4 py-2 font-medium">Result</th>
              </tr>
            </thead>
            <tbody>
              {sources.map(source => (
                <tr key={`${source.product_name}-${source.type}`} className="border-b border-edge/60 last:border-0">
                  <td className="px-4 py-2 font-medium">{source.product_name}</td>
                  <td className="px-4 py-2 text-ink-secondary">{source.type}</td>
                  <td className="px-4 py-2 text-ink-secondary">{source.version}</td>
                  <td className="px-4 py-2 text-ink-secondary">{formatDuration(source.duration_ms / 1000)}</td>
                  <td className="px-4 py-2">
                    {source.ok ? (
                      <span className="text-good-text">✓ ok</span>
                    ) : (
                      <span className="text-critical" title={source.error}>✕ {source.error ?? 'failed'}</span>
                    )}
                  </td>
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
