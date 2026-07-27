import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from 'react-router-dom';
import { api, ApiError, ConfigRecord } from '../api';
import { formatTime, humanizeCron, relativeTime, runDuration } from '../lib/format';
import RunStatusBadge from '../components/RunStatusBadge';
import ConfigForm from '../components/ConfigForm';
import SourceBadges from '../components/SourceBadges';

function RunNowButton({ config }: { config: ConfigRecord }) {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: () => api.triggerRun(config.id),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['configs'] }),
  });
  const disabled = config.busy || !!config.parse_error || mutation.isPending;
  return (
    <div className="flex items-center gap-2">
      <button
        onClick={e => {
          e.preventDefault();
          mutation.mutate();
        }}
        disabled={disabled}
        className="rounded-md border border-edge bg-surface px-3 py-1 text-xs font-medium text-ink-secondary transition hover:border-accent hover:text-accent disabled:cursor-not-allowed disabled:opacity-40"
      >
        {config.busy ? 'Running…' : 'Run now'}
      </button>
      {mutation.error && (
        <span className="text-xs text-critical">{(mutation.error as ApiError).message}</span>
      )}
    </div>
  );
}

export default function Dashboard() {
  const navigate = useNavigate();
  const { data: configs, isLoading, error } = useQuery({ queryKey: ['configs'], queryFn: api.configs });
  const { data: health } = useQuery({ queryKey: ['health'], queryFn: api.health });
  const [creating, setCreating] = useState(false);

  const running = configs?.filter(c => c.last_run?.status === 'running' || c.last_run?.status === 'queued') ?? [];

  if (isLoading) return <p className="text-ink-muted">Loading…</p>;
  if (error) return <p className="text-critical">Failed to load configs: {String((error as Error).message)}</p>;

  return (
    <div className="space-y-6">
      {running.length > 0 && (
        <div className="rounded-lg border border-accent/30 bg-surface px-4 py-3">
          <p className="text-sm text-ink-secondary">
            <span className="font-medium text-accent">● {running.length} config{running.length > 1 ? 's' : ''} syncing now</span>
            {' — '}
            {running.map((c, i) => (
              <span key={c.id}>
                {i > 0 && ', '}
                <Link to={`/runs/${c.last_run!.id}`} className="text-accent hover:underline">{c.name}</Link>
              </span>
            ))}
          </p>
        </div>
      )}

      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold tracking-tight">Configurations</h1>
        {health?.mode === 'rw' && (
          <button
            onClick={() => setCreating(true)}
            className="rounded-md bg-accent px-3.5 py-1.5 text-sm font-medium text-white transition hover:opacity-90"
          >
            New config
          </button>
        )}
      </div>

      {creating && (
        <div className="rounded-lg border border-edge bg-page p-4">
          <h2 className="mb-4 text-sm font-semibold">Create a new config</h2>
          <ConfigForm
            mode="create"
            onSaved={record => {
              setCreating(false);
              navigate(`/configs/${record.id}`);
            }}
            onCancel={() => setCreating(false)}
          />
        </div>
      )}

      {configs && configs.length === 0 && (
        <p className="rounded-lg border border-edge bg-surface px-4 py-8 text-center text-sm text-ink-muted">
          No configs loaded. Pass config files to <code className="text-ink-secondary">doc2vec controller</code>
          {health?.mode === 'rw' && ' or create one with the button above'}.
        </p>
      )}

      <div className="grid gap-4">
        {configs?.map(config => (
          <Link
            key={config.id}
            to={`/configs/${config.id}`}
            className="block rounded-lg border border-edge bg-surface p-4 transition hover:border-accent/50"
          >
            {/* Fixed column widths so schedule/last run/next run line up across cards */}
            <div className="flex flex-col gap-3 md:grid md:grid-cols-[minmax(9rem,1fr)_12rem_15rem_8rem_auto] md:items-start md:gap-x-6">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <h2 className="truncate font-semibold">{config.name}</h2>
                  {config.parse_error && (
                    <span className="shrink-0 rounded-full border border-critical/40 px-2 py-0.5 text-xs text-critical" title={config.parse_error}>
                      invalid
                    </span>
                  )}
                </div>
                <div className="mt-1.5">
                  <SourceBadges sources={config.source_summary} max={4} />
                </div>
              </div>
              <div className="text-sm">
                <p className="text-xs uppercase tracking-wide text-ink-muted">Schedule</p>
                <p className="mt-0.5 text-ink-secondary" title={config.schedule ?? undefined}>
                  {humanizeCron(config.schedule)}
                </p>
              </div>
              <div className="text-sm">
                <p className="text-xs uppercase tracking-wide text-ink-muted">Last run</p>
                <div className="mt-0.5 flex flex-wrap items-center gap-2">
                  {config.last_run ? (
                    <>
                      <RunStatusBadge status={config.last_run.status} />
                      <span className="text-xs text-ink-muted" title={formatTime(config.last_run.queued_at)}>
                        {relativeTime(config.last_run.finished_at ?? config.last_run.queued_at)}
                        {config.last_run.finished_at && config.last_run.started_at &&
                          ` · ${runDuration(config.last_run.started_at, config.last_run.finished_at)}`}
                      </span>
                    </>
                  ) : (
                    <span className="text-xs text-ink-muted">never</span>
                  )}
                </div>
              </div>
              <div className="text-sm">
                <p className="text-xs uppercase tracking-wide text-ink-muted">Next run</p>
                <p className="mt-0.5 text-ink-secondary" title={config.next_run ? formatTime(config.next_run) : undefined}>
                  {config.next_run ? relativeTime(config.next_run) : '—'}
                </p>
              </div>
              <div className="md:self-center">
                <RunNowButton config={config} />
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
