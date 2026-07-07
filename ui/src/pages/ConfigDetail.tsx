import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { api, ApiError } from '../api';
import { formatTime, humanizeCron, relativeTime } from '../lib/format';
import ConfigEditor from '../components/ConfigEditor';
import ConfigForm from '../components/ConfigForm';
import RunsTable from '../components/RunsTable';
import SourceBadges from '../components/SourceBadges';
import StatsCharts from '../components/StatsCharts';

type Tab = 'runs' | 'stats' | 'config';

export default function ConfigDetail() {
  const { id } = useParams();
  const configId = Number(id);
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<Tab>('runs');
  const [statsDays, setStatsDays] = useState(30);
  const [configView, setConfigView] = useState<'form' | 'yaml'>('form');

  const { data: config, error } = useQuery({
    queryKey: ['configs', configId],
    queryFn: () => api.config(configId),
  });
  const { data: health } = useQuery({ queryKey: ['health'], queryFn: api.health });
  const { data: runs } = useQuery({
    queryKey: ['runs', { configId }],
    queryFn: () => api.runs({ configId, limit: 100 }),
  });
  const { data: stats } = useQuery({
    queryKey: ['runs', 'stats', configId, statsDays],
    queryFn: () => api.configStats(configId, statsDays),
    enabled: tab === 'stats',
  });

  const trigger = useMutation({
    mutationFn: () => api.triggerRun(configId),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['configs'] }),
  });
  const remove = useMutation({
    mutationFn: () => api.deleteConfig(configId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['configs'] });
      navigate('/');
    },
  });

  if (error) return <p className="text-critical">{(error as ApiError).message}</p>;
  if (!config) return <p className="text-ink-muted">Loading…</p>;

  const readWrite = health?.mode === 'rw';

  return (
    <div className="space-y-6">
      <div>
        <Link to="/" className="text-sm text-ink-muted hover:text-accent">← Configurations</Link>
        <div className="mt-2 flex flex-wrap items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight">{config.name}</h1>
          {config.parse_error && (
            <span className="rounded-full border border-critical/40 px-2 py-0.5 text-xs text-critical" title={config.parse_error}>
              invalid: {config.parse_error}
            </span>
          )}
          <div className="ml-auto flex items-center gap-2">
            <button
              onClick={() => trigger.mutate()}
              disabled={config.busy || !!config.parse_error || trigger.isPending}
              className="rounded-md bg-accent px-3.5 py-1.5 text-sm font-medium text-white transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {config.busy ? 'Running…' : 'Run now'}
            </button>
            {readWrite && (
              <button
                onClick={() => {
                  if (window.confirm(`Delete config '${config.name}' and its file?\n\nRun history is kept.`)) {
                    remove.mutate();
                  }
                }}
                className="rounded-md border border-critical/40 px-3.5 py-1.5 text-sm font-medium text-critical transition hover:bg-critical/10"
              >
                Delete
              </button>
            )}
          </div>
        </div>
        {trigger.error && <p className="mt-2 text-sm text-critical">{(trigger.error as ApiError).message}</p>}
        {remove.error && <p className="mt-2 text-sm text-critical">{(remove.error as ApiError).message}</p>}
        <dl className="mt-3 flex flex-wrap gap-x-8 gap-y-2 text-sm">
          <div>
            <dt className="text-xs uppercase tracking-wide text-ink-muted">Schedule</dt>
            <dd className="mt-0.5 text-ink-secondary">
              {humanizeCron(config.schedule)}
              {config.schedule && <code className="ml-2 text-xs text-ink-muted">{config.schedule}</code>}
            </dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-wide text-ink-muted">Next run</dt>
            <dd className="mt-0.5 text-ink-secondary" title={config.next_run ? formatTime(config.next_run) : undefined}>
              {config.next_run ? relativeTime(config.next_run) : '—'}
            </dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-wide text-ink-muted">File</dt>
            <dd className="mt-0.5 font-mono text-xs text-ink-muted">{config.path}</dd>
          </div>
        </dl>
        <div className="mt-3">
          <SourceBadges sources={config.source_summary} />
        </div>
      </div>

      <div className="border-b border-edge">
        <nav className="flex gap-6 text-sm">
          {(['runs', 'stats', 'config'] as Tab[]).map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`-mb-px border-b-2 px-1 pb-2 font-medium capitalize transition ${
                tab === t ? 'border-accent text-accent' : 'border-transparent text-ink-muted hover:text-ink-secondary'
              }`}
            >
              {t}
            </button>
          ))}
        </nav>
      </div>

      {tab === 'runs' && <RunsTable runs={runs ?? []} />}

      {tab === 'stats' && (
        <div className="space-y-4">
          <div className="flex items-center gap-2 text-sm">
            <span className="text-ink-muted">Period:</span>
            {[7, 30, 90].map(days => (
              <button
                key={days}
                onClick={() => setStatsDays(days)}
                className={`rounded-md border px-2.5 py-1 text-xs font-medium transition ${
                  statsDays === days
                    ? 'border-accent text-accent'
                    : 'border-edge text-ink-muted hover:text-ink-secondary'
                }`}
              >
                {days}d
              </button>
            ))}
          </div>
          {stats ? <StatsCharts stats={stats} /> : <p className="text-ink-muted">Loading…</p>}
        </div>
      )}

      {tab === 'config' && (
        <div className="space-y-4">
          <div className="flex items-center gap-2 text-sm">
            {readWrite ? (
              (['form', 'yaml'] as const).map(view => (
                <button
                  key={view}
                  onClick={() => setConfigView(view)}
                  className={`rounded-md border px-2.5 py-1 text-xs font-medium uppercase transition ${
                    configView === view
                      ? 'border-accent text-accent'
                      : 'border-edge text-ink-muted hover:text-ink-secondary'
                  }`}
                >
                  {view}
                </button>
              ))
            ) : (
              <p className="text-xs text-ink-muted">
                Read-only mode — configs are managed from the files passed to the controller.
              </p>
            )}
          </div>
          {readWrite && configView === 'form' ? (
            <ConfigForm
              key={config.content_hash}
              mode="edit"
              config={config}
              onSaved={() => queryClient.invalidateQueries({ queryKey: ['configs', configId] })}
            />
          ) : readWrite ? (
            <ConfigEditor
              key={config.content_hash}
              mode="edit"
              config={config}
              onSaved={() => queryClient.invalidateQueries({ queryKey: ['configs', configId] })}
            />
          ) : (
            <ConfigEditor mode="view" config={config} />
          )}
        </div>
      )}
    </div>
  );
}
