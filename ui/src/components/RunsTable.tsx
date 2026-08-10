import { Link } from 'react-router-dom';
import { RunRecord } from '../api';
import { formatTime, runDuration } from '../lib/format';
import RunStatusBadge from './RunStatusBadge';

export default function RunsTable({ runs, showConfig = false, configNames }: {
  runs: RunRecord[];
  showConfig?: boolean;
  configNames?: Map<number, string>;
}) {
  if (runs.length === 0) {
    return <p className="py-8 text-center text-sm text-ink-muted">No runs yet.</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-edge text-left text-xs uppercase tracking-wide text-ink-muted">
            <th className="py-2 pr-4 font-medium">Run</th>
            {showConfig && <th className="py-2 pr-4 font-medium">Config</th>}
            <th className="py-2 pr-4 font-medium">Status</th>
            <th className="py-2 pr-4 font-medium">Trigger</th>
            <th className="py-2 pr-4 font-medium">Started</th>
            <th className="py-2 pr-4 font-medium">Duration</th>
            <th className="py-2 font-medium">Detail</th>
          </tr>
        </thead>
        <tbody>
          {runs.map(run => (
            <tr key={run.id} className="border-b border-edge/60 hover:bg-ink/[0.03]">
              <td className="py-2.5 pr-4">
                <Link to={`/runs/${run.id}`} className="font-medium text-accent hover:underline">
                  #{run.id}
                </Link>
              </td>
              {showConfig && (
                <td className="py-2.5 pr-4">
                  <Link to={`/configs/${run.config_id}`} className="hover:underline">
                    {configNames?.get(run.config_id) ?? run.config_name ?? `config ${run.config_id}`}
                  </Link>
                </td>
              )}
              <td className="py-2.5 pr-4"><RunStatusBadge status={run.status} /></td>
              <td className="py-2.5 pr-4 text-ink-secondary">
                {run.trigger}
                {run.requested_sources && run.requested_sources.length > 0 && (
                  <span
                    className="ml-1.5 rounded-full border border-edge px-1.5 py-0.5 text-xs text-ink-muted"
                    title={`Selected sources: ${run.requested_sources.map(s => `${s.product_name} (${s.type})`).join(', ')}`}
                  >
                    {run.requested_sources.length} source{run.requested_sources.length > 1 ? 's' : ''}
                  </span>
                )}
              </td>
              <td className="py-2.5 pr-4 text-ink-secondary">{formatTime(run.started_at ?? run.queued_at)}</td>
              <td className="py-2.5 pr-4 text-ink-secondary">
                {run.status === 'running' || run.status === 'queued' ? '…' : runDuration(run.started_at, run.finished_at)}
              </td>
              <td className="max-w-md truncate py-2.5 text-ink-muted" title={run.error ?? undefined}>
                {run.error ?? (run.stats?.sources ? `${run.stats.sources.length} source(s)` : '')}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
