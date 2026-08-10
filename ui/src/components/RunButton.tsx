import { useMemo, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, ApiError, ConfigRecord } from '../api';

/**
 * Split "Run now" button: the main button syncs every source, the chevron opens
 * a picker to run a subset of the config's source entries. Selection is by
 * position in the config's sources list, so entries sharing a product name
 * (e.g. istio as github + code + website) are selectable individually.
 */
export default function RunButton({ config, variant = 'subtle' }: {
  config: ConfigRecord;
  variant?: 'primary' | 'subtle';
}) {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [filter, setFilter] = useState('');

  const mutation = useMutation({
    mutationFn: (sources?: number[]) => api.triggerRun(config.id, sources, config.content_hash),
    onSuccess: () => {
      setOpen(false);
      setSelected(new Set());
      setFilter('');
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['configs'] }),
  });

  const entries = useMemo(
    () => config.source_summary.map((source, index) => ({ ...source, index })),
    [config.source_summary]
  );
  const visible = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) return entries;
    return entries.filter(e =>
      e.product_name.toLowerCase().includes(needle) || e.type.toLowerCase().includes(needle)
    );
  }, [entries, filter]);

  const disabled = config.busy || !!config.parse_error || mutation.isPending;

  const stop = (e: React.SyntheticEvent) => {
    // The dashboard renders this inside a <Link> card — keep clicks local
    e.preventDefault();
    e.stopPropagation();
  };

  const toggle = (index: number) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  };

  const allVisibleSelected = visible.length > 0 && visible.every(e => selected.has(e.index));
  const toggleAllVisible = () => {
    setSelected(prev => {
      const next = new Set(prev);
      if (allVisibleSelected) visible.forEach(e => next.delete(e.index));
      else visible.forEach(e => next.add(e.index));
      return next;
    });
  };

  const mainClass = variant === 'primary'
    ? 'bg-accent text-white transition hover:opacity-90 px-3.5 py-1.5 text-sm font-medium'
    : 'border border-edge bg-surface text-ink-secondary transition hover:border-accent hover:text-accent px-3 py-1 text-xs font-medium';
  const chevronClass = variant === 'primary'
    ? 'bg-accent text-white transition hover:opacity-90 border-l border-white/25 px-2 py-1.5 text-sm'
    : 'border border-l-0 border-edge bg-surface text-ink-secondary transition hover:border-accent hover:text-accent px-1.5 py-1 text-xs';

  return (
    <div className="relative inline-flex items-center gap-2" onClick={stop}>
      <span className="inline-flex">
        <button
          onClick={e => {
            stop(e);
            mutation.mutate(undefined);
          }}
          disabled={disabled}
          className={`${entries.length > 1 ? 'rounded-l-md' : 'rounded-md'} ${mainClass} disabled:cursor-not-allowed disabled:opacity-40`}
        >
          {config.busy ? 'Running…' : 'Run now'}
        </button>
        {entries.length > 1 && (
          <button
            onClick={e => {
              stop(e);
              setOpen(o => !o);
            }}
            disabled={disabled}
            aria-label="Choose sources to run"
            title="Choose sources to run"
            className={`rounded-r-md ${chevronClass} disabled:cursor-not-allowed disabled:opacity-40`}
          >
            ▾
          </button>
        )}
      </span>

      {mutation.error && (
        <span className="text-xs text-critical">{(mutation.error as ApiError).message}</span>
      )}

      {open && (
        <>
          <div className="fixed inset-0 z-10" onClick={e => { stop(e); setOpen(false); }} />
          <div className="absolute right-0 top-full z-20 mt-1.5 w-80 rounded-md border border-edge bg-surface shadow-lg">
            <div className="flex items-center justify-between border-b border-edge px-3 py-2">
              <span className="text-xs font-semibold uppercase tracking-wide text-ink-muted">
                Run sources{selected.size > 0 && ` (${selected.size}/${entries.length})`}
              </span>
              <button
                onClick={e => {
                  stop(e);
                  toggleAllVisible();
                }}
                className="text-xs text-accent hover:underline"
              >
                {allVisibleSelected ? 'Clear' : 'Select all'}{filter.trim() && ' shown'}
              </button>
            </div>
            {entries.length > 8 && (
              <div className="border-b border-edge px-3 py-2">
                <input
                  type="text"
                  value={filter}
                  onChange={e => setFilter(e.target.value)}
                  onClick={e => e.stopPropagation()}
                  placeholder="Filter sources…"
                  className="w-full rounded-md border border-edge bg-page px-2 py-1 text-xs text-ink-secondary placeholder:text-ink-muted focus:border-accent focus:outline-none"
                />
              </div>
            )}
            <ul className="max-h-64 overflow-y-auto py-1">
              {visible.map(entry => (
                <li key={entry.index}>
                  <label
                    className="flex cursor-pointer items-center gap-2 px-3 py-1.5 text-sm text-ink-secondary hover:bg-edge/20"
                    onClick={e => e.stopPropagation()}
                  >
                    <input
                      type="checkbox"
                      checked={selected.has(entry.index)}
                      onChange={() => toggle(entry.index)}
                      className="accent-accent"
                    />
                    <span className="truncate">{entry.product_name}</span>
                    <span className="ml-auto shrink-0 text-xs text-ink-muted">
                      {entry.type}{entry.version ? ` · ${entry.version}` : ''}
                    </span>
                  </label>
                </li>
              ))}
              {visible.length === 0 && (
                <li className="px-3 py-2 text-xs text-ink-muted">No sources match “{filter}”.</li>
              )}
            </ul>
            <div className="border-t border-edge px-3 py-2">
              <button
                onClick={e => {
                  stop(e);
                  mutation.mutate([...selected]);
                }}
                disabled={selected.size === 0 || disabled}
                className="w-full rounded-md bg-accent px-3 py-1.5 text-xs font-medium text-white transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Run {selected.size > 0 ? `${selected.size} selected` : 'selected'} source{selected.size === 1 ? '' : 's'}
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
