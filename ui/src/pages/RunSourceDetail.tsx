import { FormEvent, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useParams } from 'react-router-dom';
import { api, ApiError, ChunkLookupResult, ChunkRecord, SourceRunCounters } from '../api';
import { formatDuration, formatTime } from '../lib/format';
import RunStatusBadge from '../components/RunStatusBadge';

// Singularize the item kind for per-source stat labels ("New pages" → kind "pages")
function statCards(counters: SourceRunCounters): Array<{ label: string; value: number; tone?: 'good' | 'bad' }> {
  const kind = counters.items_kind || 'items';
  return [
    { label: `New ${kind}`, value: counters.items_new, tone: 'good' },
    { label: `Updated ${kind}`, value: counters.items_updated },
    { label: `Unchanged ${kind}`, value: counters.items_unchanged },
    { label: `Deleted ${kind}`, value: counters.items_deleted, tone: 'bad' },
    { label: 'Chunks added', value: counters.chunks_added, tone: 'good' },
    { label: 'Chunks deleted', value: counters.chunks_deleted, tone: 'bad' },
  ];
}

type SortKey = 'chunk_id' | 'position' | 'section' | 'size';

// Default directions: sizes largest-first, the rest ascending
const DEFAULT_DIR: Record<SortKey, 'asc' | 'desc'> = {
  chunk_id: 'asc',
  position: 'asc',
  section: 'asc',
  size: 'desc',
};

function compareChunks(a: ChunkRecord, b: ChunkRecord, key: SortKey, dir: 'asc' | 'desc'): number {
  const sign = dir === 'asc' ? 1 : -1;
  switch (key) {
    case 'chunk_id':
      return a.chunk_id.localeCompare(b.chunk_id) * sign;
    case 'position':
      return ((a.chunk_index ?? Number.MAX_SAFE_INTEGER) - (b.chunk_index ?? Number.MAX_SAFE_INTEGER)) * sign;
    case 'section':
      return (a.section ?? '').localeCompare(b.section ?? '') * sign;
    case 'size':
      return (a.content.length - b.content.length) * sign;
  }
}

function SortableHeader({
  label,
  sortKey,
  sort,
  onSort,
  align = 'left',
}: {
  label: string;
  sortKey: SortKey;
  sort: { key: SortKey; dir: 'asc' | 'desc' };
  onSort: (key: SortKey) => void;
  align?: 'left' | 'right';
}) {
  const active = sort.key === sortKey;
  return (
    <th className={`px-4 py-2 font-medium ${align === 'right' ? 'text-right' : 'text-left'}`}>
      <button
        type="button"
        onClick={() => onSort(sortKey)}
        className={`inline-flex items-center gap-1 uppercase tracking-wide transition hover:text-accent ${
          active ? 'text-accent' : ''
        }`}
        title={`Sort by ${label.toLowerCase()}`}
      >
        {label}
        <span className={active ? '' : 'opacity-30'}>{active ? (sort.dir === 'asc' ? '▲' : '▼') : '↕'}</span>
      </button>
    </th>
  );
}

function ChunkRow({ chunk }: { chunk: ChunkRecord }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <tr
        onClick={() => setOpen(o => !o)}
        className="cursor-pointer border-b border-edge/60 transition last:border-0 hover:bg-edge/20"
      >
        <td className="px-4 py-2 font-mono text-xs text-ink-secondary" title={chunk.chunk_id}>
          {chunk.chunk_id.slice(0, 10)}…
        </td>
        <td className="px-4 py-2 text-ink-secondary">
          {chunk.chunk_index !== null && chunk.total_chunks !== null
            ? `${chunk.chunk_index + 1} / ${chunk.total_chunks}`
            : '—'}
        </td>
        <td className="max-w-xs truncate px-4 py-2 text-ink-secondary" title={chunk.section ?? undefined}>
          {chunk.section || '—'}
        </td>
        <td className="px-4 py-2 text-right text-ink-muted">{chunk.content.length.toLocaleString()} chars</td>
        <td className="px-4 py-2 text-ink-muted">{open ? '▾' : '▸'}</td>
      </tr>
      {open && (
        <tr className="border-b border-edge/60 last:border-0">
          <td colSpan={5} className="bg-edge/10 px-4 py-3">
            <dl className="mb-3 flex flex-wrap gap-x-8 gap-y-1 text-xs">
              <div>
                <dt className="uppercase tracking-wide text-ink-muted">Chunk ID</dt>
                <dd className="mt-0.5 font-mono text-ink-secondary">{chunk.chunk_id}</dd>
              </div>
              {chunk.hash && (
                <div>
                  <dt className="uppercase tracking-wide text-ink-muted">Content hash</dt>
                  <dd className="mt-0.5 font-mono text-ink-secondary">{chunk.hash}</dd>
                </div>
              )}
              {chunk.created_at && (
                <div>
                  <dt className="uppercase tracking-wide text-ink-muted">Created / Updated</dt>
                  <dd className="mt-0.5 text-ink-secondary">{formatTime(chunk.created_at)}</dd>
                </div>
              )}
              {chunk.heading_hierarchy.length > 0 && (
                <div>
                  <dt className="uppercase tracking-wide text-ink-muted">Headings</dt>
                  <dd className="mt-0.5 text-ink-secondary">{chunk.heading_hierarchy.join(' › ')}</dd>
                </div>
              )}
              {chunk.product_name && (
                <div>
                  <dt className="uppercase tracking-wide text-ink-muted">Product</dt>
                  <dd className="mt-0.5 text-ink-secondary">
                    {chunk.product_name}
                    {chunk.version ? ` @ ${chunk.version}` : ''}
                  </dd>
                </div>
              )}
            </dl>
            <pre className="max-h-96 overflow-auto whitespace-pre-wrap rounded-md border border-edge bg-surface p-3 text-xs text-ink-secondary">
              {chunk.content}
            </pre>
          </td>
        </tr>
      )}
    </>
  );
}

export default function RunSourceDetail() {
  const { id, sourceIndex } = useParams();
  const runId = Number(id);
  const idx = Number(sourceIndex);

  const { data: run, error } = useQuery({
    queryKey: ['runs', runId],
    queryFn: () => api.run(runId),
  });

  const [url, setUrl] = useState('');
  const [lookupUrl, setLookupUrl] = useState<string | null>(null);
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>({ key: 'position', dir: 'asc' });

  const onSort = (key: SortKey) =>
    setSort(prev => (prev.key === key ? { key, dir: prev.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: DEFAULT_DIR[key] }));

  const source = run?.stats?.sources?.[idx];

  const chunkQuery = useQuery<ChunkLookupResult, ApiError>({
    queryKey: ['chunks', run?.config_id, source?.product_name, source?.version, lookupUrl],
    queryFn: () =>
      api.chunksForUrl(run!.config_id, {
        product_name: source!.product_name,
        type: source!.type,
        version: source!.version,
        url: lookupUrl!,
      }),
    enabled: run !== undefined && source !== undefined && lookupUrl !== null,
    retry: false,
  });

  const sortedChunks = useMemo(
    () => (chunkQuery.data ? [...chunkQuery.data.chunks].sort((a, b) => compareChunks(a, b, sort.key, sort.dir)) : []),
    [chunkQuery.data, sort]
  );
  // A changed URL has all its chunks recreated together, so they share one
  // date — surface the newest as the URL's created/updated date.
  const urlDate = useMemo(() => {
    const dates = (chunkQuery.data?.chunks ?? []).map(c => c.created_at).filter((d): d is string => !!d);
    return dates.length > 0 ? dates.reduce((a, b) => (a > b ? a : b)) : null;
  }, [chunkQuery.data]);

  if (error) return <p className="text-critical">{(error as ApiError).message}</p>;
  if (!run) return <p className="text-ink-muted">Loading…</p>;
  if (!source) {
    return (
      <div className="space-y-2">
        <Link to={`/runs/${runId}`} className="text-sm text-ink-muted hover:text-accent">
          ← Run #{runId}
        </Link>
        <p className="text-ink-muted">No per-source stats were recorded for this source in this run.</p>
      </div>
    );
  }

  const counters = source.counters;

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    const trimmed = url.trim();
    if (trimmed) setLookupUrl(trimmed);
  };

  return (
    <div className="space-y-6">
      <div>
        <Link to={`/runs/${runId}`} className="text-sm text-ink-muted hover:text-accent">
          ← Run #{runId}
        </Link>
        <div className="mt-2 flex flex-wrap items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight">{source.product_name}</h1>
          <RunStatusBadge status={run.status} />
          {source.ok ? (
            <span className="text-sm text-good-text">✓ ok</span>
          ) : (
            <span className="text-sm text-critical">✕ {source.error ?? 'failed'}</span>
          )}
        </div>
        <dl className="mt-3 flex flex-wrap gap-x-8 gap-y-2 text-sm">
          {[
            ['Type', source.type],
            ['Version', source.version],
            ['Duration', formatDuration(source.duration_ms / 1000)],
            ['Run started', formatTime(run.started_at)],
          ].map(([label, value]) => (
            <div key={label}>
              <dt className="text-xs uppercase tracking-wide text-ink-muted">{label}</dt>
              <dd className="mt-0.5 text-ink-secondary">{value}</dd>
            </div>
          ))}
        </dl>
      </div>

      <div>
        <h2 className="mb-2 text-sm font-semibold text-ink-secondary">Changes in this run</h2>
        {counters ? (
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
            {statCards(counters).map(card => (
              <div key={card.label} className="rounded-lg border border-edge bg-surface px-4 py-3">
                <div
                  className={`text-2xl font-semibold tabular-nums ${
                    card.value === 0
                      ? 'text-ink-muted'
                      : card.tone === 'good'
                        ? 'text-good-text'
                        : card.tone === 'bad'
                          ? 'text-critical'
                          : ''
                  }`}
                >
                  {card.value.toLocaleString()}
                </div>
                <div className="mt-1 text-xs uppercase tracking-wide text-ink-muted">{card.label}</div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-ink-muted">
            This run predates detailed source statistics — trigger a new run to collect them.
          </p>
        )}
      </div>

      <div>
        <h2 className="mb-2 text-sm font-semibold text-ink-secondary">Chunk lookup</h2>
        <p className="mb-2 text-xs text-ink-muted">
          Shows the chunks currently stored in this source's vector database for a URL (live view, ordered by
          position — click a column heading to sort).
        </p>
        <form onSubmit={onSubmit} className="flex gap-2">
          <input
            type="text"
            value={url}
            onChange={e => setUrl(e.target.value)}
            placeholder="https://…"
            className="w-full max-w-2xl rounded-md border border-edge bg-surface px-3 py-1.5 text-sm outline-none focus:border-accent"
          />
          <button
            type="submit"
            disabled={!url.trim() || chunkQuery.isFetching}
            className="rounded-md border border-edge px-3.5 py-1.5 text-sm font-medium transition hover:border-accent hover:text-accent disabled:opacity-40"
          >
            {chunkQuery.isFetching ? 'Looking up…' : 'Look up'}
          </button>
        </form>

        {chunkQuery.error && <p className="mt-3 text-sm text-critical">{chunkQuery.error.message}</p>}

        {chunkQuery.data && (
          <div className="mt-4 space-y-3">
            <dl className="flex flex-wrap gap-x-8 gap-y-2 text-sm">
              <div>
                <dt className="text-xs uppercase tracking-wide text-ink-muted">Chunks</dt>
                <dd className="mt-0.5 text-ink-secondary">{chunkQuery.data.chunks.length}</dd>
              </div>
              {chunkQuery.data.chunks.length > 0 && (
                <div>
                  <dt className="text-xs uppercase tracking-wide text-ink-muted">Created / Updated</dt>
                  <dd
                    className="mt-0.5 text-ink-secondary"
                    title={urlDate ?? 'No date recorded: these chunks were stored before dates were tracked. A date is set when the page content changes and its chunks are recreated.'}
                  >
                    {urlDate ? formatTime(urlDate) : 'unknown (synced before dates were recorded)'}
                  </dd>
                </div>
              )}
              <div>
                <dt className="text-xs uppercase tracking-wide text-ink-muted">Store</dt>
                <dd className="mt-0.5 text-ink-secondary">
                  {chunkQuery.data.database.type === 'qdrant'
                    ? `Qdrant collection '${chunkQuery.data.database.collection}'`
                    : `SQLite ${chunkQuery.data.database.path}`}
                </dd>
              </div>
            </dl>
            {chunkQuery.data.chunks.length > 0 && (
              <div className="overflow-x-auto rounded-lg border border-edge bg-surface">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-edge text-xs text-ink-muted">
                      <SortableHeader label="Chunk" sortKey="chunk_id" sort={sort} onSort={onSort} />
                      <SortableHeader label="Position" sortKey="position" sort={sort} onSort={onSort} />
                      <SortableHeader label="Section" sortKey="section" sort={sort} onSort={onSort} />
                      <SortableHeader label="Size" sortKey="size" sort={sort} onSort={onSort} align="right" />
                      <th className="px-4 py-2" />
                    </tr>
                  </thead>
                  <tbody>
                    {sortedChunks.map(chunk => (
                      <ChunkRow key={chunk.chunk_id} chunk={chunk} />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
