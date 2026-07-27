import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { api, ApiError, LogRow } from '../api';

// The live tail the browser keeps in memory. Anything older stays in Postgres
// and is reached through the (server-side) filters or the download link.
const MAX_CLIENT_ROWS = 10_000;
const SEARCH_PAGE_SIZE = 2_000;
const COUNTS_POLL_MS = 10_000;
const LEVELS = ['error', 'warn', 'info', 'debug'] as const;

const LEVEL_CLASS: Record<string, string> = {
  error: 'text-critical',
  warn: 'text-warning',
  info: 'text-ink-secondary',
  debug: 'text-ink-muted',
};

export default function LogViewer({ runId, isActive }: { runId: number; isActive: boolean }) {
  const [rows, setRows] = useState<LogRow[]>([]);
  const [ended, setEnded] = useState(!isActive);
  const [follow, setFollow] = useState(true);
  const [levelFilter, setLevelFilter] = useState<Set<string>>(new Set());
  const [keyword, setKeyword] = useState('');
  const [debouncedKeyword, setDebouncedKeyword] = useState('');
  const [totals, setTotals] = useState<Record<string, number> | null>(null);
  const [search, setSearch] = useState<{
    rows: LogRow[];
    loading: boolean;
    hasMore: boolean;
    error: string | null;
  }>({ rows: [], loading: false, hasMore: false, error: null });
  const scrollRef = useRef<HTMLDivElement>(null);
  const followRef = useRef(follow);
  followRef.current = follow;

  const levels = useMemo(() => [...levelFilter].sort(), [levelFilter]);
  const levelsKey = levels.join(',');
  const filtered = levels.length > 0 || debouncedKeyword !== '';

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedKeyword(keyword.trim()), 300);
    return () => clearTimeout(timer);
  }, [keyword]);

  // Live rows that match the active filter are appended to the search results,
  // but only once those are paged to the end — otherwise they'd jump the queue.
  const liveAppendRef = useRef<{ match: (row: LogRow) => boolean; enabled: boolean }>({
    match: () => false,
    enabled: false,
  });
  liveAppendRef.current = {
    enabled: filtered && !search.loading && !search.hasMore && !search.error,
    match: (row: LogRow) => {
      if (levels.length > 0 && !levels.includes(row.level)) return false;
      if (!debouncedKeyword) return true;
      const needle = debouncedKeyword.toLowerCase();
      return row.message.toLowerCase().includes(needle) || (row.module ?? '').toLowerCase().includes(needle);
    },
  };

  useEffect(() => {
    setRows([]);
    // Replays the trailing window from storage, then tails live output
    const source = new EventSource(`/api/runs/${runId}/logs/stream?tail=${MAX_CLIENT_ROWS}`);
    source.addEventListener('log', event => {
      const row: LogRow = JSON.parse((event as MessageEvent).data);
      setRows(prev => {
        const next = prev.length >= MAX_CLIENT_ROWS ? prev.slice(prev.length - MAX_CLIENT_ROWS + 1) : prev.slice();
        next.push(row);
        return next;
      });
      const live = liveAppendRef.current;
      if (live.enabled && live.match(row)) {
        setSearch(prev =>
          prev.rows.length > 0 && row.seq <= prev.rows[prev.rows.length - 1].seq
            ? prev
            : { ...prev, rows: [...prev.rows, row] }
        );
      }
    });
    source.addEventListener('end', () => {
      setEnded(true);
      source.close();
    });
    source.onerror = () => {
      // EventSource retries automatically while the controller is reachable
    };
    return () => source.close();
  }, [runId]);

  // Whole-run level totals: the chips must report the run, not the window
  useEffect(() => {
    let canceled = false;
    const load = () => {
      api
        .runLogCounts(runId)
        .then(counts => !canceled && setTotals(counts))
        .catch(() => undefined);
    };
    load();
    if (ended) return () => { canceled = true; };
    const timer = setInterval(load, COUNTS_POLL_MS);
    return () => {
      canceled = true;
      clearInterval(timer);
    };
  }, [runId, ended]);

  // Filtering runs server-side so it covers the whole run, not just the window
  useEffect(() => {
    if (!filtered) {
      setSearch({ rows: [], loading: false, hasMore: false, error: null });
      return;
    }
    let canceled = false;
    setSearch({ rows: [], loading: true, hasMore: false, error: null });
    api
      .runLogs(runId, { levels, q: debouncedKeyword, limit: SEARCH_PAGE_SIZE })
      .then(page => {
        if (canceled) return;
        setSearch({ rows: page, loading: false, hasMore: page.length === SEARCH_PAGE_SIZE, error: null });
      })
      .catch((err: ApiError) => {
        if (!canceled) setSearch({ rows: [], loading: false, hasMore: false, error: err.message });
      });
    return () => {
      canceled = true;
    };
    // levels is rebuilt on every render; levelsKey is its stable identity
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, levelsKey, debouncedKeyword, filtered]);

  const loadMore = useCallback(() => {
    setSearch(prev => {
      if (prev.loading || !prev.hasMore) return prev;
      const afterSeq = prev.rows.length > 0 ? prev.rows[prev.rows.length - 1].seq : 0;
      api
        .runLogs(runId, { levels, q: debouncedKeyword, afterSeq, limit: SEARCH_PAGE_SIZE })
        .then(page =>
          setSearch(cur => ({
            ...cur,
            rows: [...cur.rows, ...page],
            loading: false,
            hasMore: page.length === SEARCH_PAGE_SIZE,
          }))
        )
        .catch((err: ApiError) => setSearch(cur => ({ ...cur, loading: false, error: err.message })));
      return { ...prev, loading: true };
    });
  }, [runId, levelsKey, debouncedKeyword]);

  const windowCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const row of rows) counts[row.level] = (counts[row.level] ?? 0) + 1;
    return counts;
  }, [rows]);
  const levelCounts = totals ?? windowCounts;
  const totalLines = totals ? Object.values(totals).reduce((a, b) => a + b, 0) : rows.length;
  const trimmed = totalLines > rows.length;

  const visibleRows = filtered ? search.rows : rows;

  // Distinguishes our own scrollTop writes from user scrolling: during log
  // bursts the content grows between the programmatic scroll and its async
  // scroll event, which would otherwise measure as "not at bottom" and switch
  // follow off spontaneously.
  const programmaticScroll = useRef(false);

  useLayoutEffect(() => {
    if (followRef.current && scrollRef.current) {
      programmaticScroll.current = true;
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [visibleRows, follow]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    if (programmaticScroll.current) {
      programmaticScroll.current = false;
      // Our own scroll never disables follow; it can only confirm it
      if (atBottom && !followRef.current) setFollow(true);
      return;
    }
    if (atBottom !== followRef.current) setFollow(atBottom);
  };

  // Scrolling up is an unambiguous "stop following" signal — react to the
  // gesture itself so a heavy log burst can't re-pin the view mid-scroll
  const onWheel = (event: React.WheelEvent) => {
    if (event.deltaY < 0 && followRef.current) setFollow(false);
  };

  const toggleLevel = (level: string) => {
    setLevelFilter(prev => {
      const next = new Set(prev);
      if (next.has(level)) next.delete(level);
      else next.add(level);
      return next;
    });
  };

  const filterDirty = levelFilter.size > 0 || keyword.trim() !== '';

  return (
    <div className="rounded-lg border border-edge bg-surface">
      <div className="flex flex-wrap items-center gap-2 border-b border-edge px-4 py-2 text-xs text-ink-muted">
        <span className="shrink-0">
          {filtered
            ? `${search.rows.length.toLocaleString()}${search.hasMore ? '+' : ''} / ${totalLines.toLocaleString()} lines`
            : `${totalLines.toLocaleString()} lines${trimmed ? ` (showing last ${rows.length.toLocaleString()})` : ''}`}
        </span>
        {!ended && <span className="shrink-0 text-accent">● live</span>}
        <div className="flex items-center gap-1">
          {LEVELS.map(level => (
            <button
              key={level}
              onClick={() => toggleLevel(level)}
              title={`${levelCounts[level] ?? 0} ${level} line(s) in this run`}
              className={`rounded-full border px-2 py-0.5 font-medium uppercase transition ${
                levelFilter.has(level)
                  ? `border-current ${LEVEL_CLASS[level]}`
                  : 'border-edge text-ink-muted hover:text-ink-secondary'
              }`}
            >
              {level}
              {(levelCounts[level] ?? 0) > 0 && <span className="ml-1 font-normal">{levelCounts[level]}</span>}
            </button>
          ))}
        </div>
        <input
          value={keyword}
          onChange={e => setKeyword(e.target.value)}
          placeholder="Filter by keyword…"
          className="w-44 rounded-md border border-edge bg-page px-2 py-1 text-xs outline-none placeholder:text-ink-muted/60 focus:border-accent"
        />
        {filterDirty && (
          <button
            onClick={() => {
              setLevelFilter(new Set());
              setKeyword('');
            }}
            className="text-ink-muted underline-offset-2 hover:text-accent hover:underline"
          >
            clear
          </button>
        )}
        <a
          href={api.runLogsDownloadUrl(runId, filtered ? { levels, q: debouncedKeyword } : {})}
          className="text-ink-muted underline-offset-2 hover:text-accent hover:underline"
          title={filtered ? 'Download the matching lines' : 'Download the full log'}
        >
          ↓ {filtered ? 'download matches' : 'download log'}
        </a>
        {!follow && !ended && (
          <button
            onClick={() => {
              setFollow(true);
              scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
            }}
            className="ml-auto rounded border border-edge px-2 py-0.5 hover:border-accent hover:text-accent"
          >
            ↓ Follow
          </button>
        )}
      </div>
      <div
        ref={scrollRef}
        onScroll={onScroll}
        onWheel={onWheel}
        className="log-scroll max-h-[560px] overflow-auto px-4 py-3 font-mono text-xs leading-5"
      >
        {search.error && <p className="text-critical">Log search failed: {search.error}</p>}
        {!search.error && visibleRows.length === 0 && (
          <p className="text-ink-muted">
            {search.loading
              ? 'Searching…'
              : filtered
                ? 'No lines match the current filters.'
                : `No log output${ended ? '' : ' yet'}.`}
          </p>
        )}
        {visibleRows.map(row => (
          <div key={row.seq} className="flex gap-3 whitespace-pre-wrap break-all hover:bg-ink/[0.03]">
            <span className="shrink-0 select-none text-ink-muted">{row.ts.slice(11, 19)}</span>
            <span className={`w-10 shrink-0 select-none uppercase ${LEVEL_CLASS[row.level] ?? 'text-ink-secondary'}`}>
              {row.level}
            </span>
            <span className={LEVEL_CLASS[row.level] ?? 'text-ink-secondary'}>
              {row.module && <span className="text-ink-muted">[{row.module.replace(/^Doc2Vec:?/, '') || 'main'}] </span>}
              {row.message}
            </span>
          </div>
        ))}
        {filtered && search.hasMore && (
          <button
            onClick={loadMore}
            disabled={search.loading}
            className="mt-2 rounded border border-edge px-2 py-1 text-ink-muted hover:border-accent hover:text-accent disabled:opacity-50"
          >
            {search.loading ? 'Loading…' : `Load ${SEARCH_PAGE_SIZE.toLocaleString()} more`}
          </button>
        )}
      </div>
    </div>
  );
}
