import { useEffect, useMemo, useRef, useState } from 'react';
import { LogRow } from '../api';

const MAX_CLIENT_ROWS = 10_000;
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
  const scrollRef = useRef<HTMLDivElement>(null);
  const followRef = useRef(follow);
  followRef.current = follow;

  useEffect(() => {
    setRows([]);
    // The stream endpoint replays stored logs from seq 0, then tails live output
    const source = new EventSource(`/api/runs/${runId}/logs/stream`);
    source.addEventListener('log', event => {
      const row: LogRow = JSON.parse((event as MessageEvent).data);
      setRows(prev => {
        const next = prev.length >= MAX_CLIENT_ROWS ? prev.slice(prev.length - MAX_CLIENT_ROWS + 1) : prev.slice();
        next.push(row);
        return next;
      });
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

  const levelCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const row of rows) counts[row.level] = (counts[row.level] ?? 0) + 1;
    return counts;
  }, [rows]);

  const visibleRows = useMemo(() => {
    const needle = keyword.trim().toLowerCase();
    if (levelFilter.size === 0 && !needle) return rows;
    return rows.filter(row => {
      if (levelFilter.size > 0 && !levelFilter.has(row.level)) return false;
      if (needle) {
        return (
          row.message.toLowerCase().includes(needle) ||
          (row.module ?? '').toLowerCase().includes(needle)
        );
      }
      return true;
    });
  }, [rows, levelFilter, keyword]);

  useEffect(() => {
    if (followRef.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [visibleRows]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    if (atBottom !== followRef.current) setFollow(atBottom);
  };

  const toggleLevel = (level: string) => {
    setLevelFilter(prev => {
      const next = new Set(prev);
      if (next.has(level)) next.delete(level);
      else next.add(level);
      return next;
    });
  };

  const filtered = levelFilter.size > 0 || keyword.trim() !== '';

  return (
    <div className="rounded-lg border border-edge bg-surface">
      <div className="flex flex-wrap items-center gap-2 border-b border-edge px-4 py-2 text-xs text-ink-muted">
        <span className="shrink-0">
          {filtered ? `${visibleRows.length.toLocaleString()} / ` : ''}
          {rows.length.toLocaleString()} lines{rows.length >= MAX_CLIENT_ROWS && ' (oldest trimmed)'}
        </span>
        {!ended && <span className="shrink-0 text-accent">● live</span>}
        <div className="flex items-center gap-1">
          {LEVELS.map(level => (
            <button
              key={level}
              onClick={() => toggleLevel(level)}
              title={`${levelCounts[level] ?? 0} ${level} line(s)`}
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
        {filtered && (
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
        className="log-scroll max-h-[560px] overflow-auto px-4 py-3 font-mono text-xs leading-5"
      >
        {visibleRows.length === 0 && (
          <p className="text-ink-muted">
            {rows.length === 0 ? `No log output${ended ? '' : ' yet'}.` : 'No lines match the current filters.'}
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
      </div>
    </div>
  );
}
