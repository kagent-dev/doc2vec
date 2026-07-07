import { useEffect, useRef, useState } from 'react';
import { LogRow } from '../api';

const MAX_CLIENT_ROWS = 10_000;

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

  useEffect(() => {
    if (followRef.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [rows]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
    if (atBottom !== followRef.current) setFollow(atBottom);
  };

  return (
    <div className="rounded-lg border border-edge bg-surface">
      <div className="flex items-center gap-3 border-b border-edge px-4 py-2 text-xs text-ink-muted">
        <span>{rows.length.toLocaleString()} lines{rows.length >= MAX_CLIENT_ROWS && ' (oldest trimmed)'}</span>
        {!ended && <span className="text-accent">● live</span>}
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
        {rows.length === 0 && <p className="text-ink-muted">No log output{ended ? '' : ' yet'}.</p>}
        {rows.map(row => (
          <div key={row.seq} className="flex gap-3 whitespace-pre-wrap break-all hover:bg-ink/[0.03]">
            <span className="shrink-0 select-none text-ink-muted">{row.ts.slice(11, 19)}</span>
            <span className={`shrink-0 w-10 select-none uppercase ${LEVEL_CLASS[row.level] ?? 'text-ink-secondary'}`}>
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
