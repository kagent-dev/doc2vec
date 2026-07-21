import cronstrue from 'cronstrue';

export function formatDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || !isFinite(seconds)) return '—';
  if (seconds < 1) return '<1s';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${Math.round(seconds % 60)}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

export function runDuration(started: string | null, finished: string | null): string {
  if (!started) return '—';
  const end = finished ? new Date(finished).getTime() : Date.now();
  return formatDuration((end - new Date(started).getTime()) / 1000);
}

export function formatTime(iso: string | null | undefined): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function relativeTime(iso: string | null | undefined): string {
  if (!iso) return '—';
  const deltaMs = new Date(iso).getTime() - Date.now();
  const abs = Math.abs(deltaMs);
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' });
  if (abs < 60_000) return rtf.format(Math.round(deltaMs / 1000), 'second');
  if (abs < 3_600_000) return rtf.format(Math.round(deltaMs / 60_000), 'minute');
  if (abs < 86_400_000) return rtf.format(Math.round(deltaMs / 3_600_000), 'hour');
  return rtf.format(Math.round(deltaMs / 86_400_000), 'day');
}

export function humanizeCron(expr: string | null): string {
  if (!expr) return 'Manual only';
  try {
    return cronstrue.toString(expr, { verbose: false });
  } catch {
    return expr;
  }
}
