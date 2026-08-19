import { RunStatus } from '../api';

// Status colors are paired with a text label + symbol — never color alone
const STYLES: Record<RunStatus, { label: string; symbol: string; className: string }> = {
  succeeded: { label: 'Succeeded', symbol: '✓', className: 'text-good-text border-good/40' },
  failed: { label: 'Failed', symbol: '✕', className: 'text-critical border-critical/40' },
  running: { label: 'Running', symbol: '●', className: 'text-accent border-accent/40 animate-pulse' },
  queued: { label: 'Queued', symbol: '…', className: 'text-ink-secondary border-edge' },
  skipped: { label: 'Skipped', symbol: '≫', className: 'text-warning border-warning/40' },
  canceled: { label: 'Canceled', symbol: '⊘', className: 'text-ink-muted border-edge' },
};

export default function RunStatusBadge({ status }: { status: RunStatus }) {
  const style = STYLES[status] ?? STYLES.queued;
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border bg-surface px-2.5 py-0.5 text-xs font-medium ${style.className}`}
    >
      <span aria-hidden>{style.symbol}</span>
      {style.label}
    </span>
  );
}
