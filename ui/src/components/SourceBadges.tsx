import { useState } from 'react';
import { SourceSummary } from '../api';

const TYPE_ICONS: Record<string, string> = {
  website: '🌐',
  github: '🐙',
  zendesk: '🎫',
  local_directory: '📁',
  code: '⌨️',
  s3: '🪣',
};

export default function SourceBadges({ sources, max }: { sources: SourceSummary[]; max?: number }) {
  const [expanded, setExpanded] = useState(false);
  if (sources.length === 0) return <span className="text-xs text-ink-muted">no sources</span>;

  const limit = expanded || max === undefined ? sources.length : max;
  const shown = sources.slice(0, limit);
  const hidden = sources.length - shown.length;

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {shown.map((source, i) => (
        <span
          key={`${source.product_name}-${source.type}-${i}`}
          title={`${source.type}${source.version ? ` · ${source.version}` : ''}`}
          className="inline-flex items-center gap-1 rounded-full border border-edge bg-page px-2 py-0.5 text-xs text-ink-secondary"
        >
          <span aria-hidden className="text-[10px]">{TYPE_ICONS[source.type] ?? '•'}</span>
          {source.product_name}
          <span className="text-ink-muted">{source.type}</span>
        </span>
      ))}
      {hidden > 0 && (
        <button
          onClick={e => {
            e.preventDefault();
            setExpanded(true);
          }}
          className="rounded-full border border-edge px-2 py-0.5 text-xs text-ink-muted transition hover:border-accent hover:text-accent"
        >
          +{hidden} more
        </button>
      )}
    </div>
  );
}
