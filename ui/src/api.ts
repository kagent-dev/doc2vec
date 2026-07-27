// API client + shared types (mirrors controller/types.ts)

export interface SourceSummary {
  type: string;
  product_name: string;
  version?: string;
}

export type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'skipped' | 'canceled';

export interface SourceRunCounters {
  items_kind: string;
  items_new: number;
  items_updated: number;
  items_unchanged: number;
  items_deleted: number;
  chunks_added: number;
  chunks_deleted: number;
}

export interface SourceRunStats {
  product_name: string;
  type: string;
  version: string;
  duration_ms: number;
  ok: boolean;
  error?: string;
  counters?: SourceRunCounters;
}

export interface RunRecord {
  id: number;
  config_id: number;
  config_hash: string;
  trigger: 'scheduled' | 'manual';
  status: RunStatus;
  pid: number | null;
  exit_code: number | null;
  error: string | null;
  stats: {
    sources?: SourceRunStats[];
    warn_count?: number;
    error_count?: number;
  };
  queued_at: string;
  started_at: string | null;
  finished_at: string | null;
  config_name?: string | null;
}

export interface ChunkRecord {
  chunk_id: string;
  url: string;
  product_name: string | null;
  version: string | null;
  section: string | null;
  heading_hierarchy: string[];
  chunk_index: number | null;
  total_chunks: number | null;
  hash: string | null;
  created_at: string | null;
  content: string;
}

export interface ChunkLookupResult {
  source: { product_name: string; type: string; version: string };
  database: { type: 'sqlite'; path: string } | { type: 'qdrant'; url: string; collection: string };
  chunks: ChunkRecord[];
}

export interface ConfigRecord {
  id: number;
  path: string;
  name: string;
  content: string;
  content_hash: string;
  schedule: string | null;
  source_summary: SourceSummary[];
  parse_error: string | null;
  enabled: boolean;
  next_run: string | null;
  last_run: RunRecord | null;
  busy: boolean;
}

export interface LogRow {
  seq: number;
  ts: string;
  level: string;
  module: string | null;
  message: string;
}

export interface ConfigStats {
  daily: Array<{ day: string; status: string; count: number; avg_duration_s: number | null }>;
  recentDurations: Array<{ id: number; status: string; started_at: string; duration_s: number }>;
  totals: { total: number; succeeded: number; failed: number; skipped: number; canceled: number };
}

export interface Health {
  status: string;
  mode: 'ro' | 'rw';
  version: string;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  });
  if (!res.ok) {
    let message = res.statusText;
    try {
      const body = await res.json();
      if (body?.error) message = body.error;
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(res.status, message);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

export const api = {
  health: () => request<Health>('/api/health'),
  configs: () => request<ConfigRecord[]>('/api/configs'),
  config: (id: number) => request<ConfigRecord>(`/api/configs/${id}`),
  createConfig: (filename: string, content: string) =>
    request<ConfigRecord>('/api/configs', { method: 'POST', body: JSON.stringify({ filename, content }) }),
  updateConfig: (id: number, content: string, baseHash: string) =>
    request<ConfigRecord>(`/api/configs/${id}`, { method: 'PUT', body: JSON.stringify({ content, baseHash }) }),
  deleteConfig: (id: number) => request<void>(`/api/configs/${id}`, { method: 'DELETE' }),
  validateConfig: (content: string) =>
    request<{ valid: boolean; error: string | null; name: string; schedule: string | null; sources: SourceSummary[] }>(
      '/api/configs/validate',
      { method: 'POST', body: JSON.stringify({ content }) }
    ),
  triggerRun: (configId: number) => request<RunRecord>(`/api/configs/${configId}/run`, { method: 'POST' }),
  configStats: (configId: number, days: number) =>
    request<ConfigStats>(`/api/configs/${configId}/stats?days=${days}`),
  chunksForUrl: (configId: number, params: { product_name: string; type?: string; version?: string; url: string }) => {
    const qs = new URLSearchParams();
    qs.set('product_name', params.product_name);
    if (params.type) qs.set('type', params.type);
    if (params.version) qs.set('version', params.version);
    qs.set('url', params.url);
    return request<ChunkLookupResult>(`/api/configs/${configId}/chunks?${qs}`);
  },
  runs: (params: { configId?: number; status?: string; limit?: number; before?: number } = {}) => {
    const qs = new URLSearchParams();
    if (params.configId !== undefined) qs.set('configId', String(params.configId));
    if (params.status !== undefined) qs.set('status', params.status);
    if (params.limit !== undefined) qs.set('limit', String(params.limit));
    if (params.before !== undefined) qs.set('before', String(params.before));
    return request<RunRecord[]>(`/api/runs?${qs}`);
  },
  run: (id: number) => request<RunRecord>(`/api/runs/${id}`),
  runLogs: (id: number, params: { afterSeq?: number; limit?: number; levels?: string[]; q?: string } = {}) =>
    request<LogRow[]>(`/api/runs/${id}/logs?${logQuery(params)}`),
  runLogCounts: (id: number) => request<Record<string, number>>(`/api/runs/${id}/logs/counts`),
  runLogsDownloadUrl: (id: number, params: { levels?: string[]; q?: string } = {}) => {
    const qs = logQuery(params);
    return `/api/runs/${id}/logs/download${qs ? `?${qs}` : ''}`;
  },
  cancelRun: (id: number) => request<RunRecord>(`/api/runs/${id}/cancel`, { method: 'POST' }),
};

function logQuery(params: { afterSeq?: number; limit?: number; levels?: string[]; q?: string }): string {
  const qs = new URLSearchParams();
  if (params.afterSeq !== undefined) qs.set('afterSeq', String(params.afterSeq));
  if (params.limit !== undefined) qs.set('limit', String(params.limit));
  if (params.levels?.length) qs.set('levels', params.levels.join(','));
  if (params.q) qs.set('q', params.q);
  return qs.toString();
}
