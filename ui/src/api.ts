// API client + shared types (mirrors controller/types.ts)

export interface SourceSummary {
  type: string;
  product_name: string;
  version?: string;
}

export type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'skipped' | 'canceled';

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
    sources?: Array<{
      product_name: string;
      type: string;
      version: string;
      duration_ms: number;
      ok: boolean;
      error?: string;
    }>;
    warn_count?: number;
    error_count?: number;
  };
  queued_at: string;
  started_at: string | null;
  finished_at: string | null;
  config_name?: string | null;
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
  runs: (params: { configId?: number; status?: string; limit?: number; before?: number } = {}) => {
    const qs = new URLSearchParams();
    if (params.configId !== undefined) qs.set('configId', String(params.configId));
    if (params.status !== undefined) qs.set('status', params.status);
    if (params.limit !== undefined) qs.set('limit', String(params.limit));
    if (params.before !== undefined) qs.set('before', String(params.before));
    return request<RunRecord[]>(`/api/runs?${qs}`);
  },
  run: (id: number) => request<RunRecord>(`/api/runs/${id}`),
  runLogs: (id: number, afterSeq = 0, limit = 1000) =>
    request<LogRow[]>(`/api/runs/${id}/logs?afterSeq=${afterSeq}&limit=${limit}`),
  cancelRun: (id: number) => request<RunRecord>(`/api/runs/${id}/cancel`, { method: 'POST' }),
};
