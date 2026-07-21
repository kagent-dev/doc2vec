// Shared types for controller mode

export interface StartControllerOptions {
    configArgs: string[];          // config files and/or directories from the CLI
    databaseUrl?: string;
    port: number;
    readWrite: boolean;
    configDir?: string;            // where UI-created configs are written (read-write mode)
    maxParallel: number;
    reloadIntervalSec: number;
    logRetentionDays: number;
    slackWebhookUrl?: string;      // Slack incoming webhook — notifies when runs finish
    slackNotify?: 'all' | 'failures';
    publicUrl?: string;            // externally reachable base URL, used for links in notifications
}

export interface SourceSummary {
    type: string;
    product_name: string;
    version?: string;
}

export interface ConfigRecord {
    id: number;
    path: string;
    name: string;
    content: string;               // raw YAML, ${ENV} placeholders unresolved
    content_hash: string;
    schedule: string | null;
    source_summary: SourceSummary[];
    parse_error: string | null;    // set when the YAML is invalid — config shown as broken in the UI
    enabled: boolean;
    deleted_at: string | null;
    created_at: string;
    updated_at: string;
}

export type RunTrigger = 'scheduled' | 'manual';
export type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'skipped' | 'canceled';

export interface RunRecord {
    id: number;
    config_id: number;
    config_hash: string;
    trigger: RunTrigger;
    status: RunStatus;
    pid: number | null;
    exit_code: number | null;
    error: string | null;
    stats: RunStatsPayload;
    queued_at: string;
    started_at: string | null;
    finished_at: string | null;
}

export interface RunStatsPayload {
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
}

export interface LogRow {
    seq: number;
    ts: string;
    level: string;
    module: string | null;
    message: string;
}

export class ConflictError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ConflictError';
    }
}

export class NotFoundError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'NotFoundError';
    }
}

export class ValidationError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'ValidationError';
    }
}
