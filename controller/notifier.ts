import { Logger } from '../logger';
import { ControllerEvents } from './events';
import { ControllerStore } from './store';
import { RunRecord } from './types';

export interface SlackNotifierOptions {
    webhookUrl: string;
    notify: 'all' | 'failures';   // 'failures' also covers canceled runs
    publicUrl?: string;           // e.g. https://doc2vec.is.solo.io — enables "View run" links
}

// Terminal statuses worth a notification. 'skipped' (overlapping schedule)
// is deliberately excluded — it would be pure noise on busy schedules.
const NOTIFIED_STATUSES: RunRecord['status'][] = ['succeeded', 'failed', 'canceled'];

const STATUS_DECOR: Record<string, { emoji: string; verb: string }> = {
    succeeded: { emoji: '✅', verb: 'succeeded' },
    failed: { emoji: '❌', verb: 'failed' },
    canceled: { emoji: '⚠️', verb: 'was canceled' },
};

function formatDuration(run: RunRecord): string | null {
    if (!run.started_at || !run.finished_at) return null;
    const seconds = (new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()) / 1000;
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ${Math.round(seconds % 60)}s`;
    return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

/** Builds the Slack webhook payload (Block Kit) for a finished run. */
export function buildRunMessage(run: RunRecord, configName: string, publicUrl?: string): Record<string, any> {
    const decor = STATUS_DECOR[run.status] ?? { emoji: 'ℹ️', verb: run.status };
    const sources = run.stats?.sources ?? [];
    const failedSources = sources.filter(s => !s.ok);

    let headline = `${decor.emoji} doc2vec sync *${configName}* ${decor.verb}`;
    if (publicUrl) {
        headline += ` — <${publicUrl.replace(/\/$/, '')}/runs/${run.id}|view run #${run.id}>`;
    } else {
        headline += ` (run #${run.id})`;
    }

    const lines: string[] = [];
    if (sources.length > 0) {
        lines.push(`${sources.length - failedSources.length}/${sources.length} sources ok`);
    }
    if (failedSources.length > 0) {
        const shown = failedSources.slice(0, 5)
            .map(s => `• *${s.product_name}*: ${s.error ?? 'failed'}`);
        if (failedSources.length > 5) shown.push(`• …and ${failedSources.length - 5} more`);
        lines.push(shown.join('\n'));
    }
    if (run.error && failedSources.length === 0) {
        lines.push(run.error);
    }

    const meta: string[] = [`trigger: ${run.trigger}`];
    const duration = formatDuration(run);
    if (duration) meta.push(`duration: ${duration}`);
    if (run.stats?.warn_count) meta.push(`warnings: ${run.stats.warn_count}`);
    if (run.stats?.error_count) meta.push(`errors: ${run.stats.error_count}`);

    return {
        text: `doc2vec sync ${configName} ${decor.verb}`,   // fallback for notifications
        blocks: [
            {
                type: 'section',
                text: { type: 'mrkdwn', text: [headline, ...lines].join('\n') },
            },
            {
                type: 'context',
                elements: [{ type: 'mrkdwn', text: meta.join(' · ') }],
            },
        ],
    };
}

/**
 * Posts a Slack message (incoming webhook) whenever a run reaches a terminal
 * status. Subscribes to the same event bus that feeds the UI, so every path
 * that finishes a run — success, failure, cancel — is covered.
 */
export class SlackNotifier {
    constructor(
        private opts: SlackNotifierOptions,
        private store: ControllerStore,
        private logger: Logger
    ) {}

    attach(events: ControllerEvents): void {
        events.on('run:update', (run: RunRecord) => {
            this.maybeNotify(run).catch(err =>
                this.logger.error(`Failed to send Slack notification for run ${run.id}:`, err)
            );
        });
        this.logger.info(`Slack notifications enabled (${this.opts.notify})`);
    }

    private async maybeNotify(run: RunRecord): Promise<void> {
        if (!NOTIFIED_STATUSES.includes(run.status)) return;
        if (this.opts.notify === 'failures' && run.status === 'succeeded') return;

        const config = await this.store.getConfig(run.config_id);
        const payload = buildRunMessage(run, config?.name ?? `config ${run.config_id}`, this.opts.publicUrl);

        const response = await fetch(this.opts.webhookUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        if (!response.ok) {
            this.logger.warn(`Slack webhook returned ${response.status}: ${await response.text().catch(() => '')}`);
        }
    }
}
