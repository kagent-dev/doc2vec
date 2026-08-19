import { Cron } from 'croner';
import { Logger } from '../logger';
import { ConfigRecord } from './types';

/**
 * One croner Cron per config that has a valid schedule. Reconciled against the
 * registry's current config list whenever configs change on disk.
 */
export class Scheduler {
    private crons = new Map<number, Cron>();

    constructor(
        private onTrigger: (configId: number) => void,
        private logger: Logger
    ) {}

    sync(configs: ConfigRecord[]): void {
        const seen = new Set<number>();
        for (const config of configs) {
            if (!config.schedule || config.parse_error || !config.enabled || config.deleted_at) continue;
            seen.add(config.id);

            const existing = this.crons.get(config.id);
            if (existing && existing.getPattern() === config.schedule) continue;
            existing?.stop();

            try {
                const cron = new Cron(config.schedule, { name: `config-${config.id}` }, () => {
                    this.onTrigger(config.id);
                });
                this.crons.set(config.id, cron);
                this.logger.info(`Scheduled '${config.name}' (${config.schedule}), next run ${cron.nextRun()?.toISOString() ?? 'never'}`);
            } catch (err) {
                this.crons.delete(config.id);
                this.logger.warn(`Failed to schedule '${config.name}' with '${config.schedule}':`, err);
            }
        }

        for (const [id, cron] of this.crons) {
            if (!seen.has(id)) {
                cron.stop();
                this.crons.delete(id);
                this.logger.info(`Unscheduled config ${id}`);
            }
        }
    }

    nextRun(configId: number): string | null {
        return this.crons.get(configId)?.nextRun()?.toISOString() ?? null;
    }

    stop(): void {
        for (const cron of this.crons.values()) cron.stop();
        this.crons.clear();
    }
}
