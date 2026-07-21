import { EventEmitter } from 'events';
import { LogRow, RunRecord } from './types';

/**
 * In-process event bus connecting the job runner / config registry to SSE clients.
 *
 * Events:
 *  - 'run:update'    (run: RunRecord)                     — a run was created or changed status
 *  - 'run:log'       ({ runId: number, lines: LogRow[] }) — new log lines for a running job
 *  - 'config:update' ()                                   — the set of configs changed on disk
 */
export class ControllerEvents extends EventEmitter {
    constructor() {
        super();
        // Every SSE client adds a listener per event; don't warn at the default 10
        this.setMaxListeners(1000);
    }

    emitRunUpdate(run: RunRecord): void {
        this.emit('run:update', run);
    }

    emitRunLogs(runId: number, lines: LogRow[]): void {
        this.emit('run:log', { runId, lines });
    }

    emitConfigUpdate(): void {
        this.emit('config:update');
    }
}
