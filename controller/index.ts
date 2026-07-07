import * as fs from 'fs';
import * as path from 'path';
import { Logger, LogLevel } from '../logger';
import { ConfigRegistry } from './config-registry';
import { ControllerEvents } from './events';
import { JobRunner } from './job-runner';
import { Scheduler } from './scheduler';
import { createServer } from './server';
import { ControllerStore } from './store';
import { StartControllerOptions } from './types';

export { StartControllerOptions } from './types';

const LOG_PRUNE_INTERVAL_MS = 24 * 60 * 60 * 1000;

export async function startController(opts: StartControllerOptions): Promise<void> {
    const logger = new Logger('Controller', {
        level: LogLevel.INFO,
        useTimestamp: true,
        useColor: true,
        prettyPrint: true,
    });

    if (!opts.databaseUrl) {
        throw new Error('controller mode requires Postgres: pass --database-url or set DATABASE_URL');
    }
    if (opts.readWrite && !opts.configDir) {
        throw new Error('--read-write requires --config-dir (where new configs are written)');
    }
    if (opts.configArgs.length === 0 && !(opts.readWrite && opts.configDir)) {
        throw new Error('provide at least one config file or directory');
    }
    if (opts.configDir) {
        fs.mkdirSync(path.resolve(opts.configDir), { recursive: true });
    }

    logger.section('DOC2VEC CONTROLLER');
    logger.info(`Mode: ${opts.readWrite ? 'read-write' : 'read-only'}, max parallel jobs: ${opts.maxParallel}`);

    const store = new ControllerStore(opts.databaseUrl, logger.child('store'));
    await store.init();

    const events = new ControllerEvents();
    const runner = new JobRunner(store, events, { maxParallel: Math.max(opts.maxParallel, 1) }, logger.child('runner'));
    const registry = new ConfigRegistry(
        {
            configArgs: opts.configArgs,
            configDir: opts.configDir,
            readWrite: opts.readWrite,
            reloadIntervalSec: opts.reloadIntervalSec,
        },
        store,
        events,
        logger.child('registry')
    );
    const scheduler = new Scheduler(configId => {
        const config = registry.get(configId);
        if (!config) return;
        runner.enqueue(config, 'scheduled').catch(err =>
            logger.error(`Failed to enqueue scheduled run for '${config.name}':`, err)
        );
    }, logger.child('scheduler'));

    events.on('config:update', () => scheduler.sync(registry.list()));

    await registry.start();
    scheduler.sync(registry.list());

    const app = createServer({
        store,
        registry,
        scheduler,
        runner,
        events,
        readWrite: opts.readWrite,
        logger: logger.child('api'),
    });
    const server = app.listen(opts.port, () => {
        logger.info(`API and UI listening on http://localhost:${opts.port}`);
    });

    const pruneLogs = () => {
        store.pruneOldLogs(opts.logRetentionDays)
            .then(count => { if (count > 0) logger.info(`Pruned ${count} log row(s) older than ${opts.logRetentionDays} days`); })
            .catch(err => logger.error('Log retention pruning failed:', err));
    };
    pruneLogs();
    const pruneTimer = setInterval(pruneLogs, LOG_PRUNE_INTERVAL_MS);
    pruneTimer.unref();

    let shuttingDown = false;
    const shutdown = (signal: string) => {
        if (shuttingDown) return;
        shuttingDown = true;
        logger.info(`Received ${signal}, shutting down...`);
        clearInterval(pruneTimer);
        scheduler.stop();
        registry.stop();
        server.close();
        runner.shutdown()
            .then(() => store.close())
            .then(() => {
                logger.info('Shutdown complete');
                process.exit(0);
            })
            .catch(err => {
                logger.error('Error during shutdown:', err);
                process.exit(1);
            });
    };
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
}
