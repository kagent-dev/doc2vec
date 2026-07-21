import { describe, it, expect, afterEach, vi } from 'vitest';
import { Scheduler } from '../controller/scheduler';
import { ConfigRecord } from '../controller/types';

function makeLogger(): any {
    const l: any = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn(), section: vi.fn(), event: vi.fn() };
    l.child = vi.fn(() => makeLogger());
    return l;
}

function makeConfig(overrides: Partial<ConfigRecord> = {}): ConfigRecord {
    return {
        id: 1,
        path: '/x/a.yaml',
        name: 'a',
        content: '',
        content_hash: 'h1',
        schedule: '0 2 * * *',
        source_summary: [],
        parse_error: null,
        enabled: true,
        deleted_at: null,
        created_at: '',
        updated_at: '',
        ...overrides,
    };
}

describe('Scheduler', () => {
    let scheduler: Scheduler;

    afterEach(() => scheduler?.stop());

    it('schedules configs with a valid cron and reports nextRun', () => {
        scheduler = new Scheduler(() => {}, makeLogger());
        scheduler.sync([makeConfig()]);
        const next = scheduler.nextRun(1);
        expect(next).toBeTruthy();
        const nextDate = new Date(next!);
        expect(nextDate.getHours()).toBe(2);
        expect(nextDate.getMinutes()).toBe(0);
    });

    it('does not schedule configs without a schedule, with parse errors, or deleted', () => {
        scheduler = new Scheduler(() => {}, makeLogger());
        scheduler.sync([
            makeConfig({ id: 1, schedule: null }),
            makeConfig({ id: 2, parse_error: 'broken' }),
            makeConfig({ id: 3, deleted_at: new Date().toISOString() }),
            makeConfig({ id: 4, enabled: false }),
        ]);
        for (const id of [1, 2, 3, 4]) expect(scheduler.nextRun(id)).toBeNull();
    });

    it('reschedules when the cron pattern changes and keeps it otherwise', () => {
        scheduler = new Scheduler(() => {}, makeLogger());
        scheduler.sync([makeConfig()]);
        const before = scheduler.nextRun(1);

        scheduler.sync([makeConfig()]); // same pattern → same cron kept
        expect(scheduler.nextRun(1)).toBe(before);

        scheduler.sync([makeConfig({ schedule: '0 5 * * *' })]);
        expect(new Date(scheduler.nextRun(1)!).getHours()).toBe(5);
    });

    it('unschedules configs that disappear from the list', () => {
        scheduler = new Scheduler(() => {}, makeLogger());
        scheduler.sync([makeConfig()]);
        expect(scheduler.nextRun(1)).toBeTruthy();
        scheduler.sync([]);
        expect(scheduler.nextRun(1)).toBeNull();
    });

    it('fires the trigger callback when the schedule elapses', async () => {
        const fired: number[] = [];
        scheduler = new Scheduler(id => fired.push(id), makeLogger());
        // croner supports second-granularity patterns
        scheduler.sync([makeConfig({ schedule: '* * * * * *' })]);
        await new Promise(resolve => setTimeout(resolve, 1500));
        expect(fired.length).toBeGreaterThanOrEqual(1);
        expect(fired[0]).toBe(1);
    });
});
