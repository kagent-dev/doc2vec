import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Logger, LogLevel } from '../logger';

describe('Logger', () => {
    let logSpy: ReturnType<typeof vi.spyOn>;
    let warnSpy: ReturnType<typeof vi.spyOn>;
    let errorSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
        warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    // ─── Constructor and defaults ───────────────────────────────────
    describe('constructor', () => {
        it('should create a logger with default config', () => {
            const logger = new Logger('test');
            logger.info('hello');
            expect(logSpy).toHaveBeenCalledTimes(1);
        });

        it('should accept partial config overrides', () => {
            const logger = new Logger('test', { level: LogLevel.ERROR });
            logger.info('should not log');
            expect(logSpy).not.toHaveBeenCalled();
        });

        it('should respect useTimestamp=false', () => {
            const logger = new Logger('test', { useTimestamp: false, useColor: false });
            logger.info('message');
            const output = logSpy.mock.calls[0][0] as string;
            // Should NOT contain ISO timestamp pattern
            expect(output).not.toMatch(/\[\d{4}-\d{2}-\d{2}T/);
        });

        it('should respect useColor=false', () => {
            const logger = new Logger('test', { useColor: false });
            logger.info('message');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).not.toContain('\x1b[');
        });
    });

    // ─── Log level filtering ────────────────────────────────────────
    describe('log level filtering', () => {
        it('should log DEBUG when level is DEBUG', () => {
            const logger = new Logger('test', { level: LogLevel.DEBUG, useColor: false });
            logger.debug('debug msg');
            expect(logSpy).toHaveBeenCalledTimes(1);
        });

        it('should NOT log DEBUG when level is INFO', () => {
            const logger = new Logger('test', { level: LogLevel.INFO });
            logger.debug('debug msg');
            expect(logSpy).not.toHaveBeenCalled();
        });

        it('should log INFO when level is INFO', () => {
            const logger = new Logger('test', { level: LogLevel.INFO });
            logger.info('info msg');
            expect(logSpy).toHaveBeenCalledTimes(1);
        });

        it('should NOT log INFO when level is WARN', () => {
            const logger = new Logger('test', { level: LogLevel.WARN });
            logger.info('info msg');
            expect(logSpy).not.toHaveBeenCalled();
        });

        it('should log WARN when level is WARN', () => {
            const logger = new Logger('test', { level: LogLevel.WARN });
            logger.warn('warn msg');
            expect(warnSpy).toHaveBeenCalledTimes(1);
        });

        it('should NOT log WARN when level is ERROR', () => {
            const logger = new Logger('test', { level: LogLevel.ERROR });
            logger.warn('warn msg');
            expect(warnSpy).not.toHaveBeenCalled();
        });

        it('should log ERROR when level is ERROR', () => {
            const logger = new Logger('test', { level: LogLevel.ERROR });
            logger.error('error msg');
            expect(errorSpy).toHaveBeenCalledTimes(1);
        });

        it('should log nothing when level is NONE', () => {
            const logger = new Logger('test', { level: LogLevel.NONE });
            logger.debug('a');
            logger.info('b');
            logger.warn('c');
            logger.error('d');
            expect(logSpy).not.toHaveBeenCalled();
            expect(warnSpy).not.toHaveBeenCalled();
            expect(errorSpy).not.toHaveBeenCalled();
        });
    });

    // ─── Message formatting ─────────────────────────────────────────
    describe('message formatting', () => {
        it('should include module name in output', () => {
            const logger = new Logger('MyModule', { useColor: false, useTimestamp: false });
            logger.info('test');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('[MyModule]');
        });

        it('should include log level in output', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false });
            logger.info('test');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('[INFO ]');
        });

        it('should pad level names to 5 characters', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false });
            logger.warn('test');
            const output = warnSpy.mock.calls[0][0] as string;
            expect(output).toContain('[WARN ]');
        });

        it('should include timestamp when useTimestamp is true', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: true });
            logger.info('test');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toMatch(/\[\d{4}-\d{2}-\d{2}T/);
        });

        it('should format Error objects with stack trace', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false });
            const err = new Error('something broke');
            logger.error('failure', err);
            const output = errorSpy.mock.calls[0][0] as string;
            expect(output).toContain('Error Details');
            expect(output).toContain('something broke');
            expect(output).toContain('Stack:');
        });

        it('should pretty-print objects when prettyPrint is true', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false, prettyPrint: true });
            logger.info('data', { key: 'value' });
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('"key": "value"');
        });

        it('should handle unserializable objects', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false, prettyPrint: true });
            const circular: any = {};
            circular.self = circular;
            logger.info('data', circular);
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('[Unserializable Object]');
        });

        it('should format args inline when prettyPrint is false', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false, prettyPrint: false });
            logger.info('msg', 'arg1', 'arg2');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('arg1');
            expect(output).toContain('arg2');
        });
    });

    // ─── Color output ───────────────────────────────────────────────
    describe('colorization', () => {
        it('should apply gray color for DEBUG', () => {
            const logger = new Logger('test', { level: LogLevel.DEBUG, useColor: true, useTimestamp: false });
            logger.debug('msg');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('\x1b[90m'); // gray
        });

        it('should apply blue color for INFO', () => {
            const logger = new Logger('test', { useColor: true, useTimestamp: false });
            logger.info('msg');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('\x1b[34m'); // blue
        });

        it('should apply yellow color for WARN', () => {
            const logger = new Logger('test', { useColor: true, useTimestamp: false });
            logger.warn('msg');
            const output = warnSpy.mock.calls[0][0] as string;
            expect(output).toContain('\x1b[33m'); // yellow
        });

        it('should apply red color for ERROR', () => {
            const logger = new Logger('test', { useColor: true, useTimestamp: false });
            logger.error('msg');
            const output = errorSpy.mock.calls[0][0] as string;
            expect(output).toContain('\x1b[31m'); // red
        });

        it('should not apply color when useColor is false', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false });
            logger.info('msg');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).not.toContain('\x1b[');
        });
    });

    // ─── child logger ───────────────────────────────────────────────
    describe('child', () => {
        it('should create a child logger with prefixed name', () => {
            const logger = new Logger('parent', { useColor: false, useTimestamp: false });
            const child = logger.child('child');
            child.info('message');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('[parent:child]');
        });

        it('should inherit config from parent', () => {
            const logger = new Logger('parent', { level: LogLevel.ERROR, useColor: false });
            const child = logger.child('child');
            child.info('should not appear');
            expect(logSpy).not.toHaveBeenCalled();
        });

        it('should support nested children', () => {
            const logger = new Logger('a', { useColor: false, useTimestamp: false });
            const grandchild = logger.child('b').child('c');
            grandchild.info('msg');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('[a:b:c]');
        });
    });

    // ─── section ────────────────────────────────────────────────────
    describe('section', () => {
        it('should output a visual section separator', () => {
            const logger = new Logger('test', { useColor: false });
            logger.section('MY SECTION');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('MY SECTION');
            expect(output).toContain('=');
        });

        it('should return the logger for chaining', () => {
            const logger = new Logger('test', { useColor: false });
            const result = logger.section('Test');
            expect(result).toBe(logger);
        });

        it('should not output when level is above INFO', () => {
            const logger = new Logger('test', { level: LogLevel.WARN });
            logger.section('Section');
            expect(logSpy).not.toHaveBeenCalled();
        });
    });

    // ─── progress ───────────────────────────────────────────────────
    describe('progress', () => {
        it('should create a progress tracker with update and complete methods', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Processing', 10);
            expect(progress).toHaveProperty('update');
            expect(progress).toHaveProperty('complete');
        });

        it('should log progress updates', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Processing', 10);
            progress.update(1);
            expect(logSpy).toHaveBeenCalled();
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('Processing');
            expect(output).toContain('10%');
        });

        it('should log completion', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Processing', 5);
            progress.complete('Done');
            const lastCall = logSpy.mock.calls[logSpy.mock.calls.length - 1][0] as string;
            expect(lastCall).toContain('100%');
            expect(lastCall).toContain('Done');
        });

        it('should include a progress bar', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Test', 2);
            progress.update(1);
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('[');
            expect(output).toContain(']');
        });

        it('should cap percentage at 100', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Test', 2);
            progress.update(5); // exceeds total
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('100%');
        });

        it('should not log progress when level is above INFO', () => {
            const logger = new Logger('test', { level: LogLevel.WARN, useColor: false });
            const progress = logger.progress('Test', 10);
            progress.update(1);
            progress.complete();
            expect(logSpy).not.toHaveBeenCalled();
        });

        it('should include ETA in progress output', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Test', 100);
            // Simulate some time passing
            progress.update(50);
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('ETA:');
        });

        it('should include custom message in progress update', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Test', 10);
            progress.update(1, 'custom status');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('custom status');
        });
    });

    // ─── LogLevel enum ──────────────────────────────────────────────
    describe('LogLevel', () => {
        it('should have correct numeric values', () => {
            expect(LogLevel.DEBUG).toBe(0);
            expect(LogLevel.INFO).toBe(1);
            expect(LogLevel.WARN).toBe(2);
            expect(LogLevel.ERROR).toBe(3);
            expect(LogLevel.NONE).toBe(100);
        });

        it('should maintain ordering', () => {
            expect(LogLevel.DEBUG).toBeLessThan(LogLevel.INFO);
            expect(LogLevel.INFO).toBeLessThan(LogLevel.WARN);
            expect(LogLevel.WARN).toBeLessThan(LogLevel.ERROR);
            expect(LogLevel.ERROR).toBeLessThan(LogLevel.NONE);
        });
    });

    // ─── Empty module name ──────────────────────────────────────────
    describe('empty moduleName', () => {
        it('should omit module prefix when moduleName is empty', () => {
            const logger = new Logger('', { useColor: false, useTimestamp: false });
            logger.info('test');
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).not.toContain('[]');
        });
    });

    // ─── prettyPrint=false with objects ──────────────────────────────
    describe('formatMessage with prettyPrint=false', () => {
        it('should format objects as strings when prettyPrint is false', () => {
            const logger = new Logger('test', { useColor: false, useTimestamp: false, prettyPrint: false });
            logger.info('data', { key: 'value' });
            const output = logSpy.mock.calls[0][0] as string;
            // prettyPrint=false uses String(arg) instead of JSON.stringify
            expect(output).toContain('[object Object]');
        });
    });

    // ─── progress complete() default message ────────────────────────
    describe('progress complete() default message', () => {
        it('should use default "Completed" message when no arg passed', () => {
            const logger = new Logger('test', { useColor: false });
            const progress = logger.progress('Test', 2);
            progress.complete();
            const lastCall = logSpy.mock.calls[logSpy.mock.calls.length - 1][0] as string;
            expect(lastCall).toContain('Completed');
        });
    });

    // ─── progress bar with color ────────────────────────────────────
    describe('progress bar with useColor=true', () => {
        it('should use green color in progress bar when useColor is true', () => {
            const logger = new Logger('test', { useColor: true });
            const progress = logger.progress('Test', 2);
            progress.update(1);
            const output = logSpy.mock.calls[0][0] as string;
            expect(output).toContain('\x1b[32m'); // green
        });
    });

    // ─── section with WARN level ────────────────────────────────────
    describe('section chaining at WARN level', () => {
        it('should return logger for chaining even when level is WARN', () => {
            const logger = new Logger('test', { level: LogLevel.WARN, useColor: false });
            const result = logger.section('Hidden Section');
            expect(result).toBe(logger);
            expect(logSpy).not.toHaveBeenCalled();
        });
    });
});

// ─── defaultLogger export ───────────────────────────────────────────
import { defaultLogger } from '../logger';

describe('defaultLogger', () => {
    it('should be an instance of Logger', () => {
        expect(defaultLogger).toBeDefined();
        expect(defaultLogger).toHaveProperty('info');
        expect(defaultLogger).toHaveProperty('error');
        expect(defaultLogger).toHaveProperty('debug');
        expect(defaultLogger).toHaveProperty('warn');
    });
});
