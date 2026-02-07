import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        globals: true,
        environment: 'node',
        include: ['tests/**/*.test.ts'],
        testTimeout: 30000,
        coverage: {
            provider: 'v8',
            include: ['utils.ts', 'logger.ts', 'content-processor.ts', 'database.ts', 'code-chunker.ts', 'doc2vec.ts'],
            exclude: ['mcp/**', 'dist/**', 'tests/**'],
        },
    },
});
