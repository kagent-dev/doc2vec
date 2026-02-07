import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ContentProcessor } from '../content-processor';
import { Logger, LogLevel } from '../logger';
import * as fs from 'fs';
import * as path from 'path';
import { SourceConfig, WebsiteSourceConfig, LocalDirectorySourceConfig, CodeSourceConfig } from '../types';

// Suppress logger output during tests
const testLogger = new Logger('test', { level: LogLevel.NONE });

describe('ContentProcessor', () => {
    let processor: ContentProcessor;

    beforeEach(() => {
        processor = new ContentProcessor(testLogger);
    });

    // ─── convertHtmlToMarkdown ──────────────────────────────────────
    describe('convertHtmlToMarkdown', () => {
        it('should convert simple HTML to markdown', () => {
            const html = '<h1>Title</h1><p>Hello world</p>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('# Title');
            expect(md).toContain('Hello world');
        });

        it('should return empty string for empty input', () => {
            expect(processor.convertHtmlToMarkdown('')).toBe('');
            expect(processor.convertHtmlToMarkdown('   ')).toBe('');
        });

        it('should convert bold and italic', () => {
            const html = '<p><strong>bold</strong> and <em>italic</em></p>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('**bold**');
            // Turndown may use _ or * for italic
            expect(md).toMatch(/[_*]italic[_*]/);
        });

        it('should convert unordered lists', () => {
            const html = '<ul><li>Item 1</li><li>Item 2</li></ul>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('Item 1');
            expect(md).toContain('Item 2');
        });

        it('should convert ordered lists', () => {
            const html = '<ol><li>First</li><li>Second</li></ol>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('First');
            expect(md).toContain('Second');
        });

        it('should convert links', () => {
            const html = '<a href="https://example.com">link text</a>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('[link text](https://example.com)');
        });

        it('should handle code blocks (pre tags)', () => {
            const html = '<pre><code>const x = 1;\nconst y = 2;</code></pre>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('```');
            expect(md).toContain('const x = 1;');
        });

        it('should handle inline code', () => {
            const html = '<p>Use <code>npm install</code> to install.</p>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('`npm install`');
        });

        it('should strip disallowed tags (img, script)', () => {
            const html = '<p>Text</p><img src="test.png"/><script>alert("xss")</script>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).not.toContain('<img');
            expect(md).not.toContain('<script');
            expect(md).not.toContain('alert');
            expect(md).toContain('Text');
        });

        it('should convert headings at all levels', () => {
            const html = '<h1>H1</h1><h2>H2</h2><h3>H3</h3><h4>H4</h4><h5>H5</h5><h6>H6</h6>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('# H1');
            expect(md).toContain('## H2');
            expect(md).toContain('### H3');
            expect(md).toContain('#### H4');
            expect(md).toContain('##### H5');
            expect(md).toContain('###### H6');
        });

        it('should handle tables', () => {
            const html = `
                <table>
                    <thead><tr><th>Name</th><th>Value</th></tr></thead>
                    <tbody><tr><td>A</td><td>1</td></tr></tbody>
                </table>`;
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('Name');
            expect(md).toContain('Value');
            expect(md).toContain('|');
            expect(md).toContain('---');
        });

        it('should escape pipe characters in table cells', () => {
            const html = `
                <table>
                    <thead><tr><th>Expression</th></tr></thead>
                    <tbody><tr><td>a | b</td></tr></tbody>
                </table>`;
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('a \\| b');
        });

        it('should handle empty table cells', () => {
            const html = `
                <table>
                    <thead><tr><th>Name</th><th>Value</th></tr></thead>
                    <tbody><tr><td>A</td><td></td></tr></tbody>
                </table>`;
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('|');
        });

        it('should handle blockquotes', () => {
            const html = '<blockquote>Quoted text</blockquote>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('> Quoted text');
        });

        it('should clean up code block indentation', () => {
            const html = '<pre>    line1\n    line2\n    line3</pre>';
            const md = processor.convertHtmlToMarkdown(html);
            expect(md).toContain('line1');
            // The indentation should be cleaned up
            expect(md).not.toMatch(/^    line1/m);
        });
    });

    // ─── chunkMarkdown ──────────────────────────────────────────────
    describe('chunkMarkdown', () => {
        const baseConfig: SourceConfig = {
            type: 'website',
            product_name: 'TestProduct',
            version: '1.0',
            max_size: 100000,
            url: 'https://example.com',
            database_config: { type: 'sqlite', params: {} }
        };

        it('should create chunks from markdown content', async () => {
            const markdown = '# Introduction\n\n' + 'This is a test paragraph. '.repeat(50);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should inject breadcrumb context prefix', async () => {
            const markdown = '# Section A\n\n## Subsection B\n\n' + 'Content here. '.repeat(50);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            // At least one chunk should have a topic breadcrumb
            const hasBreadcrumb = chunks.some(c => c.content.includes('[Topic:'));
            expect(hasBreadcrumb).toBe(true);
        });

        it('should assign correct metadata', async () => {
            const markdown = '# Title\n\n' + 'Paragraph content. '.repeat(30);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/test');
            const chunk = chunks[0];
            expect(chunk.metadata.product_name).toBe('TestProduct');
            expect(chunk.metadata.version).toBe('1.0');
            expect(chunk.metadata.url).toBe('https://example.com/test');
            expect(chunk.metadata.chunk_index).toBe(0);
            expect(chunk.metadata.total_chunks).toBe(chunks.length);
        });

        it('should generate deterministic chunk IDs', async () => {
            const markdown = '# Title\n\nContent here. '.repeat(20);
            const chunks1 = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            const chunks2 = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            expect(chunks1.length).toBe(chunks2.length);
            for (let i = 0; i < chunks1.length; i++) {
                expect(chunks1[i].metadata.chunk_id).toBe(chunks2[i].metadata.chunk_id);
            }
        });

        it('should handle empty markdown', async () => {
            const chunks = await processor.chunkMarkdown('', baseConfig, 'https://example.com/page');
            expect(chunks.length).toBe(0);
        });

        it('should track heading hierarchy', async () => {
            const markdown = '# Main\n\n## Sub\n\n### Deep\n\n' + 'Content goes here. '.repeat(50);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            // Some chunk should have heading hierarchy populated
            const hasHierarchy = chunks.some(c => c.metadata.heading_hierarchy.length > 0);
            expect(hasHierarchy).toBe(true);
        });

        it('should merge small sections (semantic merging)', async () => {
            // Create very short sections that should be merged
            const markdown = '# Section A\n\nShort.\n\n# Section B\n\nAlso short.';
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            // Both short sections should be merged into one chunk
            expect(chunks.length).toBe(1);
        });

        it('should split large sections with overlap', async () => {
            // Create a very large section that must be split
            const largeContent = '# Big Section\n\n' + 'This is a long sentence that will repeat many times to exceed the max token limit. '.repeat(200);
            const chunks = await processor.chunkMarkdown(largeContent, baseConfig, 'https://example.com/page');
            expect(chunks.length).toBeGreaterThan(1);
        });

        it('should set total_chunks correctly on all chunks', async () => {
            const markdown = '# Section 1\n\n' + 'Content for section one. '.repeat(100) +
                '\n\n# Section 2\n\n' + 'Content for section two. '.repeat(100);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            const total = chunks.length;
            for (const chunk of chunks) {
                expect(chunk.metadata.total_chunks).toBe(total);
            }
        });

        it('should clean anchor links from headings in hierarchy', async () => {
            const markdown = '# Title [](#anchor-id)\n\n' + 'Some content here. '.repeat(50);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            // The heading hierarchy and section should have the anchor stripped
            for (const chunk of chunks) {
                for (const h of chunk.metadata.heading_hierarchy) {
                    expect(h).not.toContain('[](#anchor-id)');
                }
            }
        });

        it('should assign sequential chunk_index values', async () => {
            const markdown = '# Part 1\n\n' + 'Content one. '.repeat(100) +
                '\n\n# Part 2\n\n' + 'Content two. '.repeat(100) +
                '\n\n# Part 3\n\n' + 'Content three. '.repeat(100);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            for (let i = 0; i < chunks.length; i++) {
                expect(chunks[i].metadata.chunk_index).toBe(i);
            }
        });

        it('should use "Introduction" as default section when no heading', async () => {
            const markdown = 'Just some text without any headings. '.repeat(50);
            const chunks = await processor.chunkMarkdown(markdown, baseConfig, 'https://example.com/page');
            expect(chunks[0].metadata.section).toBe('Introduction');
        });
    });

    // ─── chunkCode ──────────────────────────────────────────────────
    describe('chunkCode', () => {
        const codeConfig: CodeSourceConfig = {
            type: 'code',
            source: 'local_directory',
            path: '/test',
            product_name: 'TestProduct',
            version: '1.0',
            max_size: 100000,
            database_config: { type: 'sqlite', params: {} }
        };

        it('should create chunks from code content', async () => {
            const code = 'function hello() {\n  console.log("world");\n}\n'.repeat(10);
            const chunks = await processor.chunkCode(code, codeConfig, 'file:///test/hello.ts', 'hello.ts');
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should inject file path context', async () => {
            const code = 'const x = 1;\nconst y = 2;\n';
            const chunks = await processor.chunkCode(code, codeConfig, 'file:///test/util.ts', 'src/util.ts');
            expect(chunks[0].content).toContain('[File: src/util.ts]');
        });

        it('should assign correct metadata', async () => {
            const code = 'export const VERSION = "1.0";\n';
            const chunks = await processor.chunkCode(code, codeConfig, 'file:///test/version.ts', 'version.ts');
            expect(chunks[0].metadata.product_name).toBe('TestProduct');
            expect(chunks[0].metadata.version).toBe('1.0');
            expect(chunks[0].metadata.url).toBe('file:///test/version.ts');
        });

        it('should include branch and repo when provided', async () => {
            const code = 'const a = 1;\n';
            const chunks = await processor.chunkCode(code, codeConfig, 'file:///test/a.ts', 'a.ts', 'main', 'org/repo');
            expect(chunks[0].metadata.branch).toBe('main');
            expect(chunks[0].metadata.repo).toBe('org/repo');
        });

        it('should not include branch/repo when not provided', async () => {
            const code = 'const b = 2;\n';
            const chunks = await processor.chunkCode(code, codeConfig, 'file:///test/b.ts', 'b.ts');
            expect(chunks[0].metadata.branch).toBeUndefined();
            expect(chunks[0].metadata.repo).toBeUndefined();
        });

        it('should set total_chunks on all chunks', async () => {
            const code = 'function a() { return 1; }\n'.repeat(50);
            const chunks = await processor.chunkCode(code, codeConfig, 'file:///test/funcs.ts', 'funcs.ts');
            const total = chunks.length;
            for (const chunk of chunks) {
                expect(chunk.metadata.total_chunks).toBe(total);
            }
        });

        it('should handle empty code', async () => {
            const chunks = await processor.chunkCode('', codeConfig, 'file:///test/empty.ts', 'empty.ts');
            expect(chunks.length).toBe(0);
        });

        it('should normalize backslashes in file paths', async () => {
            const code = 'const x = 1;\n';
            const chunks = await processor.chunkCode(code, codeConfig, 'file:///test/a.ts', 'src\\utils\\a.ts');
            expect(chunks[0].content).toContain('src/utils/a.ts');
        });
    });

    // ─── processDirectory ───────────────────────────────────────────
    describe('processDirectory', () => {
        const testDir = path.join(__dirname, '__test_dir__');

        beforeEach(() => {
            if (fs.existsSync(testDir)) {
                fs.rmSync(testDir, { recursive: true });
            }
            fs.mkdirSync(testDir, { recursive: true });
        });

        afterEach(() => {
            if (fs.existsSync(testDir)) {
                fs.rmSync(testDir, { recursive: true });
            }
        });

        const dirConfig: LocalDirectorySourceConfig = {
            type: 'local_directory',
            path: '',
            product_name: 'Test',
            version: '1.0',
            max_size: 100000,
            database_config: { type: 'sqlite', params: {} }
        };

        it('should process text files in directory', async () => {
            fs.writeFileSync(path.join(testDir, 'test.md'), '# Hello\n\nWorld');
            const processed: string[] = [];

            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, include_extensions: ['.md'] },
                async (filePath, content) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('test.md');
        });

        it('should filter by include_extensions', async () => {
            fs.writeFileSync(path.join(testDir, 'keep.md'), 'content');
            fs.writeFileSync(path.join(testDir, 'skip.txt'), 'content');

            const processed: string[] = [];
            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, include_extensions: ['.md'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('keep.md');
        });

        it('should filter by exclude_extensions', async () => {
            fs.writeFileSync(path.join(testDir, 'a.md'), 'content');
            fs.writeFileSync(path.join(testDir, 'b.txt'), 'content');

            const processed: string[] = [];
            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, include_extensions: ['.md', '.txt'], exclude_extensions: ['.txt'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('a.md');
        });

        it('should traverse subdirectories when recursive=true', async () => {
            const subDir = path.join(testDir, 'sub');
            fs.mkdirSync(subDir);
            fs.writeFileSync(path.join(subDir, 'nested.md'), 'content');

            const processed: string[] = [];
            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, recursive: true, include_extensions: ['.md'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('nested.md');
        });

        it('should NOT traverse subdirectories when recursive=false', async () => {
            const subDir = path.join(testDir, 'sub');
            fs.mkdirSync(subDir);
            fs.writeFileSync(path.join(subDir, 'nested.md'), 'content');
            fs.writeFileSync(path.join(testDir, 'top.md'), 'content');

            const processed: string[] = [];
            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, recursive: false, include_extensions: ['.md'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('top.md');
        });

        it('should skip files exceeding max_size', async () => {
            const bigContent = 'x'.repeat(200);
            fs.writeFileSync(path.join(testDir, 'big.md'), bigContent);

            const processed: string[] = [];
            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, max_size: 100, include_extensions: ['.md'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(0);
        });

        it('should convert .html files to markdown', async () => {
            fs.writeFileSync(path.join(testDir, 'page.html'), '<h1>Title</h1><p>Content</p>');

            let capturedContent = '';
            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, include_extensions: ['.html'] },
                async (_, content) => { capturedContent = content; },
                testLogger
            );

            expect(capturedContent).toContain('# Title');
            expect(capturedContent).toContain('Content');
        });

        it('should deduplicate visited paths', async () => {
            fs.writeFileSync(path.join(testDir, 'dup.md'), 'content');

            const processed: string[] = [];
            const visited = new Set<string>();
            visited.add(path.join(testDir, 'dup.md'));

            await processor.processDirectory(
                testDir,
                { ...dirConfig, path: testDir, include_extensions: ['.md'] },
                async (filePath) => { processed.push(filePath); },
                testLogger,
                visited
            );

            expect(processed.length).toBe(0);
        });
    });

    // ─── processCodeDirectory ───────────────────────────────────────
    describe('processCodeDirectory', () => {
        const testDir = path.join(__dirname, '__test_code_dir__');

        beforeEach(() => {
            if (fs.existsSync(testDir)) {
                fs.rmSync(testDir, { recursive: true });
            }
            fs.mkdirSync(testDir, { recursive: true });
        });

        afterEach(() => {
            if (fs.existsSync(testDir)) {
                fs.rmSync(testDir, { recursive: true });
            }
        });

        const codeConfig: CodeSourceConfig = {
            type: 'code',
            source: 'local_directory',
            path: '',
            product_name: 'Test',
            version: '1.0',
            max_size: 100000,
            database_config: { type: 'sqlite', params: {} }
        };

        it('should process code files', async () => {
            fs.writeFileSync(path.join(testDir, 'hello.ts'), 'const x = 1;');

            const processed: string[] = [];
            const result = await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, include_extensions: ['.ts'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(result.processedFiles).toBe(1);
        });

        it('should use default extensions when none specified', async () => {
            fs.writeFileSync(path.join(testDir, 'hello.ts'), 'const x = 1;');
            fs.writeFileSync(path.join(testDir, 'data.xyz'), 'data');

            const processed: string[] = [];
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            // .ts is in defaults, .xyz is not
            expect(processed.length).toBe(1);
        });

        it('should skip files with excluded extensions', async () => {
            fs.writeFileSync(path.join(testDir, 'a.ts'), 'code');
            fs.writeFileSync(path.join(testDir, 'b.js'), 'code');

            const processed: string[] = [];
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, include_extensions: ['.ts', '.js'], exclude_extensions: ['.js'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('a.ts');
        });

        it('should track files in trackFiles set', async () => {
            fs.writeFileSync(path.join(testDir, 'tracked.ts'), 'code');

            const trackFiles = new Set<string>();
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, include_extensions: ['.ts'] },
                async () => {},
                testLogger,
                undefined,
                { trackFiles }
            );

            expect(trackFiles.size).toBe(1);
            expect(Array.from(trackFiles)[0]).toContain('tracked.ts');
        });

        it('should skip files not in allowedFiles', async () => {
            fs.writeFileSync(path.join(testDir, 'allowed.ts'), 'code');
            fs.writeFileSync(path.join(testDir, 'skipped.ts'), 'code');

            const processed: string[] = [];
            const allowedFiles = new Set([path.join(testDir, 'allowed.ts')]);
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, include_extensions: ['.ts'] },
                async (filePath) => { processed.push(filePath); },
                testLogger,
                undefined,
                { allowedFiles }
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('allowed.ts');
        });

        it('should skip files older than mtimeCutoff', async () => {
            fs.writeFileSync(path.join(testDir, 'old.ts'), 'code');
            // Set mtime to far in the past
            const oldTime = new Date('2020-01-01');
            fs.utimesSync(path.join(testDir, 'old.ts'), oldTime, oldTime);

            const processed: string[] = [];
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, include_extensions: ['.ts'] },
                async (filePath) => { processed.push(filePath); },
                testLogger,
                undefined,
                { mtimeCutoff: Date.now() }
            );

            expect(processed.length).toBe(0);
        });

        it('should return maxMtime of processed files', async () => {
            fs.writeFileSync(path.join(testDir, 'recent.ts'), 'code');

            const result = await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, include_extensions: ['.ts'] },
                async () => {},
                testLogger
            );

            expect(result.maxMtime).toBeGreaterThan(0);
        });

        it('should skip files exceeding max_size', async () => {
            const bigContent = 'x'.repeat(200);
            fs.writeFileSync(path.join(testDir, 'big.ts'), bigContent);

            const processed: string[] = [];
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, max_size: 100, include_extensions: ['.ts'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(0);
        });

        it('should handle recursive traversal', async () => {
            const subDir = path.join(testDir, 'sub');
            fs.mkdirSync(subDir);
            fs.writeFileSync(path.join(subDir, 'nested.ts'), 'code');

            const processed: string[] = [];
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, recursive: true, include_extensions: ['.ts'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(1);
            expect(processed[0]).toContain('nested.ts');
        });

        it('should NOT recurse when recursive=false', async () => {
            const subDir = path.join(testDir, 'sub');
            fs.mkdirSync(subDir);
            fs.writeFileSync(path.join(subDir, 'nested.ts'), 'code');

            const processed: string[] = [];
            await processor.processCodeDirectory(
                testDir,
                { ...codeConfig, path: testDir, recursive: false, include_extensions: ['.ts'] },
                async (filePath) => { processed.push(filePath); },
                testLogger
            );

            expect(processed.length).toBe(0);
        });
    });

    // ─── isNetworkError (accessed via crawlWebsite behavior) ────────
    describe('network error detection', () => {
        // We test the private isNetworkError indirectly through its behavior.
        // The method is private, so we access it via prototype for unit testing.
        const isNetworkError = (ContentProcessor.prototype as any).isNetworkError;

        it('should detect ENOTFOUND errors', () => {
            const error = { code: 'ENOTFOUND' };
            expect(isNetworkError(error)).toBe(true);
        });

        it('should detect ECONNREFUSED errors', () => {
            const error = { code: 'ECONNREFUSED' };
            expect(isNetworkError(error)).toBe(true);
        });

        it('should detect ETIMEDOUT errors', () => {
            const error = { code: 'ETIMEDOUT' };
            expect(isNetworkError(error)).toBe(true);
        });

        it('should detect ECONNRESET errors', () => {
            const error = { code: 'ECONNRESET' };
            expect(isNetworkError(error)).toBe(true);
        });

        it('should detect EHOSTUNREACH errors', () => {
            const error = { code: 'EHOSTUNREACH' };
            expect(isNetworkError(error)).toBe(true);
        });

        it('should detect ENETUNREACH errors', () => {
            const error = { code: 'ENETUNREACH' };
            expect(isNetworkError(error)).toBe(true);
        });

        it('should detect axios errors without response', () => {
            const error = { isAxiosError: true, response: undefined };
            expect(isNetworkError(error)).toBe(true);
        });

        it('should NOT detect axios errors with response', () => {
            const error = { isAxiosError: true, response: { status: 404 } };
            expect(isNetworkError(error)).toBe(false);
        });

        it('should detect network-related error messages', () => {
            expect(isNetworkError({ message: 'getaddrinfo ENOTFOUND' })).toBe(true);
            expect(isNetworkError({ message: 'network error' })).toBe(true);
            expect(isNetworkError({ message: 'connection timeout' })).toBe(true);
            expect(isNetworkError({ message: 'dns resolution failed' })).toBe(true);
        });

        it('should NOT detect non-network errors', () => {
            const error = { code: 'ENOENT', message: 'file not found' };
            expect(isNetworkError(error)).toBe(false);
        });

        it('should handle null/undefined error', () => {
            expect(isNetworkError(null)).toBe(false);
            expect(isNetworkError(undefined)).toBe(false);
        });
    });
});
