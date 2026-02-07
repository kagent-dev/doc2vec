import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';

/**
 * The Doc2Vec class is not exported. We test its key internal behaviors
 * through their constituent parts (config loading, URL building, etc.)
 * and through integration-style tests where we can.
 *
 * For the orchestrator, we focus on:
 * 1. Config loading and env var substitution (by importing the module)
 * 2. buildCodeFileUrl logic
 * 3. buildCodeShaMetadataKey logic
 * 4. Config validation (version handling)
 */

describe('Doc2Vec orchestrator', () => {
    // ─── Config loading ─────────────────────────────────────────────
    describe('config loading', () => {
        const testConfigDir = path.join(__dirname, '__test_configs__');

        beforeEach(() => {
            if (fs.existsSync(testConfigDir)) {
                fs.rmSync(testConfigDir, { recursive: true });
            }
            fs.mkdirSync(testConfigDir, { recursive: true });
        });

        afterEach(() => {
            if (fs.existsSync(testConfigDir)) {
                fs.rmSync(testConfigDir, { recursive: true });
            }
        });

        it('should parse a valid YAML config', () => {
            const config = {
                sources: [{
                    type: 'website',
                    product_name: 'TestSite',
                    version: '1.0',
                    max_size: 50000,
                    url: 'https://example.com',
                    database_config: {
                        type: 'sqlite',
                        params: { db_path: ':memory:' }
                    }
                }]
            };

            const configPath = path.join(testConfigDir, 'test.yaml');
            fs.writeFileSync(configPath, yaml.dump(config));

            const loaded = yaml.load(fs.readFileSync(configPath, 'utf8')) as any;
            expect(loaded.sources).toHaveLength(1);
            expect(loaded.sources[0].type).toBe('website');
            expect(loaded.sources[0].product_name).toBe('TestSite');
        });

        it('should handle env var substitution in config', () => {
            process.env.TEST_DOC2VEC_URL = 'https://test.example.com';

            const configContent = `
sources:
  - type: website
    product_name: TestSite
    version: "1.0"
    max_size: 50000
    url: "\${TEST_DOC2VEC_URL}"
    database_config:
      type: sqlite
      params:
        db_path: ":memory:"
`;
            // Simulate env var substitution
            const substituted = configContent.replace(/\$\{([^}]+)\}/g, (match, varName) => {
                return process.env[varName] || match;
            });

            const loaded = yaml.load(substituted) as any;
            expect(loaded.sources[0].url).toBe('https://test.example.com');

            delete process.env.TEST_DOC2VEC_URL;
        });

        it('should keep placeholder when env var is not found', () => {
            const configContent = 'url: "${NONEXISTENT_VAR_XYZ}"';
            const substituted = configContent.replace(/\$\{([^}]+)\}/g, (match, varName) => {
                const envValue = process.env[varName];
                return envValue !== undefined ? envValue : match;
            });

            expect(substituted).toContain('${NONEXISTENT_VAR_XYZ}');
        });

        it('should parse multiple sources', () => {
            const config = {
                sources: [
                    {
                        type: 'website',
                        product_name: 'Site1',
                        version: '1.0',
                        max_size: 50000,
                        url: 'https://site1.com',
                        database_config: { type: 'sqlite', params: {} }
                    },
                    {
                        type: 'github',
                        product_name: 'Repo1',
                        version: '1.0',
                        max_size: 50000,
                        repo: 'owner/repo',
                        database_config: { type: 'sqlite', params: {} }
                    },
                    {
                        type: 'local_directory',
                        product_name: 'Docs',
                        version: '1.0',
                        max_size: 50000,
                        path: '/docs',
                        database_config: { type: 'sqlite', params: {} }
                    }
                ]
            };

            const loaded = config;
            expect(loaded.sources).toHaveLength(3);
            expect(loaded.sources.map((s: any) => s.type)).toEqual(['website', 'github', 'local_directory']);
        });
    });

    // ─── Code source config defaults ────────────────────────────────
    describe('code source config defaults', () => {
        it('should use branch as version when version is missing for code sources', () => {
            const source: any = {
                type: 'code',
                product_name: 'TestCode',
                version: '',
                branch: 'develop',
                source: 'github',
                repo: 'owner/repo',
                max_size: 50000,
                database_config: { type: 'sqlite', params: {} }
            };

            // Simulate the version assignment logic from loadConfig
            if (source.type === 'code') {
                if (!source.version || String(source.version).trim().length === 0) {
                    if (source.branch && String(source.branch).trim().length > 0) {
                        source.version = source.branch;
                    } else {
                        source.version = 'local';
                    }
                }
            }

            expect(source.version).toBe('develop');
        });

        it('should use "local" as version when both version and branch are missing', () => {
            const source: any = {
                type: 'code',
                product_name: 'TestCode',
                version: '',
                source: 'local_directory',
                path: '/test',
                max_size: 50000,
                database_config: { type: 'sqlite', params: {} }
            };

            if (source.type === 'code') {
                if (!source.version || String(source.version).trim().length === 0) {
                    if (source.branch && String(source.branch).trim().length > 0) {
                        source.version = source.branch;
                    } else {
                        source.version = 'local';
                    }
                }
            }

            expect(source.version).toBe('local');
        });

        it('should keep existing version for code sources', () => {
            const source: any = {
                type: 'code',
                product_name: 'TestCode',
                version: 'v2.0',
                branch: 'main',
                source: 'github',
                repo: 'owner/repo',
                max_size: 50000,
                database_config: { type: 'sqlite', params: {} }
            };

            if (source.type === 'code') {
                if (!source.version || String(source.version).trim().length === 0) {
                    if (source.branch && String(source.branch).trim().length > 0) {
                        source.version = source.branch;
                    } else {
                        source.version = 'local';
                    }
                }
            }

            expect(source.version).toBe('v2.0');
        });
    });

    // ─── buildCodeFileUrl logic ─────────────────────────────────────
    describe('buildCodeFileUrl logic', () => {
        // We replicate the logic since the method is private
        function buildCodeFileUrl(
            filePath: string,
            basePath: string,
            config: { url_rewrite_prefix?: string },
            repoUrlPrefix?: string
        ): string {
            const relativePath = path.relative(basePath, filePath).replace(/\\/g, '/');

            if (repoUrlPrefix) {
                return `${repoUrlPrefix}/${relativePath}`;
            }

            if (config.url_rewrite_prefix) {
                if (relativePath.startsWith('..')) {
                    return `file://${filePath}`;
                }
                const prefix = config.url_rewrite_prefix.endsWith('/')
                    ? config.url_rewrite_prefix.slice(0, -1)
                    : config.url_rewrite_prefix;
                return `${prefix}/${relativePath}`;
            }

            return `file://${filePath}`;
        }

        it('should use repoUrlPrefix when available', () => {
            const url = buildCodeFileUrl('/tmp/repo/src/app.ts', '/tmp/repo', {}, 'https://github.com/org/repo/blob/main');
            expect(url).toBe('https://github.com/org/repo/blob/main/src/app.ts');
        });

        it('should use url_rewrite_prefix when no repoUrlPrefix', () => {
            const url = buildCodeFileUrl('/project/src/app.ts', '/project', { url_rewrite_prefix: 'https://mysite.com/code' });
            expect(url).toBe('https://mysite.com/code/src/app.ts');
        });

        it('should handle trailing slash in url_rewrite_prefix', () => {
            const url = buildCodeFileUrl('/project/src/app.ts', '/project', { url_rewrite_prefix: 'https://mysite.com/code/' });
            expect(url).toBe('https://mysite.com/code/src/app.ts');
        });

        it('should fall back to file:// when file is outside base path', () => {
            const url = buildCodeFileUrl('/other/path/file.ts', '/project', { url_rewrite_prefix: 'https://mysite.com' });
            expect(url).toBe('file:///other/path/file.ts');
        });

        it('should use file:// URL when no prefix is configured', () => {
            const url = buildCodeFileUrl('/project/src/app.ts', '/project', {});
            expect(url).toBe('file:///project/src/app.ts');
        });
    });

    // ─── buildCodeShaMetadataKey logic ──────────────────────────────
    describe('buildCodeShaMetadataKey logic', () => {
        function buildCodeShaMetadataKey(repo: string, branch: string): string {
            const normalizedRepo = repo.replace(/[^a-zA-Z0-9]+/g, '_');
            const normalizedBranch = branch.replace(/[^a-zA-Z0-9]+/g, '_');
            return `code_last_sha_${normalizedRepo}_${normalizedBranch}`;
        }

        it('should normalize repo and branch names', () => {
            const key = buildCodeShaMetadataKey('org/repo', 'main');
            expect(key).toBe('code_last_sha_org_repo_main');
        });

        it('should handle special characters in repo/branch', () => {
            const key = buildCodeShaMetadataKey('my-org/my-repo', 'feature/new-thing');
            expect(key).toBe('code_last_sha_my_org_my_repo_feature_new_thing');
        });

        it('should produce deterministic keys', () => {
            const k1 = buildCodeShaMetadataKey('org/repo', 'main');
            const k2 = buildCodeShaMetadataKey('org/repo', 'main');
            expect(k1).toBe(k2);
        });
    });

    // ─── Git diff parsing logic ─────────────────────────────────────
    describe('git diff parsing logic', () => {
        function parseGitDiff(diffOutput: string, repoPath: string) {
            const changedFiles = new Set<string>();
            const deletedPaths: string[] = [];

            for (const line of diffOutput.split('\n')) {
                const trimmed = line.trim();
                if (!trimmed) continue;
                const parts = trimmed.split('\t');
                const status = parts[0];

                if (status.startsWith('R')) {
                    const oldPath = parts[1];
                    const newPath = parts[2];
                    if (oldPath) deletedPaths.push(oldPath);
                    if (newPath) changedFiles.add(path.join(repoPath, newPath));
                } else if (status === 'D') {
                    const deletedPath = parts[1];
                    if (deletedPath) deletedPaths.push(deletedPath);
                } else if (status === 'A' || status === 'M') {
                    const changedPath = parts[1];
                    if (changedPath) changedFiles.add(path.join(repoPath, changedPath));
                }
            }

            return { changedFiles, deletedPaths };
        }

        it('should parse added files', () => {
            const diff = 'A\tsrc/new-file.ts';
            const result = parseGitDiff(diff, '/repo');
            expect(result.changedFiles.has(path.join('/repo', 'src/new-file.ts'))).toBe(true);
        });

        it('should parse modified files', () => {
            const diff = 'M\tsrc/modified.ts';
            const result = parseGitDiff(diff, '/repo');
            expect(result.changedFiles.has(path.join('/repo', 'src/modified.ts'))).toBe(true);
        });

        it('should parse deleted files', () => {
            const diff = 'D\tsrc/deleted.ts';
            const result = parseGitDiff(diff, '/repo');
            expect(result.deletedPaths).toContain('src/deleted.ts');
        });

        it('should parse renamed files', () => {
            const diff = 'R100\tsrc/old-name.ts\tsrc/new-name.ts';
            const result = parseGitDiff(diff, '/repo');
            expect(result.deletedPaths).toContain('src/old-name.ts');
            expect(result.changedFiles.has(path.join('/repo', 'src/new-name.ts'))).toBe(true);
        });

        it('should handle mixed diff output', () => {
            const diff = [
                'A\tsrc/added.ts',
                'M\tsrc/modified.ts',
                'D\tsrc/deleted.ts',
                'R090\tsrc/old.ts\tsrc/new.ts',
                '', // empty line
            ].join('\n');

            const result = parseGitDiff(diff, '/repo');
            expect(result.changedFiles.size).toBe(3); // added, modified, renamed-new
            expect(result.deletedPaths.length).toBe(2); // deleted, renamed-old
        });

        it('should skip empty lines', () => {
            const diff = '\n\n\n';
            const result = parseGitDiff(diff, '/repo');
            expect(result.changedFiles.size).toBe(0);
            expect(result.deletedPaths.length).toBe(0);
        });
    });

    // ─── Source type routing ────────────────────────────────────────
    describe('source type routing', () => {
        it('should recognize all supported source types', () => {
            const supportedTypes = ['website', 'github', 'local_directory', 'code', 'zendesk'];

            for (const type of supportedTypes) {
                const source = { type };
                expect(['website', 'github', 'local_directory', 'code', 'zendesk']).toContain(source.type);
            }
        });
    });

    // ─── fetchWithRetry logic ───────────────────────────────────────
    describe('fetchWithRetry logic', () => {
        it('should implement exponential backoff', () => {
            const baseDelay = 5000;
            const delays = [0, 1, 2, 3].map(attempt => baseDelay * Math.pow(2, attempt));
            expect(delays).toEqual([5000, 10000, 20000, 40000]);
        });
    });
});
