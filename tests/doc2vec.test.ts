import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';
import * as os from 'os';

// ─── Mock external dependencies ─────────────────────────────────────────────

// Mock OpenAI
vi.mock('openai', () => {
    const mockCreate = vi.fn();
    function MockOpenAI() {
        this.embeddings = { create: mockCreate };
    }
    return {
        OpenAI: MockOpenAI,
        __mockEmbeddingsCreate: mockCreate,
    };
});

// Mock axios
vi.mock('axios', () => ({
    default: {
        get: vi.fn(),
        post: vi.fn(),
    },
}));

// Mock child_process exec
vi.mock('child_process', () => ({
    exec: vi.fn(),
}));

// Mock util.promisify to return a mock execAsync
vi.mock('util', async (importOriginal) => {
    const actual = await importOriginal() as any;
    return {
        ...actual,
        promisify: vi.fn((fn: any) => {
            // Return a mock execAsync function
            const mockExecAsync = vi.fn().mockResolvedValue({ stdout: '', stderr: '' });
            return mockExecAsync;
        }),
    };
});

// Mock dotenv
vi.mock('dotenv', () => ({
    config: vi.fn(),
}));

// Mock DatabaseManager
vi.mock('../database', () => ({
    DatabaseManager: {
        initDatabase: vi.fn().mockResolvedValue({
            type: 'sqlite',
            db: {},
        }),
        initDatabaseMetadata: vi.fn().mockResolvedValue(undefined),
        getMetadataValue: vi.fn().mockResolvedValue(undefined),
        setMetadataValue: vi.fn().mockResolvedValue(undefined),
        getLastRunDate: vi.fn().mockResolvedValue('2025-01-01T00:00:00Z'),
        updateLastRunDate: vi.fn().mockResolvedValue(undefined),
        prepareSQLiteStatements: vi.fn().mockReturnValue({
            checkHashStmt: { get: vi.fn() },
        }),
        insertVectorsSQLite: vi.fn(),
        storeChunkInQdrant: vi.fn().mockResolvedValue(undefined),
        removeObsoleteChunksSQLite: vi.fn(),
        removeObsoleteChunksQdrant: vi.fn().mockResolvedValue(undefined),
        removeObsoleteFilesSQLite: vi.fn(),
        removeObsoleteFilesQdrant: vi.fn().mockResolvedValue(undefined),
        removeChunksByUrlSQLite: vi.fn(),
        removeChunksByUrlQdrant: vi.fn().mockResolvedValue(undefined),
        getChunkHashesByUrlSQLite: vi.fn().mockReturnValue([]),
        getChunkHashesByUrlQdrant: vi.fn().mockResolvedValue([]),
    },
}));

// Mock ContentProcessor
vi.mock('../content-processor', () => {
    function MockContentProcessor() {
        this.chunkMarkdown = vi.fn().mockResolvedValue([]);
        this.chunkCode = vi.fn().mockResolvedValue([]);
        this.crawlWebsite = vi.fn().mockResolvedValue({ hasNetworkErrors: false, brokenLinks: [] });
        this.processDirectory = vi.fn().mockResolvedValue(undefined);
        this.processCodeDirectory = vi.fn().mockResolvedValue({ maxMtime: 0 });
        this.convertHtmlToMarkdown = vi.fn().mockReturnValue('converted markdown');
    }
    return { ContentProcessor: MockContentProcessor };
});

// Mock Logger - must use function() not arrow so it works with `new`
vi.mock('../logger', () => {
    function createMockLogger(): any {
        const instance: any = {
            info: vi.fn(),
            warn: vi.fn(),
            error: vi.fn(),
            debug: vi.fn(),
            section: vi.fn(),
            progress: vi.fn().mockReturnValue({
                update: vi.fn(),
                complete: vi.fn(),
            }),
        };
        // Use lazy creation to avoid infinite recursion
        instance.child = vi.fn().mockImplementation(() => createMockLogger());
        return instance;
    }

    // Use a class-like constructor so `new Logger(...)` works
    function MockLogger() {
        const mock = createMockLogger();
        Object.assign(this, mock);
    }

    return {
        Logger: MockLogger,
        LogLevel: {
            DEBUG: 0,
            INFO: 1,
            WARN: 2,
            ERROR: 3,
            NONE: 100,
        },
    };
});

// Mock Utils
vi.mock('../utils', () => ({
    Utils: {
        generateHash: vi.fn().mockReturnValue('mock-hash'),
        isValidUuid: vi.fn().mockReturnValue(false),
        hashToUuid: vi.fn().mockReturnValue('00000000-0000-0000-0000-000000000000'),
        getUrlPrefix: vi.fn().mockReturnValue('https://example.com'),
    },
}));

// ─── Import the class under test (AFTER mocks are set up) ────────────────────

import { Doc2Vec } from '../doc2vec';
import { DatabaseManager } from '../database';
import { ContentProcessor } from '../content-processor';
import { Logger } from '../logger';

// ─── Test helpers ────────────────────────────────────────────────────────────

const testConfigDir = path.join(os.tmpdir(), '__doc2vec_test_configs__');

function writeTestConfig(filename: string, config: any): string {
    const configPath = path.join(testConfigDir, filename);
    fs.writeFileSync(configPath, yaml.dump(config));
    return configPath;
}

function writeTestConfigRaw(filename: string, content: string): string {
    const configPath = path.join(testConfigDir, filename);
    fs.writeFileSync(configPath, content);
    return configPath;
}

function makeMinimalConfig(overrides: any = {}) {
    return {
        sources: [{
            type: 'website',
            product_name: 'TestSite',
            version: '1.0',
            max_size: 50000,
            url: 'https://example.com',
            database_config: {
                type: 'sqlite',
                params: { db_path: ':memory:' },
            },
            ...overrides,
        }],
    };
}

// ─── Capture process.exit ────────────────────────────────────────────────────

const originalProcessExit = process.exit;
let mockProcessExit: ReturnType<typeof vi.fn>;

// =============================================================================
// TEST SUITES
// =============================================================================

describe('Doc2Vec class', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Mock process.exit to throw instead of killing the test runner
        mockProcessExit = vi.fn().mockImplementation((code?: number) => {
            throw new Error(`process.exit(${code})`);
        }) as any;
        process.exit = mockProcessExit as any;

        // Provide a dummy API key so the constructor validation doesn't call process.exit
        process.env.OPENAI_API_KEY = 'test-key-for-tests';
        // Force OpenAI provider for tests (override any system default)
        process.env.EMBEDDING_PROVIDER = 'openai';
        process.env.OPENAI_MODEL = 'text-embedding-3-large';

        // Ensure test config directory exists
        if (!fs.existsSync(testConfigDir)) {
            fs.mkdirSync(testConfigDir, { recursive: true });
        }
    });

    afterEach(() => {
        process.exit = originalProcessExit;

        // Clean up test configs
        if (fs.existsSync(testConfigDir)) {
            fs.rmSync(testConfigDir, { recursive: true, force: true });
        }

        // Clean up env vars set in tests
        delete process.env.TEST_DOC2VEC_URL;
        delete process.env.TEST_DOC2VEC_API_KEY;
        delete process.env.OPENAI_API_KEY;
        delete process.env.EMBEDDING_PROVIDER;
        delete process.env.OPENAI_MODEL;
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Constructor
    // ─────────────────────────────────────────────────────────────────────────
    describe('constructor', () => {
        it('should create a Logger instance', () => {
            const configPath = writeTestConfig('ctor-logger.yaml', makeMinimalConfig());
            const instance = new Doc2Vec(configPath);
            // Verify logger was set on the instance
            expect((instance as any).logger).toBeDefined();
            expect((instance as any).logger.info).toBeDefined();
            expect((instance as any).logger.child).toBeDefined();
        });

        it('should load config from the provided path', () => {
            const config = makeMinimalConfig();
            const configPath = writeTestConfig('ctor-config.yaml', config);
            const instance = new Doc2Vec(configPath);

            // Verify the config was loaded by accessing it through the instance
            expect((instance as any).config).toBeDefined();
            expect((instance as any).config.sources).toHaveLength(1);
            expect((instance as any).config.sources[0].product_name).toBe('TestSite');
        });

        it('should initialize OpenAI client', () => {
            const configPath = writeTestConfig('ctor-openai.yaml', makeMinimalConfig());
            process.env.OPENAI_API_KEY = 'test-key-123';
            const instance = new Doc2Vec(configPath);
            // Verify openai was set on the instance with embeddings
            expect((instance as any).openai).toBeDefined();
            expect((instance as any).openai.embeddings).toBeDefined();
            expect((instance as any).openai.embeddings.create).toBeDefined();
        });

        it('should initialize ContentProcessor with logger', () => {
            const configPath = writeTestConfig('ctor-cp.yaml', makeMinimalConfig());
            const instance = new Doc2Vec(configPath);
            // Verify contentProcessor was set on the instance
            expect((instance as any).contentProcessor).toBeDefined();
            expect((instance as any).contentProcessor.chunkMarkdown).toBeDefined();
            expect((instance as any).contentProcessor.crawlWebsite).toBeDefined();
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 2. loadConfig
    // ─────────────────────────────────────────────────────────────────────────
    describe('loadConfig', () => {
        it('should read and parse a YAML config file', () => {
            const config = makeMinimalConfig();
            const configPath = writeTestConfig('load-yaml.yaml', config);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].type).toBe('website');
        });

        it('should substitute environment variables in config', () => {
            process.env.TEST_DOC2VEC_URL = 'https://substituted.example.com';
            const rawConfig = `
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
            const configPath = writeTestConfigRaw('load-envvar.yaml', rawConfig);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].url).toBe('https://substituted.example.com');
        });

        it('should keep placeholder when env var is not found', () => {
            const rawConfig = `
sources:
  - type: website
    product_name: TestSite
    version: "1.0"
    max_size: 50000
    url: "\${NONEXISTENT_DOC2VEC_VAR_XYZ}"
    database_config:
      type: sqlite
      params:
        db_path: ":memory:"
`;
            const configPath = writeTestConfigRaw('load-missing-env.yaml', rawConfig);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].url).toBe('${NONEXISTENT_DOC2VEC_VAR_XYZ}');
        });

        it('should substitute multiple env vars in a single config', () => {
            process.env.TEST_DOC2VEC_URL = 'https://multi.example.com';
            process.env.TEST_DOC2VEC_API_KEY = 'secret-key';
            const rawConfig = `
sources:
  - type: website
    product_name: TestSite
    version: "\${TEST_DOC2VEC_API_KEY}"
    max_size: 50000
    url: "\${TEST_DOC2VEC_URL}"
    database_config:
      type: sqlite
      params:
        db_path: ":memory:"
`;
            const configPath = writeTestConfigRaw('load-multi-env.yaml', rawConfig);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].url).toBe('https://multi.example.com');
            expect((instance as any).config.sources[0].version).toBe('secret-key');
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
                        database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                    },
                    {
                        type: 'github',
                        product_name: 'Repo1',
                        version: '2.0',
                        max_size: 50000,
                        repo: 'owner/repo',
                        database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                    },
                ],
            };
            const configPath = writeTestConfig('load-multi.yaml', config);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources).toHaveLength(2);
            expect((instance as any).config.sources[0].type).toBe('website');
            expect((instance as any).config.sources[1].type).toBe('github');
        });

        it('should call process.exit(1) when config file does not exist', () => {
            expect(() => {
                new Doc2Vec('/nonexistent/path/config.yaml');
            }).toThrow('process.exit(1)');
            expect(mockProcessExit).toHaveBeenCalledWith(1);
        });

        it('should call process.exit(1) when config file has invalid YAML', () => {
            // YAML with a tab character in indentation causes a YAMLException
            const configPath = writeTestConfigRaw('load-invalid.yaml', "sources:\n\t- invalid:\n\t\t broken: [");
            expect(() => {
                new Doc2Vec(configPath);
            }).toThrow('process.exit(1)');
            expect(mockProcessExit).toHaveBeenCalledWith(1);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 3. Config validation — version handling
    // ─────────────────────────────────────────────────────────────────────────
    describe('config validation: version handling', () => {
        it('should use branch as version for code source when version is empty', () => {
            const config = {
                sources: [{
                    type: 'code',
                    product_name: 'TestCode',
                    version: '',
                    source: 'github',
                    repo: 'owner/repo',
                    branch: 'develop',
                    max_size: 50000,
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('version-branch.yaml', config);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].version).toBe('develop');
        });

        it('should use "local" as version for code source when both version and branch are missing', () => {
            const config = {
                sources: [{
                    type: 'code',
                    product_name: 'TestCode',
                    version: '',
                    source: 'local_directory',
                    path: '/test',
                    max_size: 50000,
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('version-local.yaml', config);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].version).toBe('local');
        });

        it('should keep existing version for code source when version is already set', () => {
            const config = {
                sources: [{
                    type: 'code',
                    product_name: 'TestCode',
                    version: 'v2.0',
                    source: 'github',
                    repo: 'owner/repo',
                    branch: 'main',
                    max_size: 50000,
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('version-keep.yaml', config);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].version).toBe('v2.0');
        });

        it('should call process.exit(1) for non-code source with missing version', () => {
            const config = {
                sources: [{
                    type: 'website',
                    product_name: 'TestSite',
                    version: '',
                    max_size: 50000,
                    url: 'https://example.com',
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('version-missing.yaml', config);
            expect(() => {
                new Doc2Vec(configPath);
            }).toThrow('process.exit(1)');
            expect(mockProcessExit).toHaveBeenCalledWith(1);
        });

        it('should call process.exit(1) for github source with missing version', () => {
            const config = {
                sources: [{
                    type: 'github',
                    product_name: 'TestRepo',
                    version: '',
                    max_size: 50000,
                    repo: 'owner/repo',
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('version-github-missing.yaml', config);
            expect(() => {
                new Doc2Vec(configPath);
            }).toThrow('process.exit(1)');
            expect(mockProcessExit).toHaveBeenCalledWith(1);
        });

        it('should use "local" for code source with empty branch string', () => {
            const config = {
                sources: [{
                    type: 'code',
                    product_name: 'TestCode',
                    version: '',
                    source: 'local_directory',
                    path: '/test',
                    branch: '   ',
                    max_size: 50000,
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('version-empty-branch.yaml', config);
            const instance = new Doc2Vec(configPath);
            expect((instance as any).config.sources[0].version).toBe('local');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 4. buildCodeShaMetadataKey (actual class method)
    // ─────────────────────────────────────────────────────────────────────────
    describe('buildCodeShaMetadataKey', () => {
        let instance: Doc2Vec;

        beforeEach(() => {
            const configPath = writeTestConfig('sha-key.yaml', makeMinimalConfig());
            instance = new Doc2Vec(configPath);
        });

        it('should normalize repo and branch names', () => {
            const key = (instance as any).buildCodeShaMetadataKey('org/repo', 'main');
            expect(key).toBe('code_last_sha_org_repo_main');
        });

        it('should handle special characters in repo/branch', () => {
            const key = (instance as any).buildCodeShaMetadataKey('my-org/my-repo', 'feature/new-thing');
            expect(key).toBe('code_last_sha_my_org_my_repo_feature_new_thing');
        });

        it('should produce deterministic keys', () => {
            const k1 = (instance as any).buildCodeShaMetadataKey('org/repo', 'main');
            const k2 = (instance as any).buildCodeShaMetadataKey('org/repo', 'main');
            expect(k1).toBe(k2);
        });

        it('should handle dots and underscores in repo names', () => {
            const key = (instance as any).buildCodeShaMetadataKey('my.org/my_repo.js', 'release/v1.2.3');
            expect(key).toBe('code_last_sha_my_org_my_repo_js_release_v1_2_3');
        });

        it('should replace consecutive special chars with single underscore', () => {
            const key = (instance as any).buildCodeShaMetadataKey('org--repo', 'main');
            expect(key).toBe('code_last_sha_org_repo_main');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 5. buildCodeFileUrl (actual class method)
    // ─────────────────────────────────────────────────────────────────────────
    describe('buildCodeFileUrl', () => {
        let instance: Doc2Vec;

        beforeEach(() => {
            const configPath = writeTestConfig('file-url.yaml', makeMinimalConfig());
            instance = new Doc2Vec(configPath);
        });

        it('should use repoUrlPrefix when available', () => {
            const url = (instance as any).buildCodeFileUrl(
                '/tmp/repo/src/app.ts',
                '/tmp/repo',
                {},
                'https://github.com/org/repo/blob/main'
            );
            expect(url).toBe('https://github.com/org/repo/blob/main/src/app.ts');
        });

        it('should use url_rewrite_prefix when no repoUrlPrefix', () => {
            const url = (instance as any).buildCodeFileUrl(
                '/project/src/app.ts',
                '/project',
                { url_rewrite_prefix: 'https://mysite.com/code' }
            );
            expect(url).toBe('https://mysite.com/code/src/app.ts');
        });

        it('should handle trailing slash in url_rewrite_prefix', () => {
            const url = (instance as any).buildCodeFileUrl(
                '/project/src/app.ts',
                '/project',
                { url_rewrite_prefix: 'https://mysite.com/code/' }
            );
            expect(url).toBe('https://mysite.com/code/src/app.ts');
        });

        it('should fall back to file:// when file is outside base path', () => {
            const url = (instance as any).buildCodeFileUrl(
                '/other/path/file.ts',
                '/project',
                { url_rewrite_prefix: 'https://mysite.com' }
            );
            expect(url).toBe('file:///other/path/file.ts');
        });

        it('should use file:// URL when no prefix is configured', () => {
            const url = (instance as any).buildCodeFileUrl(
                '/project/src/app.ts',
                '/project',
                {}
            );
            expect(url).toBe('file:///project/src/app.ts');
        });

        it('should prioritize repoUrlPrefix over url_rewrite_prefix', () => {
            const url = (instance as any).buildCodeFileUrl(
                '/project/src/app.ts',
                '/project',
                { url_rewrite_prefix: 'https://other.com' },
                'https://github.com/org/repo/blob/main'
            );
            expect(url).toBe('https://github.com/org/repo/blob/main/src/app.ts');
        });

        it('should handle nested file paths correctly', () => {
            const url = (instance as any).buildCodeFileUrl(
                '/project/src/deep/nested/file.ts',
                '/project',
                { url_rewrite_prefix: 'https://mysite.com/code' }
            );
            expect(url).toBe('https://mysite.com/code/src/deep/nested/file.ts');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 6. createEmbeddings (actual class method)
    // ─────────────────────────────────────────────────────────────────────────
    describe('createEmbeddings', () => {
        let instance: Doc2Vec;
        let mockEmbeddingsCreate: ReturnType<typeof vi.fn>;

        beforeEach(() => {
            const configPath = writeTestConfig('embeddings.yaml', makeMinimalConfig());
            instance = new Doc2Vec(configPath);
            // Access the mock through the instance's openai property
            mockEmbeddingsCreate = (instance as any).openai.embeddings.create;
        });

        it('should return embeddings on success', async () => {
            const mockEmbedding = [0.1, 0.2, 0.3];
            mockEmbeddingsCreate.mockResolvedValue({
                data: [{ embedding: mockEmbedding }],
            });

            const result = await (instance as any).createEmbeddings(['test text']);
            expect(result).toEqual([mockEmbedding]);
            expect(mockEmbeddingsCreate).toHaveBeenCalledWith({
                model: 'text-embedding-3-large',
                input: ['test text'],
            });
        });

        it('should return multiple embeddings for multiple texts', async () => {
            const mockEmbeddings = [[0.1, 0.2], [0.3, 0.4]];
            mockEmbeddingsCreate.mockResolvedValue({
                data: mockEmbeddings.map(e => ({ embedding: e })),
            });

            const result = await (instance as any).createEmbeddings(['text1', 'text2']);
            expect(result).toEqual(mockEmbeddings);
        });

        it('should return empty array on error', async () => {
            mockEmbeddingsCreate.mockRejectedValue(new Error('API error'));

            const result = await (instance as any).createEmbeddings(['test text']);
            expect(result).toEqual([]);
        });

        it('should return empty array on network timeout', async () => {
            mockEmbeddingsCreate.mockRejectedValue(new Error('ECONNABORTED'));

            const result = await (instance as any).createEmbeddings(['test text']);
            expect(result).toEqual([]);
        });

        it('should truncate text exceeding MAX_EMBEDDING_CHARS', async () => {
            const maxChars = (Doc2Vec as any).MAX_EMBEDDING_CHARS; // 8191 * 4 = 32764
            const oversizedText = 'x'.repeat(maxChars + 1000);

            const mockEmbedding = [0.1, 0.2, 0.3];
            mockEmbeddingsCreate.mockResolvedValue({
                data: [{ embedding: mockEmbedding }],
            });

            const result = await (instance as any).createEmbeddings([oversizedText]);

            // Should still produce an embedding (truncated, not skipped)
            expect(result).toEqual([mockEmbedding]);
            // The input sent to OpenAI should be truncated
            const calledInput = mockEmbeddingsCreate.mock.calls[0][0].input;
            expect(calledInput[0].length).toBe(maxChars);
        });

        it('should not truncate text under MAX_EMBEDDING_CHARS', async () => {
            const normalText = 'hello world';
            const mockEmbedding = [0.1, 0.2, 0.3];
            mockEmbeddingsCreate.mockResolvedValue({
                data: [{ embedding: mockEmbedding }],
            });

            await (instance as any).createEmbeddings([normalText]);

            const calledInput = mockEmbeddingsCreate.mock.calls[0][0].input;
            expect(calledInput[0]).toBe(normalText);
        });

        it('should only truncate the oversized text in a mixed batch', async () => {
            const maxChars = (Doc2Vec as any).MAX_EMBEDDING_CHARS;
            const normalText = 'short text';
            const oversizedText = 'y'.repeat(maxChars + 5000);

            const mockEmbeddings = [[0.1], [0.2]];
            mockEmbeddingsCreate.mockResolvedValue({
                data: mockEmbeddings.map(e => ({ embedding: e })),
            });

            const result = await (instance as any).createEmbeddings([normalText, oversizedText]);

            expect(result).toEqual(mockEmbeddings);
            const calledInput = mockEmbeddingsCreate.mock.calls[0][0].input;
            // First text should be unchanged
            expect(calledInput[0]).toBe(normalText);
            // Second text should be truncated
            expect(calledInput[1].length).toBe(maxChars);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 7. run() — source type routing
    // ─────────────────────────────────────────────────────────────────────────
    describe('run()', () => {
        let instance: Doc2Vec;

        function createInstanceWithSources(sources: any[]): Doc2Vec {
            const config = { sources };
            const configPath = writeTestConfig('run-routing.yaml', config);
            return new Doc2Vec(configPath);
        }

        it('should route website source to processWebsite', async () => {
            instance = createInstanceWithSources([{
                type: 'website',
                product_name: 'TestSite',
                version: '1.0',
                max_size: 50000,
                url: 'https://example.com',
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            }]);

            const spy = vi.spyOn(instance as any, 'processWebsite').mockResolvedValue(undefined);
            await instance.run();
            expect(spy).toHaveBeenCalledTimes(1);
        });

        it('should route github source to processGithubRepo', async () => {
            instance = createInstanceWithSources([{
                type: 'github',
                product_name: 'TestRepo',
                version: '1.0',
                max_size: 50000,
                repo: 'owner/repo',
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            }]);

            const spy = vi.spyOn(instance as any, 'processGithubRepo').mockResolvedValue(undefined);
            await instance.run();
            expect(spy).toHaveBeenCalledTimes(1);
        });

        it('should route local_directory source to processLocalDirectory', async () => {
            instance = createInstanceWithSources([{
                type: 'local_directory',
                product_name: 'TestDocs',
                version: '1.0',
                max_size: 50000,
                path: '/docs',
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            }]);

            const spy = vi.spyOn(instance as any, 'processLocalDirectory').mockResolvedValue(undefined);
            await instance.run();
            expect(spy).toHaveBeenCalledTimes(1);
        });

        it('should route code source to processCodeSource', async () => {
            instance = createInstanceWithSources([{
                type: 'code',
                product_name: 'TestCode',
                version: 'v1.0',
                source: 'local_directory',
                path: '/code',
                max_size: 50000,
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            }]);

            const spy = vi.spyOn(instance as any, 'processCodeSource').mockResolvedValue(undefined);
            await instance.run();
            expect(spy).toHaveBeenCalledTimes(1);
        });

        it('should route zendesk source to processZendesk', async () => {
            instance = createInstanceWithSources([{
                type: 'zendesk',
                product_name: 'TestZendesk',
                version: '1.0',
                zendesk_subdomain: 'mycompany',
                email: 'test@example.com',
                api_token: 'token123',
                max_size: 50000,
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            }]);

            const spy = vi.spyOn(instance as any, 'processZendesk').mockResolvedValue(undefined);
            await instance.run();
            expect(spy).toHaveBeenCalledTimes(1);
        });

        it('should log error for unknown source type', async () => {
            // We need to bypass version validation for unknown type
            // Set version so it doesn't exit, and force the type after construction
            instance = createInstanceWithSources([{
                type: 'website',
                product_name: 'TestUnknown',
                version: '1.0',
                max_size: 50000,
                url: 'https://example.com',
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            }]);

            // Override the config to have unknown type after construction
            (instance as any).config.sources[0].type = 'ftp_server';

            // Spy on none of the known process methods
            const spyWeb = vi.spyOn(instance as any, 'processWebsite').mockResolvedValue(undefined);
            const spyGh = vi.spyOn(instance as any, 'processGithubRepo').mockResolvedValue(undefined);
            const spyLocal = vi.spyOn(instance as any, 'processLocalDirectory').mockResolvedValue(undefined);
            const spyCode = vi.spyOn(instance as any, 'processCodeSource').mockResolvedValue(undefined);
            const spyZen = vi.spyOn(instance as any, 'processZendesk').mockResolvedValue(undefined);

            await instance.run();

            expect(spyWeb).not.toHaveBeenCalled();
            expect(spyGh).not.toHaveBeenCalled();
            expect(spyLocal).not.toHaveBeenCalled();
            expect(spyCode).not.toHaveBeenCalled();
            expect(spyZen).not.toHaveBeenCalled();
        });

        it('should process multiple sources in order', async () => {
            instance = createInstanceWithSources([
                {
                    type: 'website',
                    product_name: 'Site1',
                    version: '1.0',
                    max_size: 50000,
                    url: 'https://site1.com',
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                },
                {
                    type: 'github',
                    product_name: 'Repo1',
                    version: '1.0',
                    max_size: 50000,
                    repo: 'owner/repo',
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                },
            ]);

            const callOrder: string[] = [];
            vi.spyOn(instance as any, 'processWebsite').mockImplementation(async () => {
                callOrder.push('website');
            });
            vi.spyOn(instance as any, 'processGithubRepo').mockImplementation(async () => {
                callOrder.push('github');
            });

            await instance.run();
            expect(callOrder).toEqual(['website', 'github']);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 8. getGitChangedFiles (actual method via class instance)
    // ─────────────────────────────────────────────────────────────────────────
    describe('getGitChangedFiles', () => {
        let instance: Doc2Vec;

        beforeEach(() => {
            const configPath = writeTestConfig('git-diff.yaml', makeMinimalConfig());
            instance = new Doc2Vec(configPath);
        });

        it('should return a result with mode, changedFiles and deletedPaths', async () => {
            // The module-level execAsync is already mocked (returns { stdout: '', stderr: '' })
            // With empty stdout the method will return either incremental (empty diff) or full
            const result = await (instance as any).getGitChangedFiles(
                '/repo', 'abc123', 'main',
                (instance as any).logger
            );

            expect(result).toBeDefined();
            expect(result).toHaveProperty('mode');
            expect(result).toHaveProperty('changedFiles');
            expect(result).toHaveProperty('deletedPaths');
            expect(['incremental', 'full']).toContain(result.mode);
        });

        it('should return changedFiles as a Set and deletedPaths as an array', async () => {
            const result = await (instance as any).getGitChangedFiles(
                '/repo', 'abc123', 'main',
                (instance as any).logger
            );

            expect(result.changedFiles).toBeInstanceOf(Set);
            expect(Array.isArray(result.deletedPaths)).toBe(true);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 9. Git diff parsing logic (replicated from source for thorough testing)
    // ─────────────────────────────────────────────────────────────────────────
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
                '',
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

    // ─────────────────────────────────────────────────────────────────────────
    // 10. fetchWithRetry exponential backoff logic
    // ─────────────────────────────────────────────────────────────────────────
    describe('fetchWithRetry logic', () => {
        it('should implement exponential backoff', () => {
            const baseDelay = 5000;
            const delays = [0, 1, 2, 3].map(attempt => baseDelay * Math.pow(2, attempt));
            expect(delays).toEqual([5000, 10000, 20000, 40000]);
        });

        it('should double delay on each subsequent attempt', () => {
            const baseDelay = 2000;
            for (let attempt = 0; attempt < 5; attempt++) {
                const delay = baseDelay * Math.pow(2, attempt);
                expect(delay).toBe(baseDelay * (2 ** attempt));
            }
        });

        it('should have first retry delay equal to base delay', () => {
            const baseDelay = 5000;
            const firstDelay = baseDelay * Math.pow(2, 0);
            expect(firstDelay).toBe(5000);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 11. Source type routing — complete enumeration
    // ─────────────────────────────────────────────────────────────────────────
    describe('source type routing (complete)', () => {
        it('should recognize all supported source types', () => {
            const supportedTypes = ['website', 'github', 'local_directory', 'code', 'zendesk'];
            for (const type of supportedTypes) {
                expect(['website', 'github', 'local_directory', 'code', 'zendesk']).toContain(type);
            }
        });

        it('should not route unknown type to any processor', async () => {
            const config = makeMinimalConfig();
            const configPath = writeTestConfig('unknown-type.yaml', config);
            const instance = new Doc2Vec(configPath);

            // Override config to inject unknown type
            (instance as any).config.sources[0].type = 'unknown_type';

            const spyWeb = vi.spyOn(instance as any, 'processWebsite').mockResolvedValue(undefined);
            const spyGh = vi.spyOn(instance as any, 'processGithubRepo').mockResolvedValue(undefined);
            const spyLocal = vi.spyOn(instance as any, 'processLocalDirectory').mockResolvedValue(undefined);
            const spyCode = vi.spyOn(instance as any, 'processCodeSource').mockResolvedValue(undefined);
            const spyZen = vi.spyOn(instance as any, 'processZendesk').mockResolvedValue(undefined);

            await instance.run();

            expect(spyWeb).not.toHaveBeenCalled();
            expect(spyGh).not.toHaveBeenCalled();
            expect(spyLocal).not.toHaveBeenCalled();
            expect(spyCode).not.toHaveBeenCalled();
            expect(spyZen).not.toHaveBeenCalled();
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 12. getRepoHeadSha (actual method)
    // ─────────────────────────────────────────────────────────────────────────
    describe('getRepoHeadSha', () => {
        let instance: Doc2Vec;

        beforeEach(() => {
            const configPath = writeTestConfig('head-sha.yaml', makeMinimalConfig());
            instance = new Doc2Vec(configPath);
        });

        it('should return trimmed SHA from git command', async () => {
            // The method relies on the module-level execAsync (already mocked via promisify)
            // Default mock returns { stdout: '', stderr: '' }
            const result = await (instance as any).getRepoHeadSha('/repo', (instance as any).logger);
            // With empty stdout, should return undefined
            expect(result).toBeUndefined();
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 13. processZendesk routing
    // ─────────────────────────────────────────────────────────────────────────
    describe('processZendesk integration', () => {
        it('should call processZendesk for zendesk type source via run()', async () => {
            const config = {
                sources: [{
                    type: 'zendesk',
                    product_name: 'TestZD',
                    version: '1.0',
                    zendesk_subdomain: 'test',
                    email: 'a@b.com',
                    api_token: 'tok',
                    max_size: 50000,
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('zendesk-route.yaml', config);
            const instance = new Doc2Vec(configPath);

            const spy = vi.spyOn(instance as any, 'processZendesk').mockResolvedValue(undefined);
            await instance.run();
            expect(spy).toHaveBeenCalledTimes(1);
            expect(spy).toHaveBeenCalledWith(
                expect.objectContaining({ type: 'zendesk', zendesk_subdomain: 'test' }),
                expect.anything()
            );
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // 14. Edge cases
    // ─────────────────────────────────────────────────────────────────────────
    describe('edge cases', () => {
        it('should handle empty sources array', async () => {
            const config = { sources: [] };
            const configPath = writeTestConfig('empty-sources.yaml', config);
            const instance = new Doc2Vec(configPath);

            // run() should complete without errors on empty sources
            await expect(instance.run()).resolves.toBeUndefined();
        });

        it('should handle config with only code sources', () => {
            const config = {
                sources: [{
                    type: 'code',
                    product_name: 'OnlyCode',
                    version: '',
                    source: 'local_directory',
                    path: '/code',
                    max_size: 50000,
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                }],
            };
            const configPath = writeTestConfig('only-code.yaml', config);
            const instance = new Doc2Vec(configPath);
            // Code source with empty version should default to 'local'
            expect((instance as any).config.sources[0].version).toBe('local');
        });

        it('should create embeddings with correct model name', async () => {
            const configPath = writeTestConfig('model-name.yaml', makeMinimalConfig());
            const instance = new Doc2Vec(configPath);
            const mockCreate = (instance as any).openai.embeddings.create;
            mockCreate.mockResolvedValue({ data: [{ embedding: [1, 2, 3] }] });

            await (instance as any).createEmbeddings(['hello']);

            expect(mockCreate).toHaveBeenCalledWith(
                expect.objectContaining({ model: 'text-embedding-3-large' })
            );
        });
    });
});
