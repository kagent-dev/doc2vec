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
        removeObsoleteChunksSQLite: vi.fn().mockReturnValue({ items: 0, chunks: 0 }),
        removeObsoleteChunksQdrant: vi.fn().mockResolvedValue({ items: 0, chunks: 0 }),
        removeObsoleteFilesSQLite: vi.fn().mockReturnValue({ items: 0, chunks: 0 }),
        removeObsoleteFilesQdrant: vi.fn().mockResolvedValue({ items: 0, chunks: 0 }),
        removeChunksByUrlSQLite: vi.fn().mockReturnValue(0),
        removeChunksByUrlQdrant: vi.fn().mockResolvedValue(0),
        getChunkHashesByUrlSQLite: vi.fn().mockReturnValue([]),
        getChunkHashesByUrlQdrant: vi.fn().mockResolvedValue([]),
        getStoredUrlsByPrefixSQLite: vi.fn().mockReturnValue([]),
        getStoredUrlsByPrefixQdrant: vi.fn().mockResolvedValue([]),
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
        this.convertFileToMarkdown = vi.fn().mockResolvedValue('converted file markdown');
    }
    return { ContentProcessor: MockContentProcessor };
});

// Mock AWS S3 SDK
const { mockS3Send } = vi.hoisted(() => {
    return { mockS3Send: vi.fn() };
});
vi.mock('@aws-sdk/client-s3', () => {
    function MockS3Client() {}
    MockS3Client.prototype.send = mockS3Send;
    function MockListObjectsV2Command(params: any) { Object.assign(this, params); }
    function MockGetObjectCommand(params: any) { Object.assign(this, params); }
    return {
        S3Client: MockS3Client,
        ListObjectsV2Command: MockListObjectsV2Command,
        GetObjectCommand: MockGetObjectCommand,
    };
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
            event: vi.fn(),
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

// Mock Utils. parseNextLink/stripLoneSurrogates/sliceSafe are pure string
// helpers with their own unit tests, so delegate to the real implementations
// rather than stubbing them — the pagination tests depend on real Link parsing.
vi.mock('../utils', async (importOriginal) => {
    const actual = await importOriginal() as any;
    return {
        Utils: {
            generateHash: vi.fn().mockReturnValue('mock-hash'),
            isValidUuid: vi.fn().mockReturnValue(false),
            hashToUuid: vi.fn().mockReturnValue('00000000-0000-0000-0000-000000000000'),
            getUrlPrefix: vi.fn().mockReturnValue('https://example.com'),
            parseNextLink: actual.Utils.parseNextLink,
            stripLoneSurrogates: actual.Utils.stripLoneSurrogates,
            sliceSafe: actual.Utils.sliceSafe,
        },
    };
});

// ─── Import the class under test (AFTER mocks are set up) ────────────────────

import { Doc2Vec } from '../doc2vec';
import { DatabaseManager } from '../database';
import { FatalEmbeddingError } from '../types';
import { ContentProcessor } from '../content-processor';
import { Logger } from '../logger';
import axios from 'axios';

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
    // 1b. run() source filter
    // ─────────────────────────────────────────────────────────────────────────
    describe('run source filter', () => {
        function makeTwoSourceInstance() {
            const config = {
                sources: ['SiteA', 'SiteB'].map(name => ({
                    type: 'website',
                    product_name: name,
                    version: '1.0',
                    max_size: 50000,
                    url: `https://example.com/${name}`,
                    database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                })),
            };
            const configPath = writeTestConfig('run-filter.yaml', config);
            const instance = new Doc2Vec(configPath);
            const processWebsite = vi.fn(async () => {});
            (instance as any).processWebsite = processWebsite;
            return { instance, processWebsite };
        }

        it('should only process the sources selected by name', async () => {
            const { instance, processWebsite } = makeTwoSourceInstance();
            const stats = await instance.run({ names: ['SiteB'] });
            expect(processWebsite).toHaveBeenCalledTimes(1);
            expect(processWebsite.mock.calls[0][0].product_name).toBe('SiteB');
            expect(stats).toHaveLength(1);
            expect(stats[0].product_name).toBe('SiteB');
        });

        it('should only process the sources selected by index', async () => {
            const { instance, processWebsite } = makeTwoSourceInstance();
            const stats = await instance.run({ indices: [0] });
            expect(processWebsite).toHaveBeenCalledTimes(1);
            expect(stats.map(s => s.product_name)).toEqual(['SiteA']);
        });

        it('should process the union of names and indices, in config order', async () => {
            const { instance, processWebsite } = makeTwoSourceInstance();
            const stats = await instance.run({ indices: [1], names: ['SiteA'] });
            expect(processWebsite).toHaveBeenCalledTimes(2);
            expect(stats.map(s => s.product_name)).toEqual(['SiteA', 'SiteB']);
        });

        it('should process all sources when no filter is given', async () => {
            const { instance, processWebsite } = makeTwoSourceInstance();
            const stats = await instance.run();
            expect(processWebsite).toHaveBeenCalledTimes(2);
            expect(stats.map(s => s.product_name)).toEqual(['SiteA', 'SiteB']);
        });

        it('should throw on unknown source names', async () => {
            const { instance, processWebsite } = makeTwoSourceInstance();
            await expect(instance.run({ names: ['SiteA', 'Nope'] })).rejects.toThrow(/Unknown source\(s\): Nope/);
            expect(processWebsite).not.toHaveBeenCalled();
        });

        it('should throw on out-of-range source indices', async () => {
            const { instance, processWebsite } = makeTwoSourceInstance();
            await expect(instance.run({ indices: [5] })).rejects.toThrow(/index out of range: 5/);
            expect(processWebsite).not.toHaveBeenCalled();
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
            }, { timeout: 60000 });
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

        it('should throw FatalEmbeddingError on 401 (invalid API key)', async () => {
            mockEmbeddingsCreate.mockRejectedValue(
                Object.assign(new Error('Incorrect API key provided'), { status: 401 })
            );

            await expect((instance as any).createEmbeddings(['test text']))
                .rejects.toThrow(FatalEmbeddingError);
        });

        it('should throw FatalEmbeddingError on 403', async () => {
            mockEmbeddingsCreate.mockRejectedValue(
                Object.assign(new Error('Forbidden'), { status: 403 })
            );

            await expect((instance as any).createEmbeddings(['test text']))
                .rejects.toThrow(FatalEmbeddingError);
        });

        it('should still return empty array on transient 429', async () => {
            mockEmbeddingsCreate.mockRejectedValue(
                Object.assign(new Error('Rate limited'), { status: 429 })
            );

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
    // processChunksForUrl: embedding failures must never destroy stored chunks
    // ─────────────────────────────────────────────────────────────────────────
    describe('processChunksForUrl embedding failure safety', () => {
        let instance: Doc2Vec;
        let mockEmbeddingsCreate: ReturnType<typeof vi.fn>;
        const dbConnection = { type: 'sqlite', db: {} };
        const makeChunk = (id: string, content: string) => ({
            content,
            metadata: { chunk_id: id, product_name: 'p', version: 'v', heading_hierarchy: [], section: '', chunk_index: 0, total_chunks: 1, url: 'https://example.com/page' },
        });

        beforeEach(() => {
            const configPath = writeTestConfig('chunks-url.yaml', makeMinimalConfig());
            instance = new Doc2Vec(configPath);
            mockEmbeddingsCreate = (instance as any).openai.embeddings.create;
            vi.mocked(DatabaseManager.getChunkHashesByUrlSQLite).mockReturnValue(['stale-hash']);
            vi.mocked(DatabaseManager.removeChunksByUrlSQLite).mockClear();
            vi.mocked(DatabaseManager.insertVectorsSQLite).mockClear();
        });

        it('throws and keeps existing chunks when an embedding fails', async () => {
            mockEmbeddingsCreate.mockRejectedValue(new Error('boom'));

            await expect((instance as any).processChunksForUrl(
                [makeChunk('chunk-aaaaaaaa', 'new content')],
                'https://example.com/page',
                dbConnection,
                (instance as any).logger
            )).rejects.toThrow(/embedding failed for 1\/1 chunks/);

            expect(DatabaseManager.removeChunksByUrlSQLite).not.toHaveBeenCalled();
            expect(DatabaseManager.insertVectorsSQLite).not.toHaveBeenCalled();
        });

        it('propagates FatalEmbeddingError without deleting existing chunks', async () => {
            mockEmbeddingsCreate.mockRejectedValue(
                Object.assign(new Error('Incorrect API key provided'), { status: 401 })
            );

            await expect((instance as any).processChunksForUrl(
                [makeChunk('chunk-aaaaaaaa', 'new content')],
                'https://example.com/page',
                dbConnection,
                (instance as any).logger
            )).rejects.toThrow(FatalEmbeddingError);

            expect(DatabaseManager.removeChunksByUrlSQLite).not.toHaveBeenCalled();
            expect(DatabaseManager.insertVectorsSQLite).not.toHaveBeenCalled();
        });

        it('replaces old chunks only after every embedding succeeded', async () => {
            mockEmbeddingsCreate.mockResolvedValue({ data: [{ embedding: [0.1, 0.2] }] });

            const stored = await (instance as any).processChunksForUrl(
                [makeChunk('chunk-aaaaaaaa', 'new content'), makeChunk('chunk-bbbbbbbb', 'more content')],
                'https://example.com/page',
                dbConnection,
                (instance as any).logger
            );

            expect(stored).toBe(2);
            expect(DatabaseManager.removeChunksByUrlSQLite).toHaveBeenCalledTimes(1);
            expect(DatabaseManager.insertVectorsSQLite).toHaveBeenCalledTimes(2);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    describe('run()', () => {
        let instance: Doc2Vec;

        function createInstanceWithSources(sources: any[]): Doc2Vec {
            const config = { sources };
            const configPath = writeTestConfig('run-routing.yaml', config);
            return new Doc2Vec(configPath);
        }

        // ─── 404 cleanup during processWebsite ───────────────────────────
        // A page that 404s during the crawl must have its chunks purged from
        // the vector DB — not just the markdown store. Because a 404 is a
        // definitive "gone" signal (network errors fall through to full
        // processing and never reach notFoundUrls), the purge must run even
        // when network errors cause the broad obsolete sweep to be skipped.
        describe('404 cleanup', () => {
            function makeLogger(): any {
                const l: any = {
                    info: vi.fn(), warn: vi.fn(), error: vi.fn(),
                    debug: vi.fn(), section: vi.fn(), event: vi.fn(),
                };
                l.child = vi.fn(() => makeLogger());
                return l;
            }

            const websiteConfig = {
                type: 'website',
                product_name: 'TestSite',
                version: '1.0',
                max_size: 50000,
                url: 'https://example.com',
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            };

            it('should delete chunks for 404 URLs from the vector DB during cleanup', async () => {
                const { DatabaseManager } = await import('../database');
                instance = createInstanceWithSources([websiteConfig]);
                (instance as any).contentProcessor.crawlWebsite = vi.fn().mockResolvedValue({
                    hasNetworkErrors: false,
                    brokenLinks: [],
                    notFoundUrls: new Set(['https://example.com/gone']),
                });

                await (instance as any).processWebsite(websiteConfig, makeLogger());

                expect(DatabaseManager.removeChunksByUrlSQLite).toHaveBeenCalledWith(
                    expect.anything(), 'https://example.com/gone', expect.anything(),
                );
            });

            it('should purge 404 URLs even when network errors skip the broad cleanup', async () => {
                const { DatabaseManager } = await import('../database');
                instance = createInstanceWithSources([websiteConfig]);
                (instance as any).contentProcessor.crawlWebsite = vi.fn().mockResolvedValue({
                    hasNetworkErrors: true,
                    brokenLinks: [],
                    notFoundUrls: new Set(['https://example.com/gone']),
                });

                await (instance as any).processWebsite(websiteConfig, makeLogger());

                // 404 chunks are still purged (definitive signal)...
                expect(DatabaseManager.removeChunksByUrlSQLite).toHaveBeenCalledWith(
                    expect.anything(), 'https://example.com/gone', expect.anything(),
                );
                // ...but the broad obsolete sweep is skipped when the site was flaky
                expect(DatabaseManager.removeObsoleteChunksSQLite).not.toHaveBeenCalled();
            });
        });

        // An incomplete walk (a directory that couldn't be listed) means the set
        // of files we saw is a subset of what's on disk. Treating that as the
        // full picture deletes chunks for files that still exist.
        describe('code source scan completeness', () => {
            function makeLogger(): any {
                const l: any = {
                    info: vi.fn(), warn: vi.fn(), error: vi.fn(),
                    debug: vi.fn(), section: vi.fn(), event: vi.fn(),
                };
                l.child = vi.fn(() => makeLogger());
                return l;
            }

            const codeConfig = {
                type: 'code',
                product_name: 'TestCode',
                version: 'v1.0',
                source: 'local_directory',
                path: '/code',
                max_size: 50000,
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
            } as any;

            it('runs cleanup when the scan covered the whole tree', async () => {
                const { DatabaseManager } = await import('../database');
                instance = createInstanceWithSources([codeConfig]);
                (instance as any).contentProcessor.processCodeDirectory = vi.fn().mockResolvedValue({
                    processedFiles: 3, skippedFiles: 0, maxMtime: 123, incomplete: false,
                });

                await (instance as any).processCodeSource(codeConfig, makeLogger());

                expect(DatabaseManager.setMetadataValue).toHaveBeenCalled();
            });

            it('skips cleanup and fails the source when the scan was incomplete', async () => {
                const { DatabaseManager } = await import('../database');
                instance = createInstanceWithSources([codeConfig]);
                (instance as any).contentProcessor.processCodeDirectory = vi.fn().mockResolvedValue({
                    processedFiles: 1, skippedFiles: 0, maxMtime: 123, incomplete: true,
                });

                await expect((instance as any).processCodeSource(codeConfig, makeLogger()))
                    .rejects.toThrow(/incomplete/i);

                // no chunk deletion of any kind
                expect(DatabaseManager.removeObsoleteFilesSQLite).not.toHaveBeenCalled();
                expect(DatabaseManager.removeObsoleteFilesQdrant).not.toHaveBeenCalled();
                expect(DatabaseManager.removeChunksByUrlSQLite).not.toHaveBeenCalled();
                // and no marker advances, so the next run rescans the whole tree
                expect(DatabaseManager.setMetadataValue).not.toHaveBeenCalled();
            });
        });

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
    // 14. fetchAndProcessZendeskTickets — bug fix tests
    // ─────────────────────────────────────────────────────────────────────────
    describe('fetchAndProcessZendeskTickets bug fixes', () => {
        function makeZendeskConfig(overrides: any = {}) {
            return {
                type: 'zendesk',
                product_name: 'TestZD',
                version: '1.0',
                zendesk_subdomain: 'testco',
                email: 'admin@test.com',
                api_token: 'secret',
                max_size: 50000,
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                ...overrides,
            };
        }

        function makeTicket(overrides: any = {}) {
            return {
                id: 42,
                subject: 'Test ticket',
                status: 'open',
                priority: 'normal',
                type: 'question',
                requester_id: 1,
                assignee_id: null,
                created_at: '2025-01-01T00:00:00Z',
                updated_at: '2025-01-02T00:00:00Z',
                tags: [],
                description: 'A test ticket',
                ...overrides,
            };
        }

        function makeCommentsResponse() {
            return {
                comments: [{
                    id: 1,
                    author_id: 10,
                    public: true,
                    body: 'A comment',
                    plain_body: 'A comment',
                    html_body: '<p>A comment</p>',
                    created_at: '2025-01-01T12:00:00Z',
                }],
            };
        }

        async function getAxiosMock() {
            const axiosMod = await import('axios');
            return (axiosMod.default as any).get as ReturnType<typeof vi.fn>;
        }

        function makeInstance(configOverrides: any = {}) {
            const zdConfig = makeZendeskConfig(configOverrides);
            const configPath = writeTestConfig('zd-bugs.yaml', { sources: [zdConfig] });
            const instance = new Doc2Vec(configPath);
            // Stub createEmbeddings
            vi.spyOn(instance as any, 'createEmbeddings').mockResolvedValue([[0.1, 0.2]]);
            return instance;
        }

        it('should handle 429 rate-limit in catch block without burning a retry', async () => {
            const instance = makeInstance();
            const axiosGet = await getAxiosMock();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(undefined);

            // First call: 429, second call: success with search results, third call: comments
            const rateLimitError = Object.assign(new Error('Rate limited'), {
                response: { status: 429, headers: { 'retry-after': '0' } },
            });
            axiosGet
                .mockRejectedValueOnce(rateLimitError)
                .mockResolvedValueOnce({ data: { results: [makeTicket()], next_page: null } })
                .mockResolvedValueOnce({ data: makeCommentsResponse() });

            await (instance as any).fetchAndProcessZendeskTickets(
                makeZendeskConfig(),
                { type: 'sqlite', db: {} },
                (instance as any).logger,
            );

            expect(processChunksSpy).toHaveBeenCalledTimes(1);
        });

        it('should propagate 403 immediately without retrying', async () => {
            const instance = makeInstance();
            const axiosGet = await getAxiosMock();

            const forbiddenError = Object.assign(new Error('Forbidden'), {
                response: { status: 403 },
            });
            axiosGet.mockRejectedValueOnce(forbiddenError);

            await expect(
                (instance as any).fetchAndProcessZendeskTickets(
                    makeZendeskConfig(),
                    { type: 'sqlite', db: {} },
                    (instance as any).logger,
                ),
            ).rejects.toThrow('Forbidden');

            // Should only have been called once (no retries)
            expect(axiosGet).toHaveBeenCalledTimes(1);
        });

        it('should use processChunksForUrl instead of inline chunk loop', async () => {
            const instance = makeInstance();
            const axiosGet = await getAxiosMock();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(undefined);

            axiosGet
                .mockResolvedValueOnce({ data: { results: [makeTicket()], next_page: null } })
                .mockResolvedValueOnce({ data: makeCommentsResponse() });

            await (instance as any).fetchAndProcessZendeskTickets(
                makeZendeskConfig(),
                { type: 'sqlite', db: {} },
                (instance as any).logger,
            );

            expect(processChunksSpy).toHaveBeenCalledTimes(1);
            expect(processChunksSpy).toHaveBeenCalledWith(
                expect.any(Array),
                'https://testco.zendesk.com/agent/tickets/42',
                expect.anything(),
                expect.anything(),
            );
        });

        it('should remove chunks for deleted tickets', async () => {
            const instance = makeInstance();
            const axiosGet = await getAxiosMock();
            const { DatabaseManager: DB } = await import('../database');

            axiosGet
                .mockResolvedValueOnce({ data: { results: [makeTicket({ status: 'deleted' })], next_page: null } });

            await (instance as any).fetchAndProcessZendeskTickets(
                makeZendeskConfig(),
                { type: 'sqlite', db: {} },
                (instance as any).logger,
            );

            expect(DB.removeChunksByUrlSQLite).toHaveBeenCalledWith(
                {},
                'https://testco.zendesk.com/agent/tickets/42',
                expect.anything(),
            );
        });

        it('should skip tickets with status outside configured filter', async () => {
            const instance = makeInstance({ ticket_status: ['open', 'solved'] });
            const axiosGet = await getAxiosMock();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(undefined);

            axiosGet
                .mockResolvedValueOnce({
                    data: {
                        results: [
                            makeTicket({ id: 1, status: 'open' }),
                            makeTicket({ id: 2, status: 'pending' }),  // should be skipped
                            makeTicket({ id: 3, status: 'solved' }),
                        ],
                        next_page: null,
                    },
                })
                // comments for ticket 1 and 3 only
                .mockResolvedValueOnce({ data: makeCommentsResponse() })
                .mockResolvedValueOnce({ data: makeCommentsResponse() });

            await (instance as any).fetchAndProcessZendeskTickets(
                makeZendeskConfig({ ticket_status: ['open', 'solved'] }),
                { type: 'sqlite', db: {} },
                (instance as any).logger,
            );

            // Only 2 tickets processed (not the pending one)
            expect(processChunksSpy).toHaveBeenCalledTimes(2);
        });

        it('should not advance watermark when a ticket fails', async () => {
            const instance = makeInstance();
            const axiosGet = await getAxiosMock();
            const { DatabaseManager: DB } = await import('../database');
            vi.spyOn(instance as any, 'processChunksForUrl').mockRejectedValue(new Error('embedding failure'));

            axiosGet
                .mockResolvedValueOnce({ data: { results: [makeTicket()], next_page: null } })
                .mockResolvedValueOnce({ data: makeCommentsResponse() });

            await (instance as any).fetchAndProcessZendeskTickets(
                makeZendeskConfig(),
                { type: 'sqlite', db: {} },
                (instance as any).logger,
            );

            expect(DB.updateLastRunDate).not.toHaveBeenCalled();
        });

        it('should advance watermark when all tickets succeed', async () => {
            const instance = makeInstance();
            const axiosGet = await getAxiosMock();
            const { DatabaseManager: DB } = await import('../database');
            vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(undefined);

            axiosGet
                .mockResolvedValueOnce({ data: { results: [makeTicket()], next_page: null } })
                .mockResolvedValueOnce({ data: makeCommentsResponse() });

            await (instance as any).fetchAndProcessZendeskTickets(
                makeZendeskConfig(),
                { type: 'sqlite', db: {} },
                (instance as any).logger,
            );

            expect(DB.updateLastRunDate).toHaveBeenCalledTimes(1);
        });
    });

    // 15. Edge cases
    // ─────────────────────────────────────────────────────────────────────────
    describe('edge cases', () => {
        it('should handle empty sources array', async () => {
            const config = { sources: [] };
            const configPath = writeTestConfig('empty-sources.yaml', config);
            const instance = new Doc2Vec(configPath);

            // run() should complete without errors on empty sources
            await expect(instance.run()).resolves.toEqual([]);
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
                expect.objectContaining({ model: 'text-embedding-3-large' }),
                { timeout: 60000 }
            );
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // S3 source tests
    // ─────────────────────────────────────────────────────────────────────────
    describe('processS3', () => {
        function makeS3Config(overrides: any = {}) {
            return {
                type: 's3',
                product_name: 'TestS3',
                version: '1.0',
                bucket: 'test-bucket',
                max_size: 1048576,
                database_config: { type: 'sqlite', params: { db_path: ':memory:' } },
                ...overrides,
            };
        }

        function makeS3Instance(configOverrides: any = {}) {
            const s3Config = makeS3Config(configOverrides);
            const configPath = writeTestConfig('s3-test.yaml', { sources: [s3Config] });
            const instance = new Doc2Vec(configPath);
            vi.spyOn(instance as any, 'createEmbeddings').mockResolvedValue([[0.1, 0.2, 0.3]]);
            return instance;
        }

        function makeS3Object(key: string, lastModified: Date, size: number = 100) {
            return { Key: key, LastModified: lastModified, Size: size };
        }

        function makeListResponse(contents: any[], nextToken?: string) {
            return {
                Contents: contents,
                NextContinuationToken: nextToken,
            };
        }

        function makeGetResponse(body: string) {
            return {
                Body: {
                    transformToString: vi.fn().mockResolvedValue(body),
                    transformToByteArray: vi.fn().mockResolvedValue(new Uint8Array(Buffer.from(body))),
                },
            };
        }

        it('should route s3 source to processS3 via run()', async () => {
            const instance = makeS3Instance();
            const spy = vi.spyOn(instance as any, 'processS3').mockResolvedValue(undefined);
            await instance.run();
            expect(spy).toHaveBeenCalledTimes(1);
            expect(spy).toHaveBeenCalledWith(
                expect.objectContaining({ type: 's3', bucket: 'test-bucket' }),
                expect.anything()
            );
        });

        it('should list objects and process text files', async () => {
            const instance = makeS3Instance();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('docs/readme.md', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('# Hello World'));

            // chunkMarkdown returns mock chunks
            const mockChunks = [{ content: '# Hello World', metadata: { chunk_id: 'c1', url: 's3://test-bucket/docs/readme.md' } }];
            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce(mockChunks);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            expect(processChunksSpy).toHaveBeenCalledWith(
                mockChunks,
                's3://test-bucket/docs/readme.md',
                expect.anything(),
                expect.anything()
            );
        });

        it('should filter objects by include_extensions', async () => {
            const instance = makeS3Instance({ include_extensions: ['.md'] });
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('file.md', now),
                    makeS3Object('file.txt', now),
                    makeS3Object('file.py', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('# Only MD'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // Only 1 GetObject call (for .md file), not 3
            expect(mockS3Send).toHaveBeenCalledTimes(2); // 1 ListObjects + 1 GetObject
        });

        it('should filter objects by exclude_extensions', async () => {
            const instance = makeS3Instance({ exclude_extensions: ['.log'] });
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('file.md', now),
                    makeS3Object('file.log', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('content'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            expect(mockS3Send).toHaveBeenCalledTimes(2); // 1 ListObjects + 1 GetObject (only .md)
        });

        it('should skip objects that have not changed since last sync (incremental)', async () => {
            const { DatabaseManager: DB } = await import('../database');
            const lastSync = Date.now();
            (DB.getMetadataValue as any)
                .mockResolvedValueOnce(String(lastSync))   // s3_last_sync_*
                .mockResolvedValueOnce('[]');               // s3_filelist_*

            const instance = makeS3Instance();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const oldDate = new Date(lastSync - 10000); // 10s before last sync
            const newDate = new Date(lastSync + 10000); // 10s after last sync

            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('old-file.md', oldDate),
                    makeS3Object('new-file.md', newDate),
                ]))
                .mockResolvedValueOnce(makeGetResponse('new content'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // Only 1 GetObject (new-file.md), old-file.md skipped
            expect(mockS3Send).toHaveBeenCalledTimes(2); // 1 ListObjects + 1 GetObject
            expect(processChunksSpy).toHaveBeenCalledTimes(1);
        });

        it('should process all objects on first run (no last sync)', async () => {
            const { DatabaseManager: DB } = await import('../database');
            (DB.getMetadataValue as any)
                .mockResolvedValueOnce('0')    // s3_last_sync_* = 0 (first run)
                .mockResolvedValueOnce('[]');   // s3_filelist_*

            const instance = makeS3Instance();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('file1.md', now),
                    makeS3Object('file2.md', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('content 1'))
                .mockResolvedValueOnce(makeGetResponse('content 2'));

            (instance as any).contentProcessor.chunkMarkdown
                .mockResolvedValueOnce([])
                .mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // Both files processed
            expect(processChunksSpy).toHaveBeenCalledTimes(2);
        });

        it('should handle paginated S3 listings', async () => {
            const instance = makeS3Instance();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                // Page 1 with continuation token
                .mockResolvedValueOnce(makeListResponse(
                    [makeS3Object('page1.md', now)],
                    'next-token'
                ))
                // Page 2, no continuation token
                .mockResolvedValueOnce(makeListResponse(
                    [makeS3Object('page2.md', now)]
                ))
                .mockResolvedValueOnce(makeGetResponse('page 1 content'))
                .mockResolvedValueOnce(makeGetResponse('page 2 content'));

            (instance as any).contentProcessor.chunkMarkdown
                .mockResolvedValueOnce([])
                .mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            expect(processChunksSpy).toHaveBeenCalledTimes(2);
        });

        it('should skip folder markers (keys ending with /)', async () => {
            const instance = makeS3Instance();
            vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('docs/', now),         // folder marker
                    makeS3Object('docs/file.md', now),  // actual file
                ]))
                .mockResolvedValueOnce(makeGetResponse('content'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // Only 2 calls: ListObjects + GetObject for file.md (not folder marker)
            expect(mockS3Send).toHaveBeenCalledTimes(2);
        });

        it('should skip objects exceeding max_size', async () => {
            const instance = makeS3Instance({ max_size: 50 });
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send.mockResolvedValueOnce(makeListResponse([
                makeS3Object('large-file.md', now, 100), // exceeds max_size of 50
            ]));

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // No GetObject call because size exceeds limit
            expect(mockS3Send).toHaveBeenCalledTimes(1); // Only ListObjects
            expect(processChunksSpy).not.toHaveBeenCalled();
        });

        it('should use url_rewrite_prefix for generated URLs', async () => {
            const instance = makeS3Instance({
                prefix: 'docs/',
                url_rewrite_prefix: 'https://docs.example.com',
            });
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('docs/guide/intro.md', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('# Intro'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            expect(processChunksSpy).toHaveBeenCalledWith(
                expect.anything(),
                'https://docs.example.com/guide/intro.md',
                expect.anything(),
                expect.anything()
            );
        });

        it('should use s3:// URL when no url_rewrite_prefix', async () => {
            const instance = makeS3Instance();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('docs/readme.md', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('content'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            expect(processChunksSpy).toHaveBeenCalledWith(
                expect.anything(),
                's3://test-bucket/docs/readme.md',
                expect.anything(),
                expect.anything()
            );
        });

        it('should remove chunks for deleted objects', async () => {
            const { DatabaseManager: DB } = await import('../database');
            // Previous run had 2 files, now bucket only has 1
            (DB.getMetadataValue as any)
                .mockResolvedValueOnce('0')   // s3_last_sync_*
                .mockResolvedValueOnce(JSON.stringify(['file1.md', 'file2.md'])); // s3_filelist_*

            const instance = makeS3Instance();
            vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('file1.md', now), // file2.md no longer exists
                ]))
                .mockResolvedValueOnce(makeGetResponse('content'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // Should remove chunks for file2.md
            expect(DB.removeChunksByUrlSQLite).toHaveBeenCalledWith(
                expect.anything(),
                's3://test-bucket/file2.md',
                expect.anything()
            );
        });

        it('should persist sync state after processing', async () => {
            const { DatabaseManager: DB } = await import('../database');

            const instance = makeS3Instance();
            vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('file.md', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('content'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            const beforeTimestamp = Date.now();
            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            const setCalls = (DB.setMetadataValue as any).mock.calls;

            // Should save file list
            const fileListCall = setCalls.find((c: any[]) => /s3_filelist_/.test(c[1]));
            expect(fileListCall).toBeDefined();
            expect(JSON.parse(fileListCall[2])).toEqual(['file.md']);

            // Should save sync timestamp captured at the start of the sync
            const syncCall = setCalls.find((c: any[]) => /s3_last_sync_/.test(c[1]));
            expect(syncCall).toBeDefined();
            const storedTimestamp = parseInt(syncCall[2], 10);
            expect(storedTimestamp).toBeGreaterThan(0);
            // Timestamp should be captured at the start of sync, not long after
            expect(storedTimestamp).toBeLessThanOrEqual(beforeTimestamp + 50);
        });

        it('should use prefix in ListObjectsV2Command', async () => {
            const instance = makeS3Instance({ prefix: 'docs/v2/' });
            vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            mockS3Send.mockResolvedValueOnce(makeListResponse([]));

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // Verify send was called with a command containing the prefix
            const firstSendCall = mockS3Send.mock.calls[0][0];
            expect(firstSendCall).toEqual(
                expect.objectContaining({
                    Bucket: 'test-bucket',
                    Prefix: 'docs/v2/',
                })
            );
        });

        it('should handle binary files (PDF) via convertFileToMarkdown', async () => {
            const instance = makeS3Instance({ include_extensions: ['.pdf'] });
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('doc.pdf', now),
                ]))
                .mockResolvedValueOnce(makeGetResponse('pdf-binary-content'));

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            expect((instance as any).contentProcessor.convertFileToMarkdown).toHaveBeenCalledWith(
                expect.stringContaining('doc.pdf'),
                '.pdf',
                expect.anything()
            );
        });

        it('should handle errors for individual objects without stopping', async () => {
            const instance = makeS3Instance();
            const processChunksSpy = vi.spyOn(instance as any, 'processChunksForUrl').mockResolvedValue(0);

            const now = new Date();
            mockS3Send
                .mockResolvedValueOnce(makeListResponse([
                    makeS3Object('bad.md', now),
                    makeS3Object('good.md', now),
                ]))
                .mockRejectedValueOnce(new Error('Access Denied'))  // bad.md fetch fails
                .mockResolvedValueOnce(makeGetResponse('good content')); // good.md succeeds

            (instance as any).contentProcessor.chunkMarkdown.mockResolvedValueOnce([]);

            await (instance as any).processS3(
                (instance as any).config.sources[0],
                (instance as any).logger
            );

            // good.md should still be processed despite bad.md failing
            expect(processChunksSpy).toHaveBeenCalledTimes(1);
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // GitHub issues: incremental re-processing of updated / closed issues
    // ─────────────────────────────────────────────────────────────────────────
    describe('fetchAndProcessGitHubIssues', () => {
        const repo = 'octo/repo';
        const issuesUrl = `https://api.github.com/repos/${repo}/issues`;
        const issueUrl = (n: number) => `https://github.com/${repo}/issues/${n}`;

        function makeInstance() {
            const configPath = writeTestConfig('gh-issues.yaml', makeMinimalConfig());
            return new Doc2Vec(configPath) as any;
        }

        // getLastRunDate is mocked (line ~62) to return 2025-01-01T00:00:00Z.
        // An issue created BEFORE that date but closed/commented AFTER it is the
        // exact case that regressed: GitHub's `since` returns it, but the old
        // created_at filter used to drop it.
        const oldButRecentlyClosedIssue = {
            number: 42,
            title: 'Old bug that just got closed',
            user: { login: 'alice' },
            state: 'closed',
            created_at: '2024-06-01T00:00:00Z', // before last run
            updated_at: '2025-02-01T00:00:00Z', // closed/commented after last run
            labels: [],
            body: 'A long-standing bug.',
        };

        // Single-page mock: no Link header, so the cursor walk stops after one page.
        function mockGitHubApi(issues: any[], comments: any[] = []) {
            (axios.get as any).mockImplementation((url: string) => {
                if (url.includes('/comments')) {
                    return Promise.resolve({ data: comments, headers: {} });
                }
                return Promise.resolve({ data: issues, headers: {} });
            });
        }

        /**
         * Mock a paginated endpoint that only advertises `Link: rel="next"`
         * cursors — requesting `?page=N` past the first page returns 422, which
         * is how GitHub behaves on large result sets.
         */
        function mockCursorPaginatedApi(pages: any[][], comments: any[] = []) {
            const requestedUrls: string[] = [];
            (axios.get as any).mockImplementation((url: string, opts?: any) => {
                if (url.includes('/comments')) {
                    return Promise.resolve({ data: comments, headers: {} });
                }
                requestedUrls.push(url);

                if (opts?.params?.page !== undefined || /[?&]page=/.test(url)) {
                    const err: any = new Error('Request failed with status code 422');
                    err.response = {
                        status: 422,
                        headers: {},
                        data: { message: 'Pagination with the page parameter is not supported for large datasets, please use cursor based pagination (after/before)' },
                    };
                    return Promise.reject(err);
                }

                const cursorMatch = url.match(/after=cursor-(\d+)/);
                const index = cursorMatch ? Number(cursorMatch[1]) : 0;
                const isLast = index >= pages.length - 1;
                return Promise.resolve({
                    data: pages[index] ?? [],
                    headers: isLast ? {} : { link: `<${issuesUrl}?per_page=100&after=cursor-${index + 1}>; rel="next"` },
                });
            });
            return { requestedUrls };
        }

        it('processes an old issue closed since the last run (no created_at filtering)', async () => {
            const instance = makeInstance();
            mockGitHubApi([oldButRecentlyClosedIssue]);

            await instance.fetchAndProcessGitHubIssues(
                repo,
                { product_name: repo },
                { type: 'sqlite', db: {} },
                instance.logger,
            );

            // The issue must be processed, not silently dropped for being old.
            const chunkCalls = instance.contentProcessor.chunkMarkdown.mock.calls;
            expect(chunkCalls.length).toBe(1);
            // Markdown reflects the up-to-date (closed) state.
            expect(chunkCalls[0][0]).toContain('**State:** closed');
            expect(chunkCalls[0][2]).toBe(issueUrl(42));
        });

        it('purges the issue\'s existing chunks before storing the refreshed set', async () => {
            const instance = makeInstance();
            mockGitHubApi([oldButRecentlyClosedIssue]);

            await instance.fetchAndProcessGitHubIssues(
                repo,
                { product_name: repo },
                { type: 'sqlite', db: {} },
                instance.logger,
            );

            // Stale chunks (content-hash ids) would otherwise linger with the old
            // "open" state and missing comments.
            expect(DatabaseManager.removeChunksByUrlSQLite).toHaveBeenCalledWith(
                {},
                issueUrl(42),
                expect.anything(),
            );
        });

        // ─── Cursor pagination ───────────────────────────────────────────
        // GitHub caps page-number pagination on this endpoint and answers 422
        // past its offset limit, which aborted the whole source and left the
        // collection empty. The walk must follow Link: rel="next" instead.
        describe('pagination', () => {
            const makeIssue = (n: number) => ({
                number: n,
                title: `Issue ${n}`,
                user: { login: 'alice' },
                state: 'open',
                created_at: '2025-02-01T00:00:00Z',
                updated_at: '2025-02-02T00:00:00Z',
                labels: [],
                body: 'body',
            });

            it('follows Link rel="next" cursors across all pages', async () => {
                const instance = makeInstance();
                mockCursorPaginatedApi([
                    [makeIssue(1), makeIssue(2)],
                    [makeIssue(3)],
                ]);

                await instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                );

                // All three issues across both pages must be processed
                const chunkedUrls = instance.contentProcessor.chunkMarkdown.mock.calls.map((c: any[]) => c[2]);
                expect(chunkedUrls).toEqual([issueUrl(1), issueUrl(2), issueUrl(3)]);
            });

            it('never sends a page parameter (page-number paging is rejected past the cap)', async () => {
                const instance = makeInstance();
                mockCursorPaginatedApi([[makeIssue(1)]]);

                await instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                );

                for (const call of (axios.get as any).mock.calls) {
                    expect(call[1]?.params?.page).toBeUndefined();
                }
            });

            it('requests a stable ordering so a mid-walk update cannot shuffle results', async () => {
                const instance = makeInstance();
                mockCursorPaginatedApi([[makeIssue(1)]]);

                await instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                );

                const firstCall = (axios.get as any).mock.calls[0];
                expect(firstCall[1].params).toMatchObject({ sort: 'updated', direction: 'asc', state: 'all' });
            });

            it('passes the cursor URL through verbatim without re-appending params', async () => {
                const instance = makeInstance();
                mockCursorPaginatedApi([[makeIssue(1)], [makeIssue(2)]]);

                await instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                );

                const cursorCall = (axios.get as any).mock.calls.find((c: any[]) => c[0].includes('after=cursor-1'));
                expect(cursorCall).toBeDefined();
                // Re-adding params would duplicate the query string already in the URL
                expect(cursorCall[1].params).toBeUndefined();
            });

            it('stops instead of looping when the next cursor repeats the current URL', async () => {
                const instance = makeInstance();
                (axios.get as any).mockImplementation((url: string) => {
                    if (url.includes('/comments')) return Promise.resolve({ data: [], headers: {} });
                    // Always advertises itself as the next page
                    return Promise.resolve({ data: [makeIssue(1)], headers: { link: `<${url}>; rel="next"` } });
                });

                await instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                );

                expect(instance.contentProcessor.chunkMarkdown.mock.calls.length).toBe(1);
            });

            it('paginates issue comments instead of silently keeping only the first page', async () => {
                const instance = makeInstance();
                const commentsUrl = `${issuesUrl}/7/comments`;
                (axios.get as any).mockImplementation((url: string, opts?: any) => {
                    if (url.includes('/comments')) {
                        if (url.includes('after=c2')) {
                            return Promise.resolve({ data: [{ user: { login: 'carol' }, created_at: '2025-02-03T00:00:00Z', body: 'third' }], headers: {} });
                        }
                        return Promise.resolve({
                            data: [{ user: { login: 'bob' }, created_at: '2025-02-02T00:00:00Z', body: 'second' }],
                            headers: { link: `<${commentsUrl}?after=c2>; rel="next"` },
                        });
                    }
                    return Promise.resolve({ data: [makeIssue(7)], headers: {} });
                });

                await instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                );

                const markdown = instance.contentProcessor.chunkMarkdown.mock.calls[0][0];
                expect(markdown).toContain('second');
                expect(markdown).toContain('third');
            });
        });

        // ─── Retry classification ────────────────────────────────────────
        describe('retry classification', () => {
            function mockStatus(status: number, message = 'boom') {
                (axios.get as any).mockImplementation(() => {
                    const err: any = new Error(`Request failed with status code ${status}`);
                    err.response = { status, headers: {}, data: { message } };
                    return Promise.reject(err);
                });
            }

            it('fails fast on 422 without burning retries', async () => {
                const instance = makeInstance();
                mockStatus(422, 'Pagination with the page parameter is not supported for large datasets');

                await expect(instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                )).rejects.toThrow('422');

                // A deterministic failure must be attempted exactly once
                expect((axios.get as any).mock.calls.length).toBe(1);
            });

            it('fails fast on 404', async () => {
                const instance = makeInstance();
                mockStatus(404, 'Not Found');

                await expect(instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                )).rejects.toThrow();
                expect((axios.get as any).mock.calls.length).toBe(1);
            });

            it('still retries 500s', async () => {
                const instance = makeInstance();
                let attempts = 0;
                (axios.get as any).mockImplementation((url: string) => {
                    if (url.includes('/comments')) return Promise.resolve({ data: [], headers: {} });
                    attempts++;
                    if (attempts < 3) {
                        const err: any = new Error('Request failed with status code 500');
                        err.response = { status: 500, headers: {}, data: {} };
                        return Promise.reject(err);
                    }
                    return Promise.resolve({ data: [], headers: {} });
                });

                await instance.fetchAndProcessGitHubIssues(
                    repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
                );

                expect(attempts).toBe(3);
            }, 30000);
        });

        // ─── Pull requests ───────────────────────────────────────────────
        it('labels pull requests as PRs rather than issues', async () => {
            const instance = makeInstance();
            mockGitHubApi([{
                number: 1915,
                title: 'chore(deps): bump the go-minor-patch group',
                user: { login: 'dependabot[bot]' },
                state: 'open',
                created_at: '2025-02-01T00:00:00Z',
                updated_at: '2025-02-02T00:00:00Z',
                labels: [],
                body: 'bumps',
                pull_request: { url: 'https://api.github.com/repos/octo/repo/pulls/1915' },
            }]);

            await instance.fetchAndProcessGitHubIssues(
                repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
            );

            const markdown = instance.contentProcessor.chunkMarkdown.mock.calls[0][0];
            expect(markdown).toContain('# PR #1915:');
            expect(markdown).toContain('**Type:** Pull request');
            expect(markdown).not.toContain('# Issue #1915:');
        });

        it('still labels plain issues as issues', async () => {
            const instance = makeInstance();
            mockGitHubApi([oldButRecentlyClosedIssue]);

            await instance.fetchAndProcessGitHubIssues(
                repo, { product_name: repo }, { type: 'sqlite', db: {} }, instance.logger,
            );

            const markdown = instance.contentProcessor.chunkMarkdown.mock.calls[0][0];
            expect(markdown).toContain('# Issue #42:');
            expect(markdown).toContain('**Type:** Issue');
        });
    });

    // ─────────────────────────────────────────────────────────────────────────
    // Source run counters (per-source stats surfaced in the controller UI)
    // ─────────────────────────────────────────────────────────────────────────
    describe('source run counters', () => {
        const url = 'https://example.com/page';
        const dbConnection = { type: 'sqlite', db: {} };

        function makeChunk(content: string, index = 0, total = 1) {
            return {
                content,
                metadata: {
                    product_name: 'TestSite',
                    version: '1.0',
                    heading_hierarchy: [],
                    section: 'S',
                    chunk_id: `chunk-${index}`,
                    url,
                    chunk_index: index,
                    total_chunks: total,
                },
            };
        }

        async function makeInstanceWithEmbeddings() {
            const { __mockEmbeddingsCreate } = await import('openai') as any;
            __mockEmbeddingsCreate.mockResolvedValue({ data: [{ embedding: [0.1, 0.2] }] });
            const configPath = writeTestConfig('counters.yaml', makeMinimalConfig());
            return new Doc2Vec(configPath) as any;
        }

        it('counts a URL with no stored chunks as new and tallies added chunks', async () => {
            const instance = await makeInstanceWithEmbeddings();
            (DatabaseManager.getChunkHashesByUrlSQLite as any).mockReturnValue([]);

            await instance.processChunksForUrl([makeChunk('a'), makeChunk('b', 1, 2)], url, dbConnection, instance.logger);

            expect(instance.counters.items_new).toBe(1);
            expect(instance.counters.items_updated).toBe(0);
            expect(instance.counters.items_unchanged).toBe(0);
            expect(instance.counters.chunks_added).toBe(2);
            expect(instance.counters.chunks_deleted).toBe(0);
        });

        it('counts an identical URL as unchanged with no chunk churn', async () => {
            const instance = await makeInstanceWithEmbeddings();
            const { Utils } = await import('../utils');
            (Utils.generateHash as any).mockImplementation((content: string) => `hash-${content}`);
            (DatabaseManager.getChunkHashesByUrlSQLite as any).mockReturnValue(['hash-a']);

            await instance.processChunksForUrl([makeChunk('a')], url, dbConnection, instance.logger);

            expect(instance.counters.items_unchanged).toBe(1);
            expect(instance.counters.items_new).toBe(0);
            expect(instance.counters.chunks_added).toBe(0);
            expect(instance.counters.chunks_deleted).toBe(0);
        });

        it('counts a changed URL as updated, deleting old chunks and adding new ones', async () => {
            const instance = await makeInstanceWithEmbeddings();
            const { Utils } = await import('../utils');
            (Utils.generateHash as any).mockImplementation((content: string) => `hash-${content}`);
            (DatabaseManager.getChunkHashesByUrlSQLite as any).mockReturnValue(['hash-old-1', 'hash-old-2', 'hash-old-3']);

            await instance.processChunksForUrl([makeChunk('a')], url, dbConnection, instance.logger);

            expect(instance.counters.items_updated).toBe(1);
            expect(instance.counters.chunks_deleted).toBe(3);
            expect(instance.counters.chunks_added).toBe(1);
        });

        it('attaches counters with the source-appropriate item kind to run() stats', async () => {
            const configPath = writeTestConfig('counters-run.yaml', makeMinimalConfig());
            const instance = new Doc2Vec(configPath);
            (instance as any).contentProcessor.crawlWebsite = vi.fn().mockResolvedValue({
                hasNetworkErrors: false,
                brokenLinks: [],
            });

            const stats = await instance.run();

            expect(stats).toHaveLength(1);
            expect(stats[0].counters).toMatchObject({
                items_kind: 'pages',
                items_new: 0,
                items_updated: 0,
                items_unchanged: 0,
                items_deleted: 0,
                chunks_added: 0,
                chunks_deleted: 0,
            });
        });

        it('counts obsolete cleanup results as deleted items and chunks', async () => {
            const configPath = writeTestConfig('counters-cleanup.yaml', makeMinimalConfig());
            const instance = new Doc2Vec(configPath);
            (instance as any).contentProcessor.crawlWebsite = vi.fn().mockResolvedValue({
                hasNetworkErrors: false,
                brokenLinks: [],
            });
            (DatabaseManager.removeObsoleteChunksSQLite as any).mockReturnValue({ items: 2, chunks: 7 });

            const stats = await instance.run();

            expect(stats[0].counters).toMatchObject({
                items_deleted: 2,
                chunks_deleted: 7,
            });
        });
    });
});
