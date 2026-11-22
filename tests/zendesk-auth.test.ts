import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Logger, LogLevel } from '../logger';
import { ZendeskAuth, defaultZendeskOAuthScope, DEFAULT_ZENDESK_OAUTH_EXPIRES_IN } from '../zendesk-auth';

vi.mock('axios', () => ({
    default: {
        get: vi.fn(),
        post: vi.fn(),
    },
}));

const testLogger = new Logger('zendesk-auth-test', { level: LogLevel.NONE });

function makeConfig(overrides: any = {}) {
    return {
        type: 'zendesk',
        product_name: 'zd',
        version: 'latest',
        zendesk_subdomain: 'testco',
        database_config: { type: 'sqlite', params: {} },
        ...overrides,
    } as any;
}

const OAUTH = { oauth_client_id: 'doc2vec', oauth_client_secret: 's3cret-value' };

async function axiosPost() {
    const axiosMod = await import('axios');
    return (axiosMod.default as any).post as ReturnType<typeof vi.fn>;
}

describe('ZendeskAuth', () => {
    beforeEach(async () => {
        (await axiosPost()).mockReset();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    describe('configuration', () => {
        it('uses Basic auth for the legacy email + api_token pair', async () => {
            const auth = new ZendeskAuth(makeConfig({ email: 'a@b.com', api_token: 'tok' }), testLogger);
            expect(auth.usesOAuth).toBe(false);
            expect(await auth.header()).toBe(`Basic ${Buffer.from('a@b.com/token:tok').toString('base64')}`);
            expect(await axiosPost()).not.toHaveBeenCalled();
        });

        it('prefers OAuth when both OAuth and API token fields are set', () => {
            const auth = new ZendeskAuth(makeConfig({ ...OAUTH, email: 'a@b.com', api_token: 'tok' }), testLogger);
            expect(auth.usesOAuth).toBe(true);
        });

        it('rejects a config with no credentials', () => {
            expect(() => new ZendeskAuth(makeConfig(), testLogger)).toThrow(/set oauth_client_id and oauth_client_secret/);
        });

        it('rejects half of the OAuth pair', () => {
            expect(() => new ZendeskAuth(makeConfig({ oauth_client_id: 'doc2vec' }), testLogger))
                .toThrow(/must be set together/);
            expect(() => new ZendeskAuth(makeConfig({ oauth_client_secret: 'x', email: 'a@b.com', api_token: 'tok' }), testLogger))
                .toThrow(/must be set together/);
        });

        it('rejects an unsubstituted ${VAR} placeholder without echoing a value', () => {
            expect(() => new ZendeskAuth(makeConfig({ oauth_client_id: 'doc2vec', oauth_client_secret: '${ZENDESK_OAUTH_CLIENT_SECRET}' }), testLogger))
                .toThrow(/environment variable ZENDESK_OAUTH_CLIENT_SECRET is not set/);
            expect(() => new ZendeskAuth(makeConfig({ email: 'a@b.com', api_token: '${ZENDESK_API_TOKEN}' }), testLogger))
                .toThrow(/environment variable ZENDESK_API_TOKEN is not set/);
        });

        it('invalidate() refuses to retry for API token auth', () => {
            const auth = new ZendeskAuth(makeConfig({ email: 'a@b.com', api_token: 'tok' }), testLogger);
            expect(auth.invalidate()).toBe(false);
        });
    });

    describe('client_credentials grant', () => {
        it('posts a form-encoded client_credentials request and returns a Bearer header', async () => {
            const post = await axiosPost();
            post.mockResolvedValueOnce({ data: { access_token: 'tok-1', token_type: 'bearer', scope: 'read', expires_in: 1800 } });

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            expect(await auth.header()).toBe('Bearer tok-1');

            expect(post).toHaveBeenCalledTimes(1);
            const [url, body, opts] = post.mock.calls[0];
            expect(url).toBe('https://testco.zendesk.com/oauth/tokens');
            expect(opts.headers['Content-Type']).toBe('application/x-www-form-urlencoded');
            const params = new URLSearchParams(body);
            expect(Object.fromEntries(params)).toEqual({
                grant_type: 'client_credentials',
                client_id: 'doc2vec',
                client_secret: 's3cret-value',
                scope: 'tickets:read hc:read',
                expires_in: String(DEFAULT_ZENDESK_OAUTH_EXPIRES_IN),
            });
        });

        it('derives the default scope from what the source fetches', () => {
            expect(defaultZendeskOAuthScope(makeConfig())).toBe('tickets:read hc:read');
            expect(defaultZendeskOAuthScope(makeConfig({ fetch_articles: false }))).toBe('tickets:read');
            expect(defaultZendeskOAuthScope(makeConfig({ fetch_articles: false, excluded_organizations: ['Acme'] })))
                .toBe('tickets:read organizations:read');
            expect(defaultZendeskOAuthScope(makeConfig({ fetch_articles: false, excluded_organizations: [] }))).toBe('tickets:read');
            expect(defaultZendeskOAuthScope(makeConfig({ fetch_tickets: false }))).toBe('hc:read');
            expect(defaultZendeskOAuthScope(makeConfig({ fetch_tickets: false, fetch_articles: false }))).toBe('read');
        });

        it('sends the configured scope and lifetime', async () => {
            const post = await axiosPost();
            post.mockResolvedValueOnce({ data: { access_token: 'tok-1', expires_in: 600 } });

            const auth = new ZendeskAuth(makeConfig({ ...OAUTH, oauth_scope: ' tickets:read hc:read ', oauth_token_expires_in: 600 }), testLogger);
            await auth.header();

            const params = new URLSearchParams(post.mock.calls[0][1]);
            expect(params.get('scope')).toBe('tickets:read hc:read');
            expect(params.get('expires_in')).toBe('600');
        });

        it('reuses the token until a minute before it expires', async () => {
            vi.useFakeTimers();
            vi.setSystemTime(new Date('2026-11-20T00:00:00Z'));
            const post = await axiosPost();
            post
                .mockResolvedValueOnce({ data: { access_token: 'tok-1', expires_in: 1800 } })
                .mockResolvedValueOnce({ data: { access_token: 'tok-2', expires_in: 1800 } });

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            expect(await auth.header()).toBe('Bearer tok-1');

            vi.setSystemTime(new Date('2026-11-20T00:28:59Z'));
            expect(await auth.header()).toBe('Bearer tok-1');
            expect(post).toHaveBeenCalledTimes(1);

            vi.setSystemTime(new Date('2026-11-20T00:29:01Z'));
            expect(await auth.header()).toBe('Bearer tok-2');
            expect(post).toHaveBeenCalledTimes(2);
        });

        it('keeps a token with no expires_in for the whole run', async () => {
            vi.useFakeTimers();
            vi.setSystemTime(new Date('2026-11-20T00:00:00Z'));
            const post = await axiosPost();
            post.mockResolvedValueOnce({ data: { access_token: 'tok-1', expires_in: null } });

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            await auth.header();
            vi.setSystemTime(new Date('2026-11-27T00:00:00Z'));
            expect(await auth.header()).toBe('Bearer tok-1');
            expect(post).toHaveBeenCalledTimes(1);
        });

        it('requests a new token after invalidate()', async () => {
            const post = await axiosPost();
            post
                .mockResolvedValueOnce({ data: { access_token: 'tok-1', expires_in: 1800 } })
                .mockResolvedValueOnce({ data: { access_token: 'tok-2', expires_in: 1800 } });

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            expect(await auth.header()).toBe('Bearer tok-1');
            expect(auth.invalidate()).toBe(true);
            expect(await auth.header()).toBe('Bearer tok-2');
        });

        it('shares one token request between concurrent callers', async () => {
            const post = await axiosPost();
            let resolve!: (v: any) => void;
            post.mockReturnValueOnce(new Promise(r => { resolve = r; }));

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            const both = Promise.all([auth.header(), auth.header()]);
            resolve({ data: { access_token: 'tok-1', expires_in: 1800 } });
            expect(await both).toEqual(['Bearer tok-1', 'Bearer tok-1']);
            expect(post).toHaveBeenCalledTimes(1);
        });

        it('reports Zendesk\'s error without the client secret', async () => {
            const post = await axiosPost();
            post.mockRejectedValueOnce(Object.assign(new Error('Request failed with status code 401'), {
                config: { data: 'grant_type=client_credentials&client_id=doc2vec&client_secret=s3cret-value' },
                response: { status: 401, data: { error: 'invalid_client', error_description: 'Client authentication failed.' } },
            }));

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            const err = await auth.header().catch(e => e);
            expect(err).toBeInstanceOf(Error);
            expect(err.message).toBe(
                'Zendesk OAuth token request to https://testco.zendesk.com/oauth/tokens failed with HTTP 401 ' +
                '(invalid_client: Client authentication failed.)'
            );
            expect(JSON.stringify(err)).not.toContain('s3cret-value');
            expect(err.config).toBeUndefined();
        });

        it('reports a network error by code', async () => {
            const post = await axiosPost();
            post.mockRejectedValueOnce(Object.assign(new Error('getaddrinfo ENOTFOUND'), { code: 'ENOTFOUND' }));

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            await expect(auth.header()).rejects.toThrow(/failed: ENOTFOUND$/);
        });

        it('rejects a response without access_token and retries on the next call', async () => {
            const post = await axiosPost();
            post
                .mockResolvedValueOnce({ data: {} })
                .mockResolvedValueOnce({ data: { access_token: 'tok-2', expires_in: 1800 } });

            const auth = new ZendeskAuth(makeConfig(OAUTH), testLogger);
            await expect(auth.header()).rejects.toThrow(/has no access_token/);
            expect(await auth.header()).toBe('Bearer tok-2');
        });
    });
});
