import axios from 'axios';
import { Buffer } from 'buffer';
import { Logger } from './logger';
import { ZendeskSourceConfig } from './types';

/**
 * Read-only access to every GET endpoint the sync calls: search.json, ticket
 * comments, organizations and help_center/articles. Zendesk does not document
 * which resource scope search.json needs, so the default is the broad `read`
 * rather than `tickets:read organizations:read hc:read`.
 */
export const DEFAULT_ZENDESK_OAUTH_SCOPE = 'read';

/**
 * Zendesk's own default for clients created on or after 2026-04-30. Sent
 * explicitly because older clients default to tokens that never expire, and
 * every sync run mints a new token.
 */
export const DEFAULT_ZENDESK_OAUTH_EXPIRES_IN = 1800;

/** Fetch a new token this long before the current one expires. */
const EXPIRY_MARGIN_MS = 60_000;

const PLACEHOLDER = /^\$\{([^}]+)\}$/;

/**
 * The Authorization header for a Zendesk source.
 *
 * OAuth (preferred): `oauth_client_id` + `oauth_client_secret` of a
 * confidential OAuth client, exchanged for a short-lived bearer token with the
 * client_credentials grant. Zendesk returns no refresh token for this grant,
 * so an expired token is replaced by asking again with the same credentials.
 * The token acts as the Zendesk user who created the OAuth client.
 *
 * API token (legacy): `email` + `api_token` as HTTP Basic. Zendesk stopped
 * issuing API tokens on 2026-10-27 and deactivates the remaining ones on
 * 2027-04-30.
 */
export class ZendeskAuth {
    private readonly mode: 'oauth' | 'api_token';
    private accessToken: string | null = null;
    private expiresAt = 0;
    private inflight: Promise<string> | null = null;

    constructor(private readonly config: ZendeskSourceConfig, private readonly logger: Logger) {
        const has = (v: unknown) => typeof v === 'string' && v.trim().length > 0;
        const hasOAuth = has(config.oauth_client_id) || has(config.oauth_client_secret);

        if (hasOAuth) {
            if (!has(config.oauth_client_id) || !has(config.oauth_client_secret)) {
                throw new Error(`Zendesk source ${config.zendesk_subdomain}: oauth_client_id and oauth_client_secret must be set together`);
            }
            this.assertSubstituted('oauth_client_id', config.oauth_client_id!);
            this.assertSubstituted('oauth_client_secret', config.oauth_client_secret!);
            this.mode = 'oauth';
        } else if (has(config.email) && has(config.api_token)) {
            this.assertSubstituted('email', config.email!);
            this.assertSubstituted('api_token', config.api_token!);
            this.mode = 'api_token';
            logger.warn(
                `Zendesk source ${config.zendesk_subdomain} authenticates with an API token. ` +
                `Zendesk deactivates all API tokens on 2027-04-30; switch to oauth_client_id/oauth_client_secret.`
            );
        } else {
            throw new Error(
                `Zendesk source ${config.zendesk_subdomain}: set oauth_client_id and oauth_client_secret ` +
                `(or the legacy email and api_token)`
            );
        }
    }

    get usesOAuth(): boolean {
        return this.mode === 'oauth';
    }

    /** The Authorization header value for the next request. */
    async header(): Promise<string> {
        if (this.mode === 'api_token') {
            return `Basic ${Buffer.from(`${this.config.email}/token:${this.config.api_token}`).toString('base64')}`;
        }
        if (this.accessToken && Date.now() < this.expiresAt - EXPIRY_MARGIN_MS) {
            return `Bearer ${this.accessToken}`;
        }
        if (!this.inflight) {
            this.inflight = this.requestToken().finally(() => { this.inflight = null; });
        }
        return `Bearer ${await this.inflight}`;
    }

    /**
     * Forget the cached token after Zendesk answered 401, so the next header()
     * asks for a new one. Returns false when a new token cannot help (API token
     * auth), so the caller should not retry.
     */
    invalidate(): boolean {
        if (this.mode !== 'oauth') return false;
        this.accessToken = null;
        this.expiresAt = 0;
        return true;
    }

    private async requestToken(): Promise<string> {
        const url = `https://${this.config.zendesk_subdomain}.zendesk.com/oauth/tokens`;
        const scope = this.config.oauth_scope?.trim() || DEFAULT_ZENDESK_OAUTH_SCOPE;
        const expiresIn = this.config.oauth_token_expires_in ?? DEFAULT_ZENDESK_OAUTH_EXPIRES_IN;
        const body = new URLSearchParams({
            grant_type: 'client_credentials',
            client_id: this.config.oauth_client_id!,
            client_secret: this.config.oauth_client_secret!,
            scope,
            expires_in: String(expiresIn),
        });

        let data: any;
        try {
            const response = await axios.post(url, body.toString(), {
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                timeout: 30_000,
            });
            data = response.data;
        } catch (error: any) {
            // Rebuild the error: axios's carries the request body, client secret included.
            const status = error?.response?.status;
            const zd = error?.response?.data;
            const detail = [zd?.error, zd?.error_description].filter(Boolean).join(': ');
            throw new Error(
                `Zendesk OAuth token request to ${url} failed` +
                (status ? ` with HTTP ${status}` : `: ${error?.code || error?.message || 'unknown error'}`) +
                (detail ? ` (${detail})` : '')
            );
        }

        if (!data?.access_token) {
            throw new Error(`Zendesk OAuth token response from ${url} has no access_token`);
        }
        this.accessToken = data.access_token as string;
        const lifetime = Number(data.expires_in);
        this.expiresAt = Number.isFinite(lifetime) && lifetime > 0 ? Date.now() + lifetime * 1000 : Infinity;
        this.logger.info(
            `Obtained Zendesk OAuth token for ${this.config.zendesk_subdomain} ` +
            `(scope: ${data.scope || scope}, expires in: ${Number.isFinite(lifetime) && lifetime > 0 ? `${lifetime}s` : 'never'})`
        );
        return this.accessToken;
    }

    private assertSubstituted(field: string, value: string): void {
        const m = PLACEHOLDER.exec(value.trim());
        if (m) {
            throw new Error(`Zendesk source ${this.config.zendesk_subdomain}: ${field} is the placeholder ${value}; environment variable ${m[1]} is not set`);
        }
    }
}
