import crypto from 'crypto';
import * as path from 'path';
import { Logger } from './logger';

export class Utils {
    static generateHash(content: string): string {
        return crypto.createHash("sha256").update(content).digest("hex");
    }

    static generateMetadataUUID(repo: string): string {
        // Simple deterministic approach - hash the repo name and convert to UUID format
        const hash = crypto.createHash('md5').update(`metadata_${repo}`).digest('hex');
        // Format as UUID with version bits set correctly (version 4)
        return `${hash.substr(0, 8)}-${hash.substr(8, 4)}-4${hash.substr(13, 3)}-${hash.substr(16, 4)}-${hash.substr(20, 12)}`;
    }

    static getUrlPrefix(url: string): string {
        try {
            const parsedUrl = new URL(url);
            return parsedUrl.origin + parsedUrl.pathname;
        } catch (error) {
            return url;
        }
    }

    static normalizeUrl(url: string): string {
        try {
            const urlObj = new URL(url);
            urlObj.hash = '';
            urlObj.search = '';
            return urlObj.toString();
        } catch (error) {
            return url;
        }
    }

    static buildUrl(href: string, currentUrl: string, logger?: Logger): string {
        try {
            return new URL(href, currentUrl).toString();
        } catch (error) {
            if (logger) {
                logger.warn(`Invalid URL found: ${href}`);
            }
            return '';
        }
    }

    static shouldProcessUrl(url: string): boolean {
        const parsedUrl = new URL(url);
        const pathname = parsedUrl.pathname;

        // Paths ending with / are directory-like URLs (e.g., /app/2.1.x/), always process them
        if (pathname.endsWith('/')) return true;

        const ext = path.extname(pathname);
        if (!ext) return true;
        return ['.html', '.htm', '.pdf'].includes(ext.toLowerCase());
    }

    static isPdfUrl(url: string): boolean {
        try {
            const parsedUrl = new URL(url);
            const pathname = parsedUrl.pathname;
            const ext = path.extname(pathname);
            return ext.toLowerCase() === '.pdf';
        } catch (error) {
            return false;
        }
    }

    static isValidUuid(str: string): boolean {
        const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
        return uuidRegex.test(str);
    }

    static hashToUuid(hash: string): string {
        const truncatedHash = hash.substring(0, 32);
        
        return [
            truncatedHash.substring(0, 8),
            truncatedHash.substring(8, 12),
            '5' + truncatedHash.substring(13, 16),
            '8' + truncatedHash.substring(17, 20),
            truncatedHash.substring(20, 32)
        ].join('-');
    }

    static tokenize(text: string): string[] {
        return text.split(/(\s+)/).filter(token => token.length > 0);
    }

    /**
     * Extract the `rel="next"` URL from an RFC 5988 Link header.
     *
     * GitHub's list endpoints cap page-number pagination (`?page=N`) and reject
     * deeper offsets with HTTP 422 ("please use cursor based pagination"), so
     * walking a large result set requires following these URLs — they already
     * carry the opaque `after=` cursor and must be passed through verbatim
     * rather than rebuilt from parts.
     */
    static parseNextLink(linkHeader: string | undefined | null): string | null {
        if (!linkHeader) return null;
        for (const part of linkHeader.split(',')) {
            const match = part.match(/<([^>]+)>\s*;\s*rel\s*=\s*"?next"?/i);
            if (match) return match[1].trim();
        }
        return null;
    }

    /**
     * Drop unpaired UTF-16 surrogates.
     *
     * A lone surrogate is not valid UTF-8, so strict JSON parsers reject the
     * whole request body — Qdrant answers 400 "lone leading surrogate in hex
     * escape" and the chunk is lost. Content can arrive this way from an
     * upstream source, so sanitize before storing.
     */
    static stripLoneSurrogates(text: string): string {
        // Node 20+ exposes toWellFormed(), which replaces lone surrogates with
        // U+FFFD. Removing them outright keeps the text closer to the original.
        return text
            .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/g, '')
            .replace(/(^|[^\uD800-\uDBFF])([\uDC00-\uDFFF])/g, '$1');
    }

    /**
     * Slice a string without splitting a surrogate pair.
     *
     * Plain `slice()` works on UTF-16 code units, so a boundary landing inside
     * a pair (emoji, some CJK) leaves a lone surrogate on each side. Both
     * boundaries are nudged the same way — a straddled pair always travels with
     * the *following* slice — so consecutive slices stay lossless: no pair falls
     * into the gap between them, and none is duplicated.
     */
    static sliceSafe(text: string, start: number, end: number): string {
        const isHigh = (code: number) => code >= 0xd800 && code <= 0xdbff;
        const isLow = (code: number) => code >= 0xdc00 && code <= 0xdfff;
        // True when the pair straddling `index` must move to the next slice
        const straddles = (index: number) =>
            index > 0 && index < text.length && isHigh(text.charCodeAt(index - 1)) && isLow(text.charCodeAt(index));

        let from = Math.max(0, Math.min(start, text.length));
        let to = Math.max(from, Math.min(text.length, end));
        // Step back to pick up the high half the previous slice left behind
        if (straddles(from)) from--;
        // Leave the whole pair for the next slice
        if (to > from && straddles(to)) to--;
        return text.slice(from, to);
    }

    /**
     * Normalize a glob pattern or a relative path to the form the matcher uses:
     * forward slashes, no leading `./`, no leading or trailing `/`.
     */
    private static normalizeGlobPath(value: string): string {
        return value
            .replace(/\\/g, '/')
            .replace(/^\.\//, '')
            .replace(/^\/+/, '')
            .replace(/\/+$/, '');
    }

    /**
     * Build a regular expression from a glob pattern.
     *
     * `**` crosses directory separators, `*` and `?` do not. A globstar
     * followed by a slash also matches zero directories, so a pattern like
     * `**` + `/*_test.go` matches `main_test.go`
     * at the root as well as `pkg/a/main_test.go`.
     */
    private static globToRegExp(pattern: string): RegExp {
        let source = '';
        let i = 0;
        while (i < pattern.length) {
            const char = pattern[i];
            if (char === '*') {
                let end = i;
                while (pattern[end] === '*') end++;
                const isGlobstar = end - i > 1;
                if (!isGlobstar) {
                    source += '[^/]*';
                    i = end;
                } else if (pattern[end] === '/') {
                    source += '(?:.*/)?';
                    i = end + 1;
                } else {
                    source += '.*';
                    i = end;
                }
            } else if (char === '?') {
                source += '[^/]';
                i++;
            } else {
                source += char.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                i++;
            }
        }
        return new RegExp(`^${source}$`);
    }

    /**
     * Report whether a relative path matches one of the glob patterns.
     */
    static matchesAnyGlob(relativePath: string, patterns: string[]): boolean {
        if (!patterns || patterns.length === 0) return false;
        const normalizedPath = Utils.normalizeGlobPath(relativePath);
        if (normalizedPath === '') return false;
        return patterns.some((pattern) => {
            const normalizedPattern = Utils.normalizeGlobPath(pattern);
            if (normalizedPattern === '') return false;
            return Utils.globToRegExp(normalizedPattern).test(normalizedPath);
        });
    }

    /**
     * Report whether a directory is excluded, so the caller can skip the whole
     * subtree instead of walking it.
     *
     * A directory is excluded when a pattern matches the directory itself
     * (`vendor`) or when a pattern excludes everything below it (`vendor/**`).
     * Patterns that only select some descendants (a test-file pattern, for
     * example) do not exclude the directory — the walk must continue and
     * filter each file.
     */
    static isDirectoryExcluded(relativePath: string, patterns: string[]): boolean {
        if (!patterns || patterns.length === 0) return false;
        const prefixes = patterns
            .map((pattern) => Utils.normalizeGlobPath(pattern))
            .filter((pattern) => pattern.endsWith('/**'))
            .map((pattern) => pattern.slice(0, -3));
        return Utils.matchesAnyGlob(relativePath, patterns) || Utils.matchesAnyGlob(relativePath, prefixes);
    }

}