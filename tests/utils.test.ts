import { describe, it, expect, vi } from 'vitest';
import { Utils } from '../utils';

describe('Utils', () => {
    // ─── generateHash ───────────────────────────────────────────────
    describe('generateHash', () => {
        it('should return a valid SHA-256 hex string', () => {
            const hash = Utils.generateHash('hello world');
            expect(hash).toMatch(/^[a-f0-9]{64}$/);
        });

        it('should produce deterministic output', () => {
            const h1 = Utils.generateHash('test content');
            const h2 = Utils.generateHash('test content');
            expect(h1).toBe(h2);
        });

        it('should produce different hashes for different inputs', () => {
            const h1 = Utils.generateHash('input A');
            const h2 = Utils.generateHash('input B');
            expect(h1).not.toBe(h2);
        });

        it('should handle empty string', () => {
            const hash = Utils.generateHash('');
            expect(hash).toMatch(/^[a-f0-9]{64}$/);
        });

        it('should handle unicode content', () => {
            const hash = Utils.generateHash('Hello');
            expect(hash).toMatch(/^[a-f0-9]{64}$/);
        });

        it('should handle very long strings', () => {
            const longStr = 'a'.repeat(100_000);
            const hash = Utils.generateHash(longStr);
            expect(hash).toMatch(/^[a-f0-9]{64}$/);
        });
    });

    // ─── generateMetadataUUID ───────────────────────────────────────
    describe('generateMetadataUUID', () => {
        it('should return a valid UUID-format string', () => {
            const uuid = Utils.generateMetadataUUID('owner/repo');
            // UUID format: 8-4-4-4-12 hex chars
            expect(uuid).toMatch(/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[a-f0-9]{4}-[a-f0-9]{12}$/);
        });

        it('should produce deterministic UUIDs for the same repo', () => {
            const u1 = Utils.generateMetadataUUID('owner/repo');
            const u2 = Utils.generateMetadataUUID('owner/repo');
            expect(u1).toBe(u2);
        });

        it('should produce different UUIDs for different repos', () => {
            const u1 = Utils.generateMetadataUUID('owner/repo-a');
            const u2 = Utils.generateMetadataUUID('owner/repo-b');
            expect(u1).not.toBe(u2);
        });

        it('should always set version nibble to 4', () => {
            const uuid = Utils.generateMetadataUUID('any-repo');
            // The third section starts with 4 (version 4)
            const parts = uuid.split('-');
            expect(parts[2][0]).toBe('4');
        });
    });

    // ─── getUrlPrefix ───────────────────────────────────────────────
    describe('getUrlPrefix', () => {
        it('should return origin + pathname', () => {
            const result = Utils.getUrlPrefix('https://example.com/docs/api?q=test#section');
            expect(result).toBe('https://example.com/docs/api');
        });

        it('should handle URL without query or hash', () => {
            const result = Utils.getUrlPrefix('https://example.com/path');
            expect(result).toBe('https://example.com/path');
        });

        it('should handle root URL', () => {
            const result = Utils.getUrlPrefix('https://example.com');
            expect(result).toBe('https://example.com/');
        });

        it('should return the original string for invalid URLs', () => {
            const result = Utils.getUrlPrefix('not-a-url');
            expect(result).toBe('not-a-url');
        });

        it('should handle URLs with port', () => {
            const result = Utils.getUrlPrefix('http://localhost:3000/api/v1');
            expect(result).toBe('http://localhost:3000/api/v1');
        });
    });

    // ─── normalizeUrl ───────────────────────────────────────────────
    describe('normalizeUrl', () => {
        it('should strip hash fragments', () => {
            const result = Utils.normalizeUrl('https://example.com/page#section');
            expect(result).toBe('https://example.com/page');
        });

        it('should strip query parameters', () => {
            const result = Utils.normalizeUrl('https://example.com/page?q=test&a=1');
            expect(result).toBe('https://example.com/page');
        });

        it('should strip both hash and query', () => {
            const result = Utils.normalizeUrl('https://example.com/page?q=test#section');
            expect(result).toBe('https://example.com/page');
        });

        it('should leave clean URLs unchanged', () => {
            const result = Utils.normalizeUrl('https://example.com/page');
            expect(result).toBe('https://example.com/page');
        });

        it('should return original string for invalid URLs', () => {
            const result = Utils.normalizeUrl('not-a-url');
            expect(result).toBe('not-a-url');
        });

        it('should handle trailing slash', () => {
            const result = Utils.normalizeUrl('https://example.com/page/');
            expect(result).toBe('https://example.com/page/');
        });
    });

    // ─── buildUrl ───────────────────────────────────────────────────
    describe('buildUrl', () => {
        it('should resolve relative URLs', () => {
            const result = Utils.buildUrl('/about', 'https://example.com/page');
            expect(result).toBe('https://example.com/about');
        });

        it('should resolve relative URLs with ./', () => {
            const result = Utils.buildUrl('./other', 'https://example.com/docs/page');
            expect(result).toBe('https://example.com/docs/other');
        });

        it('should handle absolute URLs', () => {
            const result = Utils.buildUrl('https://other.com/page', 'https://example.com/');
            expect(result).toBe('https://other.com/page');
        });

        it('should return empty string for invalid URLs', () => {
            // Suppress console.warn during this test
            const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
            const result = Utils.buildUrl('://broken', '://also-broken');
            expect(result).toBe('');
            warnSpy.mockRestore();
        });

        it('should resolve path-relative URLs', () => {
            const result = Utils.buildUrl('sibling', 'https://example.com/docs/page');
            expect(result).toBe('https://example.com/docs/sibling');
        });
    });

    // ─── shouldProcessUrl ───────────────────────────────────────────
    describe('shouldProcessUrl', () => {
        it('should return true for URLs without extensions', () => {
            expect(Utils.shouldProcessUrl('https://example.com/docs')).toBe(true);
        });

        it('should return true for .html files', () => {
            expect(Utils.shouldProcessUrl('https://example.com/page.html')).toBe(true);
        });

        it('should return true for .htm files', () => {
            expect(Utils.shouldProcessUrl('https://example.com/page.htm')).toBe(true);
        });

        it('should return true for .pdf files', () => {
            expect(Utils.shouldProcessUrl('https://example.com/doc.pdf')).toBe(true);
        });

        it('should return true for .PDF files (case insensitive)', () => {
            expect(Utils.shouldProcessUrl('https://example.com/doc.PDF')).toBe(true);
        });

        it('should return false for .jpg files', () => {
            expect(Utils.shouldProcessUrl('https://example.com/img.jpg')).toBe(false);
        });

        it('should return false for .css files', () => {
            expect(Utils.shouldProcessUrl('https://example.com/style.css')).toBe(false);
        });

        it('should return false for .js files', () => {
            expect(Utils.shouldProcessUrl('https://example.com/script.js')).toBe(false);
        });

        it('should return false for .png files', () => {
            expect(Utils.shouldProcessUrl('https://example.com/image.png')).toBe(false);
        });

        it('should return true for root URL', () => {
            expect(Utils.shouldProcessUrl('https://example.com/')).toBe(true);
        });

        it('should return true for version-like paths with trailing slash', () => {
            expect(Utils.shouldProcessUrl('https://example.com/app/2.1.x/')).toBe(true);
        });
    });

    // ─── isPdfUrl ───────────────────────────────────────────────────
    describe('isPdfUrl', () => {
        it('should return true for .pdf URLs', () => {
            expect(Utils.isPdfUrl('https://example.com/doc.pdf')).toBe(true);
        });

        it('should return true for .PDF URLs (case insensitive)', () => {
            expect(Utils.isPdfUrl('https://example.com/doc.PDF')).toBe(true);
        });

        it('should return false for non-PDF URLs', () => {
            expect(Utils.isPdfUrl('https://example.com/page.html')).toBe(false);
        });

        it('should return false for URLs without extensions', () => {
            expect(Utils.isPdfUrl('https://example.com/docs')).toBe(false);
        });

        it('should return false for invalid URLs', () => {
            expect(Utils.isPdfUrl('not-a-url')).toBe(false);
        });

        it('should handle URLs with query parameters', () => {
            expect(Utils.isPdfUrl('https://example.com/doc.pdf?v=1')).toBe(true);
        });
    });

    // ─── isValidUuid ────────────────────────────────────────────────
    describe('isValidUuid', () => {
        it('should return true for valid v4 UUID', () => {
            expect(Utils.isValidUuid('550e8400-e29b-41d4-a716-446655440000')).toBe(true);
        });

        it('should return true for valid v5 UUID', () => {
            expect(Utils.isValidUuid('550e8400-e29b-51d4-a716-446655440000')).toBe(true);
        });

        it('should return false for invalid format', () => {
            expect(Utils.isValidUuid('not-a-uuid')).toBe(false);
        });

        it('should return false for empty string', () => {
            expect(Utils.isValidUuid('')).toBe(false);
        });

        it('should be case insensitive', () => {
            expect(Utils.isValidUuid('550E8400-E29B-41D4-A716-446655440000')).toBe(true);
        });

        it('should reject UUIDs with wrong version nibble', () => {
            // Version nibble must be 1-5
            expect(Utils.isValidUuid('550e8400-e29b-61d4-a716-446655440000')).toBe(false);
        });

        it('should reject UUIDs with wrong variant nibble', () => {
            // Variant nibble for RFC 4122 must be 8, 9, a, or b
            expect(Utils.isValidUuid('550e8400-e29b-41d4-0716-446655440000')).toBe(false);
        });
    });

    // ─── hashToUuid ─────────────────────────────────────────────────
    describe('hashToUuid', () => {
        it('should convert a hex hash to UUID format', () => {
            const hash = 'a'.repeat(64); // valid SHA-256 length
            const uuid = Utils.hashToUuid(hash);
            // Should be in UUID format: 8-4-4-4-12
            expect(uuid).toMatch(/^[a-f0-9]{8}-[a-f0-9]{4}-5[a-f0-9]{3}-8[a-f0-9]{3}-[a-f0-9]{12}$/);
        });

        it('should set version nibble to 5', () => {
            const hash = 'b'.repeat(64);
            const uuid = Utils.hashToUuid(hash);
            const parts = uuid.split('-');
            expect(parts[2][0]).toBe('5');
        });

        it('should set variant nibble to 8', () => {
            const hash = 'c'.repeat(64);
            const uuid = Utils.hashToUuid(hash);
            const parts = uuid.split('-');
            expect(parts[3][0]).toBe('8');
        });

        it('should produce deterministic output', () => {
            const hash = Utils.generateHash('test');
            const u1 = Utils.hashToUuid(hash);
            const u2 = Utils.hashToUuid(hash);
            expect(u1).toBe(u2);
        });

        it('should handle short hashes by truncating to 32 chars', () => {
            const hash = 'abcdef1234567890abcdef1234567890abcdef1234567890';
            const uuid = Utils.hashToUuid(hash);
            expect(uuid.replace(/-/g, '').length).toBeLessThanOrEqual(32);
        });
    });

    // ─── tokenize ───────────────────────────────────────────────────
    describe('tokenize', () => {
        it('should split text by whitespace, keeping separators', () => {
            const tokens = Utils.tokenize('hello world');
            expect(tokens).toEqual(['hello', ' ', 'world']);
        });

        it('should handle multiple spaces', () => {
            const tokens = Utils.tokenize('hello   world');
            expect(tokens).toEqual(['hello', '   ', 'world']);
        });

        it('should handle tabs and newlines', () => {
            const tokens = Utils.tokenize('hello\tworld\nfoo');
            expect(tokens).toEqual(['hello', '\t', 'world', '\n', 'foo']);
        });

        it('should return single token for no-whitespace text', () => {
            const tokens = Utils.tokenize('helloworld');
            expect(tokens).toEqual(['helloworld']);
        });

        it('should handle empty string', () => {
            const tokens = Utils.tokenize('');
            expect(tokens).toEqual([]);
        });

        it('should handle whitespace-only string', () => {
            const tokens = Utils.tokenize('   ');
            expect(tokens).toEqual(['   ']);
        });

        it('should handle mixed content with punctuation', () => {
            const tokens = Utils.tokenize('# Hello World!\n\nParagraph here.');
            expect(tokens.length).toBeGreaterThan(0);
            expect(tokens.join('')).toBe('# Hello World!\n\nParagraph here.');
        });
    });

    // ─── shouldProcessUrl - invalid URL ─────────────────────────────
    describe('shouldProcessUrl - invalid URL', () => {
        it('should throw on invalid URL', () => {
            expect(() => Utils.shouldProcessUrl('not-a-url')).toThrow();
        });
    });

    // ─── hashToUuid - edge cases ────────────────────────────────────
    describe('hashToUuid - edge cases', () => {
        it('should handle hash shorter than 32 chars', () => {
            const uuid = Utils.hashToUuid('abcd1234');
            // Should still produce a string with dashes, just potentially shorter segments
            expect(uuid).toContain('-');
        });

        it('should handle exactly 32 hex characters', () => {
            const hash = 'abcdef1234567890abcdef1234567890';
            const uuid = Utils.hashToUuid(hash);
            // Should be in UUID format: 8-4-4-4-12 with version=5 and variant=8
            expect(uuid).toMatch(/^[a-f0-9]{8}-[a-f0-9]{4}-5[a-f0-9]{3}-8[a-f0-9]{3}-[a-f0-9]{12}$/);
        });
    });
    // ─── parseNextLink ──────────────────────────────────────────────
    // GitHub caps page-number pagination on the issues endpoint (HTTP 422 past
    // its offset limit), so following these cursor URLs is the only way to walk
    // a large result set.
    describe('parseNextLink', () => {
        it('extracts the rel="next" URL', () => {
            const header = '<https://api.github.com/repositories/1/issues?per_page=100&after=CURSOR>; rel="next"';
            expect(Utils.parseNextLink(header))
                .toBe('https://api.github.com/repositories/1/issues?per_page=100&after=CURSOR');
        });

        it('picks next out of a multi-rel header', () => {
            const header = '<https://api.example/prev>; rel="prev", <https://api.example/next>; rel="next", <https://api.example/last>; rel="last"';
            expect(Utils.parseNextLink(header)).toBe('https://api.example/next');
        });

        it('returns null when there is no next page', () => {
            const header = '<https://api.example/first>; rel="first", <https://api.example/prev>; rel="prev"';
            expect(Utils.parseNextLink(header)).toBeNull();
        });

        it('returns null for empty, undefined or null headers', () => {
            expect(Utils.parseNextLink(undefined)).toBeNull();
            expect(Utils.parseNextLink(null)).toBeNull();
            expect(Utils.parseNextLink('')).toBeNull();
        });

        it('preserves the cursor query string verbatim', () => {
            const url = 'https://api.github.com/repositories/74175805/issues?per_page=100&state=all&since=2024-01-01T00%3A00%3A00Z&after=Y3Vyc29yOnYyOpLPAAAB';
            expect(Utils.parseNextLink(`<${url}>; rel="next"`)).toBe(url);
        });
    });

    // ─── stripLoneSurrogates ────────────────────────────────────────
    // A lone surrogate is invalid UTF-8, so Qdrant rejects the whole JSON body
    // ("lone leading surrogate in hex escape") and the chunk is lost.
    describe('stripLoneSurrogates', () => {
        it('removes a lone high surrogate', () => {
            const text = 'before \ud83d after';
            const cleaned = Utils.stripLoneSurrogates(text);
            expect(cleaned).toBe('before  after');
            expect(cleaned.isWellFormed?.()).not.toBe(false);
        });

        it('removes a lone low surrogate', () => {
            const cleaned = Utils.stripLoneSurrogates('before \ude00 after');
            expect(cleaned).toBe('before  after');
            expect(cleaned.isWellFormed?.()).not.toBe(false);
        });

        it('keeps valid surrogate pairs intact', () => {
            const emoji = 'ok \ud83d\ude00 done';
            expect(Utils.stripLoneSurrogates(emoji)).toBe(emoji);
        });

        it('keeps plain text unchanged', () => {
            expect(Utils.stripLoneSurrogates('plain ascii + accents éàü + 日本語')).toBe('plain ascii + accents éàü + 日本語');
        });

        it('handles a lone surrogate adjacent to a valid pair', () => {
            const cleaned = Utils.stripLoneSurrogates('\ud83d\ud83d\ude00');
            expect(cleaned).toBe('\ud83d\ude00');
            expect(cleaned.isWellFormed?.()).not.toBe(false);
        });

        it('produces JSON-safe output for content that broke the upsert', () => {
            const broken = 'x'.repeat(10) + '\ud83d';
            expect(JSON.parse(JSON.stringify({ c: Utils.stripLoneSurrogates(broken) })).c).toBe('x'.repeat(10));
        });
    });

    // ─── sliceSafe ──────────────────────────────────────────────────
    // The chunker splits oversized sections by character offset; a boundary
    // landing inside a surrogate pair used to emit two broken chunks.
    describe('sliceSafe', () => {
        it('never splits a surrogate pair at the end boundary', () => {
            // Pair straddles index 4/5
            const text = 'abcd\ud83d\ude00efg';
            const slice = Utils.sliceSafe(text, 0, 5);
            expect(slice).toBe('abcd');
            expect(slice.isWellFormed?.()).not.toBe(false);
        });

        it('picks up the whole pair the previous slice left behind', () => {
            const text = 'abcd\ud83d\ude00efg';
            // Index 5 is the low half; the slice steps back so the pair is intact
            const slice = Utils.sliceSafe(text, 5, text.length);
            expect(slice).toBe('\ud83d\ude00efg');
            expect(slice.isWellFormed?.()).not.toBe(false);
        });

        it('behaves like slice when no pair is straddled', () => {
            expect(Utils.sliceSafe('hello world', 0, 5)).toBe('hello');
            expect(Utils.sliceSafe('hello world', 6, 11)).toBe('world');
        });

        it('keeps a pair whole when it sits fully inside the range', () => {
            const text = 'ab\ud83d\ude00cd';
            expect(Utils.sliceSafe(text, 0, text.length)).toBe(text);
        });

        it('clamps out-of-range bounds', () => {
            expect(Utils.sliceSafe('abc', -5, 99)).toBe('abc');
        });

        it('reassembles the original text when slicing contiguously across a pair', () => {
            const text = 'aaaa\ud83d\ude00bbbb';
            const first = Utils.sliceSafe(text, 0, 5);
            const second = Utils.sliceSafe(text, 5, text.length);
            expect(first + second).toBe(text);
            expect((first + second).isWellFormed?.()).not.toBe(false);
        });
    });
});
