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
});
