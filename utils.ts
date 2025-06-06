import crypto from 'crypto';
import * as path from 'path';

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

    static buildUrl(href: string, currentUrl: string): string {
        try {
            return new URL(href, currentUrl).toString();
        } catch (error) {
            console.warn(`Invalid URL found: ${href}`);
            return '';
        }
    }

    static shouldProcessUrl(url: string): boolean {
        const parsedUrl = new URL(url);
        const pathname = parsedUrl.pathname;
        const ext = path.extname(pathname);

        if (!ext) return true;
        return ['.html', '.htm'].includes(ext.toLowerCase());
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
} 