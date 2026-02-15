import { Readability } from '@mozilla/readability';
import axios from 'axios';
import { load } from 'cheerio';
import { JSDOM } from 'jsdom';
import puppeteer, { Browser, Page } from 'puppeteer';
import sanitizeHtml from 'sanitize-html';
import TurndownService from 'turndown';
import * as fs from 'fs';
import * as path from 'path';
import { Logger } from './logger';
import { Utils } from './utils';
import {
    SourceConfig,
    WebsiteSourceConfig,
    LocalDirectorySourceConfig,
    CodeSourceConfig,
    DocumentChunk,
    BrokenLink
} from './types';
import type { TokenChunker, Tokenizer } from '@chonkiejs/core';
import { CodeChunker } from './code-chunker';

export class ContentProcessor {
    private turndownService: TurndownService;
    private logger: Logger;
    private tokenChunkerCache: Map<string, Promise<TokenChunker>>;
    private codeChunkerCache: Map<string, Promise<CodeChunker>>;
    private tokenizerCache: Promise<Tokenizer> | null;

    constructor(logger: Logger) {
        this.logger = logger;
        this.turndownService = new TurndownService({
            codeBlockStyle: 'fenced',
            headingStyle: 'atx'
        });
        this.tokenChunkerCache = new Map();
        this.codeChunkerCache = new Map();
        this.tokenizerCache = null;
        this.setupTurndownRules();
    }

    private setupTurndownRules() {
        const logger = this.logger.child('markdown');
        logger.debug('Setting up Turndown rules for markdown conversion');
        
        this.turndownService.addRule('codeBlocks', {
            filter: (node: Node): boolean => node.nodeName === 'PRE',
            replacement: (content: string, node: Node): string => {
                const htmlNode = node as HTMLElement;
                const code = htmlNode.querySelector('code');

                let codeContent;
                if (code) {
                    codeContent = code.textContent || '';
                } else {
                    codeContent = htmlNode.textContent || '';
                }

                const lines = codeContent.split('\n');
                let minIndent = Infinity;
                for (const line of lines) {
                    if (line.trim() === '') continue;
                    const leadingWhitespace = line.match(/^\s*/)?.[0] || '';
                    minIndent = Math.min(minIndent, leadingWhitespace.length);
                }

                const cleanedLines = lines.map(line => {
                    return line.substring(minIndent);
                });

                let cleanContent = cleanedLines.join('\n');
                cleanContent = cleanContent.replace(/^\s+|\s+$/g, '');
                cleanContent = cleanContent.replace(/\n{2,}/g, '\n');

                return `\n\`\`\`\n${cleanContent}\n\`\`\`\n`;
            }
        });

        this.turndownService.addRule('tableCell', {
            filter: ['th', 'td'],
            replacement: (content: string, node: Node): string => {
                const htmlNode = node as HTMLElement;

                let cellContent = '';
                if (htmlNode.querySelector('p')) {
                    cellContent = Array.from(htmlNode.querySelectorAll('p'))
                        .map(p => p.textContent || '')
                        .join(' ')
                        .trim();
                } else {
                    cellContent = content.trim();
                }

                return ` ${cellContent.replace(/\|/g, '\\|')} |`;
            }
        });

        this.turndownService.addRule('tableRow', {
            filter: 'tr',
            replacement: (content: string, node: Node): string => {
                const htmlNode = node as HTMLTableRowElement;
                const cells = Array.from(htmlNode.cells);
                const isHeader = htmlNode.parentNode?.nodeName === 'THEAD';

                let output = '|' + content.trimEnd();

                if (isHeader) {
                    const separator = cells.map(() => '---').join(' | ');
                    output += '\n|' + separator + '|';
                }

                if (!isHeader || !htmlNode.nextElementSibling) {
                    output += '\n';
                }

                return output;
            }
        });

        this.turndownService.addRule('table', {
            filter: 'table',
            replacement: (content: string): string => {
                return '\n' + content.replace(/\n+/g, '\n').trim() + '\n';
            }
        });

        this.turndownService.addRule('preserveTableWhitespace', {
            filter: (node: Node): boolean => {
                return (
                    (node.nodeName === 'TD' || node.nodeName === 'TH') &&
                    (node.textContent?.trim().length === 0)
                );
            },
            replacement: (): string => {
                return ' |';
            }
        });
        
        logger.debug('Turndown rules setup complete');
    }

    public convertHtmlToMarkdown(html: string): string {
        if (!html || !html.trim()) {
            return '';
        }

        // Pre-process tabbed content before sanitization strips ARIA attributes
        const dom = new JSDOM(html);
        const tabButtons = dom.window.document.querySelectorAll('[role="tab"]');
        if (tabButtons.length > 0) {
            const logger = this.logger.child('markdown');
            this.preprocessTabs(dom.window.document, logger);
            html = dom.window.document.body.innerHTML;
        }

        // Sanitize the HTML first
        const cleanHtml = sanitizeHtml(html, {
            allowedTags: [
                'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a', 'ul', 'ol',
                'li', 'b', 'i', 'strong', 'em', 'code', 'pre',
                'div', 'span', 'table', 'thead', 'tbody', 'tr', 'th', 'td',
                'blockquote', 'br'
            ],
            allowedAttributes: {
                'a': ['href'],
                'pre': ['class', 'data-language'],
                'code': ['class', 'data-language'],
                'div': ['class'],
                'span': ['class']
            }
        });

        // Convert to markdown using TurndownService
        return this.turndownService.turndown(cleanHtml).trim();
    }

    async parseSitemap(sitemapUrl: string, logger: Logger): Promise<string[]> {
        logger.info(`Parsing sitemap from ${sitemapUrl}`);
        try {
            const response = await axios.get(sitemapUrl);
            const $ = load(response.data, { xmlMode: true });
            
            const urls: string[] = [];
            
            // Handle standard sitemaps
            $('url > loc').each((_, element) => {
                const url = $(element).text().trim();
                if (url) {
                    urls.push(url);
                }
            });
            
            // Handle sitemap indexes (sitemaps that link to other sitemaps)
            const sitemapLinks: string[] = [];
            $('sitemap > loc').each((_, element) => {
                const nestedSitemapUrl = $(element).text().trim();
                if (nestedSitemapUrl) {
                    sitemapLinks.push(nestedSitemapUrl);
                }
            });
            
            // Recursively process nested sitemaps
            for (const nestedSitemapUrl of sitemapLinks) {
                logger.debug(`Found nested sitemap: ${nestedSitemapUrl}`);
                const nestedUrls = await this.parseSitemap(nestedSitemapUrl, logger);
                urls.push(...nestedUrls);
            }
            
            logger.info(`Found ${urls.length} URLs in sitemap ${sitemapUrl}`);
            return urls;
        } catch (error) {
            logger.error(`Error parsing sitemap at ${sitemapUrl}:`, error);
            return [];
        }
    }

    async crawlWebsite(
        baseUrl: string,
        sourceConfig: WebsiteSourceConfig,
        processPageContent: (url: string, content: string) => Promise<void>,
        parentLogger: Logger,
        visitedUrls: Set<string>
    ): Promise<{ hasNetworkErrors: boolean; brokenLinks: BrokenLink[] }> {
        const logger = parentLogger.child('crawler');
        const queue: string[] = [baseUrl];
        const brokenLinks: BrokenLink[] = [];
        const brokenLinkKeys: Set<string> = new Set();
        const referrers: Map<string, Set<string>> = new Map();

        const addReferrer = (targetUrl: string, sourceUrl: string) => {
            const existing = referrers.get(targetUrl);
            if (existing) {
                existing.add(sourceUrl);
            } else {
                referrers.set(targetUrl, new Set([sourceUrl]));
            }
        };

        const addBrokenLink = (sourceUrl: string, targetUrl: string) => {
            const key = `${sourceUrl} -> ${targetUrl}`;
            if (brokenLinkKeys.has(key)) return;
            brokenLinkKeys.add(key);
            brokenLinks.push({ source: sourceUrl, target: targetUrl });
        };

        addReferrer(baseUrl, baseUrl);
        
        // Process sitemap if provided
        if (sourceConfig.sitemap_url) {
            logger.section('SITEMAP PROCESSING');
            const sitemapUrls = await this.parseSitemap(sourceConfig.sitemap_url, logger);
            
            // Add sitemap URLs to the queue if they're within the website scope
            for (const url of sitemapUrls) {
                if (url.startsWith(sourceConfig.url)) {
                    addReferrer(url, sourceConfig.sitemap_url);
                    if (!queue.includes(url)) {
                        logger.debug(`Adding URL from sitemap to queue: ${url}`);
                        queue.push(url);
                    }
                }
            }
            
            logger.info(`Added ${queue.length - 1} URLs from sitemap to the crawl queue`);
        }

        logger.info(`Starting crawl from ${baseUrl} with ${queue.length} URLs in initial queue`);
        let processedCount = 0;
        let skippedCount = 0;
        let skippedSizeCount = 0;
        let pdfProcessedCount = 0;
        let errorCount = 0;
        let hasNetworkErrors = false;

        // Launch a single browser instance and reuse one page (tab) for all URLs
        let browser: Browser | null = null;
        let page: Page | null = null;

        const launchBrowser = async (): Promise<{ browser: Browser; page: Page }> => {
            let executablePath: string | undefined = process.env.PUPPETEER_EXECUTABLE_PATH;
            if (!executablePath) {
                if (fs.existsSync('/usr/bin/chromium')) {
                    executablePath = '/usr/bin/chromium';
                } else if (fs.existsSync('/usr/bin/chromium-browser')) {
                    executablePath = '/usr/bin/chromium-browser';
                }
            }
            const b = await puppeteer.launch({
                executablePath,
                args: ['--no-sandbox', '--disable-setuid-sandbox'],
                protocolTimeout: 60000,
            });
            const p = await b.newPage();
            return { browser: b, page: p };
        };

        // Track whether the page needs to be recreated (e.g., after a timeout error)
        let pageNeedsRecreation = false;

        const ensureBrowser = async (): Promise<Page> => {
            if (!browser || !browser.isConnected()) {
                logger.info(browser ? 'Browser disconnected, relaunching...' : 'Launching browser...');
                const launched = await launchBrowser();
                browser = launched.browser;
                page = launched.page;
                pageNeedsRecreation = false;
            } else if (pageNeedsRecreation) {
                // The previous page.evaluate or navigation timed out / errored.
                // Close the stale page and create a fresh one to avoid corrupted state.
                logger.info('Recreating page after previous error...');
                try {
                    if (page) await page.close();
                } catch {
                    // Ignore errors closing a stale page
                }
                page = await browser.newPage();
                pageNeedsRecreation = false;
            }
            return page!;
        };

        try {
            while (queue.length > 0) {
                const url = queue.shift();
                if (!url) continue;

                const normalizedUrl = Utils.normalizeUrl(url);
                if (visitedUrls.has(normalizedUrl)) continue;
                visitedUrls.add(normalizedUrl);

                if (!Utils.shouldProcessUrl(url)) {
                    logger.debug(`Skipping URL with unsupported extension: ${url}`);
                    skippedCount++;
                    continue;
                }

                try {
                    logger.info(`Crawling: ${url}`);
                    const sources = referrers.get(url) ?? new Set([baseUrl]);

                    // For HTML pages, ensure the browser is running and pass the shared page
                    // For PDFs, processPage handles them without Puppeteer
                    const currentPage = Utils.isPdfUrl(url) ? undefined : await ensureBrowser();

                    const result = await this.processPage(url, sourceConfig, (reportedUrl, status) => {
                        if (status === 404) {
                            for (const source of sources) {
                                addBrokenLink(source, reportedUrl);
                            }
                        }
                    }, currentPage);

                    if (result.content !== null) {
                        await processPageContent(url, result.content);
                        if (Utils.isPdfUrl(url)) {
                            pdfProcessedCount++;
                        } else {
                            processedCount++;
                        }
                    } else {
                        skippedSizeCount++;
                    }

                    // Use links extracted from the full rendered DOM by processPage
                    // (no separate axios request needed)
                    if (result.links.length > 0) {
                        const pageUrlForLinks = result.finalUrl || url;
                        logger.debug(`Finding links on page ${url}`);
                        let newLinksFound = 0;

                        for (const href of result.links) {
                            const fullUrl = Utils.buildUrl(href, pageUrlForLinks);
                            if (fullUrl.startsWith(sourceConfig.url)) {
                                addReferrer(fullUrl, pageUrlForLinks);
                                if (!visitedUrls.has(Utils.normalizeUrl(fullUrl))) {
                                    if (!queue.includes(fullUrl)) {
                                        queue.push(fullUrl);
                                        newLinksFound++;
                                    }
                                }
                            }
                        }

                        logger.debug(`Found ${newLinksFound} new links on ${url}`);
                    }

                    // Navigate to about:blank to clear any lingering JS, timers, or
                    // event listeners from the processed page before moving to the next URL.
                    if (currentPage) {
                        try {
                            await currentPage.goto('about:blank', { waitUntil: 'load', timeout: 5000 });
                        } catch {
                            // If even about:blank fails, the page is stuck — mark for recreation
                            pageNeedsRecreation = true;
                        }
                    }
                } catch (error: any) {
                    logger.error(`Failed during processing or link discovery for ${url}:`, error);
                    errorCount++;

                    // Mark the page for recreation so the next URL gets a clean tab.
                    // This prevents cascading failures from a stuck/corrupted page state.
                    pageNeedsRecreation = true;

                    const status = this.getHttpStatus(error);
                    if (status === 404) {
                        const sources = referrers.get(url) ?? new Set([baseUrl]);
                        for (const source of sources) {
                            addBrokenLink(source, url);
                        }
                    }
                    
                    // Check if this is a network error (DNS resolution, connection issues, etc.)
                    if (this.isNetworkError(error)) {
                        hasNetworkErrors = true;
                        logger.warn(`Network error detected for ${url}, this may affect cleanup decisions`);
                    }
                }
            }
        } finally {
            // Close the shared browser instance when the crawl is done
            const browserToClose = browser as Browser | null;
            if (browserToClose && browserToClose.isConnected()) {
                await browserToClose.close();
                logger.debug('Shared browser closed after crawl completed');
            }
        }

        logger.info(`Crawl completed. HTML Pages: ${processedCount}, PDFs: ${pdfProcessedCount}, Skipped (Extension): ${skippedCount}, Skipped (Size): ${skippedSizeCount}, Errors: ${errorCount}`);
        
        if (hasNetworkErrors) {
            logger.warn('Network errors were encountered during crawling. Cleanup may be skipped to avoid removing valid chunks.');
        }
        
        return { hasNetworkErrors, brokenLinks };
    }

    private isNetworkError(error: any): boolean {
        // Check for common network error patterns
        if (error?.code) {
            // DNS resolution errors
            if (error.code === 'ENOTFOUND') return true;
            // Connection refused
            if (error.code === 'ECONNREFUSED') return true;
            // Connection timeout
            if (error.code === 'ETIMEDOUT') return true;
            // Connection reset
            if (error.code === 'ECONNRESET') return true;
            // Host unreachable
            if (error.code === 'EHOSTUNREACH') return true;
            // Network unreachable
            if (error.code === 'ENETUNREACH') return true;
        }
        
        // Check for axios-specific network errors
        if (error?.isAxiosError) {
            // If there's no response, it's likely a network error
            if (!error.response) return true;
        }
        
        // Check error message for network-related terms
        const errorMessage = error?.message?.toLowerCase() || '';
        if (errorMessage.includes('getaddrinfo') || 
            errorMessage.includes('network') || 
            errorMessage.includes('timeout') ||
            errorMessage.includes('connection') ||
            errorMessage.includes('dns')) {
            return true;
        }
        
        return false;
    }

    async processPage(
        url: string,
        sourceConfig: SourceConfig,
        onHttpStatus?: (url: string, status: number) => void,
        existingPage?: Page
    ): Promise<{ content: string | null, links: string[], finalUrl: string }> {
        const logger = this.logger.child('page-processor');
        logger.debug(`Processing content from ${url}`);

        // Check if this is a PDF URL
        if (Utils.isPdfUrl(url)) {
            logger.info(`Processing PDF: ${url}`);
            try {
                const markdown = await this.downloadAndConvertPdfFromUrl(url, logger);
                
                // Check size limit for PDF content
                if (markdown.length > sourceConfig.max_size) {
                    logger.warn(`PDF content (${markdown.length} chars) exceeds max size (${sourceConfig.max_size}). Skipping ${url}.`);
                    return { content: null, links: [], finalUrl: url };
                }
                
                return { content: markdown, links: [], finalUrl: url };
            } catch (error) {
                const status = this.getHttpStatus(error);
                if (status !== undefined && status >= 400) {
                    if (onHttpStatus) {
                        onHttpStatus(url, status);
                    }
                    throw error;
                }
                logger.error(`Failed to process PDF ${url}:`, error);
                return { content: null, links: [], finalUrl: url };
            }
        }

        // HTML page processing logic
        // If an existing page (tab) is provided, reuse it; otherwise launch a standalone browser
        let browser: Browser | null = null;
        let page: Page;
        const ownsTheBrowser = !existingPage;
        try {
            if (existingPage) {
                page = existingPage;
            } else {
                // Standalone mode: launch a browser for this single page
                let executablePath: string | undefined = process.env.PUPPETEER_EXECUTABLE_PATH;
                if (!executablePath) {
                    if (fs.existsSync('/usr/bin/chromium')) {
                        executablePath = '/usr/bin/chromium';
                    } else if (fs.existsSync('/usr/bin/chromium-browser')) {
                        executablePath = '/usr/bin/chromium-browser';
                    }
                }
                
                browser = await puppeteer.launch({
                    executablePath,
                    args: ['--no-sandbox', '--disable-setuid-sandbox'],
                    protocolTimeout: 60000,
                });
                page = await browser.newPage();
            }

            logger.debug(`Navigating to ${url}`);
            const response = await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
            const status = response?.status();
            if (status !== undefined && status >= 400) {
                const error = new Error(`Failed to load page: HTTP ${status}`);
                (error as any).status = status;
                throw error;
            }

            // Get the final URL after any redirects
            const finalUrl = page.url();

            // Extract ALL links from the full rendered DOM before any content filtering
            // This searches the entire document, not just the main content area
            const links: string[] = await this.evaluateWithTimeout(page, () => {
                const anchors = document.querySelectorAll('a[href]');
                const hrefs: string[] = [];
                anchors.forEach(a => {
                    const href = a.getAttribute('href');
                    if (href && !href.startsWith('#') && !href.startsWith('mailto:')) {
                        hrefs.push(href);
                    }
                });
                return hrefs;
            });
            logger.debug(`Extracted ${links.length} links from full DOM of ${url}`);

            const htmlContent: string = await this.evaluateWithTimeout(page, () => {
                // Try specific content selectors first, then fall back to broader ones
                const mainContentElement = 
                    document.querySelector('.docs-content') ||        // Common docs pattern
                    document.querySelector('.doc-content') ||         // Alternative docs pattern
                    document.querySelector('.markdown-body') ||       // GitHub-style
                    document.querySelector('article') ||              // Semantic article
                    document.querySelector('div[role="main"].document') || 
                    document.querySelector('main') || 
                    document.body;
                return mainContentElement.innerHTML;
            });

            if (htmlContent.length > sourceConfig.max_size) {
                logger.warn(`Raw HTML content (${htmlContent.length} chars) exceeds max size (${sourceConfig.max_size}). Skipping detailed processing for ${url}.`);
                return { content: null, links, finalUrl };
            }

            logger.debug(`Got HTML content (${htmlContent.length} chars), creating DOM`);
            const dom = new JSDOM(htmlContent);
            const document = dom.window.document;

            document.querySelectorAll('pre').forEach((pre: HTMLElement) => {
                pre.classList.add('article-content');
                pre.setAttribute('data-readable-content-score', '100');
                this.markCodeParents(pre.parentElement);
            });

            // Pre-process tabbed content: inject tab labels into panels
            // and make hidden panels visible so Readability doesn't discard them.
            // Done in JSDOM (not in Puppeteer's page.evaluate) to avoid triggering
            // reactive framework re-renders when modifying data-state attributes.
            this.preprocessTabs(document, logger);

            // Extract H1s BEFORE Readability - it often strips them as "chrome"
            // We'll inject them back after Readability processing
            const h1Elements = document.querySelectorAll('h1');
            const extractedH1s: string[] = [];
            logger.debug(`[Readability Debug] Found ${h1Elements.length} H1 elements before Readability`);
            h1Elements.forEach((h1: Element, index: number) => {
                const h1Text = h1.textContent?.trim() || '';
                // Skip empty H1s or icon-only H1s (like "link" anchors)
                if (h1Text && h1Text.length > 3 && !h1Text.match(/^(link|#|menu|close)$/i)) {
                    extractedH1s.push(h1Text);
                    logger.debug(`[Readability Debug] Extracted H1[${index}]: "${h1Text.substring(0, 50)}..."`);
                }
                h1.classList.add('original-h1');
            });

            logger.debug(`Applying Readability to extract main content`);
            const reader = new Readability(document, {
                charThreshold: 20,
                classesToPreserve: ['article-content', 'original-h1'],
            });
            const article = reader.parse();

            if (!article) {
                logger.warn(`Failed to parse article content with Readability for ${url}`);
                return { content: null, links, finalUrl };
            }
            
            // Debug: Log what Readability extracted
            logger.debug(`[Readability Debug] article.title: "${article.title}"`);
            logger.debug(`[Readability Debug] article.content length: ${article.content?.length}`);
            logger.debug(`[Readability Debug] article.content starts with: "${article.content?.substring(0, 200)}..."`);
            logger.debug(`[Readability Debug] Contains H1 tag: ${article.content?.includes('<h1')}`);
            logger.debug(`[Readability Debug] Contains H2 tag: ${article.content?.includes('<h2')}`);
            logger.debug(`[Readability Debug] Contains original-h1 class: ${article.content?.includes('original-h1')}`);

            // Restore H1s: find elements with our marker class and convert back from H2
            const articleDom = new JSDOM(article.content);
            const articleDoc = articleDom.window.document;
            const originalH1Elements = articleDoc.querySelectorAll('.original-h1');
            logger.debug(`[Readability Debug] Found ${originalH1Elements.length} elements with .original-h1 class to restore`);
            originalH1Elements.forEach((heading: Element, index: number) => {
                logger.debug(`[Readability Debug] Restoring[${index}]: tagName=${heading.tagName}, text="${heading.textContent?.trim().substring(0, 50)}..."`);
                // Create a new H1 element with the same content
                const h1 = articleDoc.createElement('h1');
                h1.innerHTML = heading.innerHTML;
                // Copy other attributes except class
                Array.from(heading.attributes).forEach(attr => {
                    if (attr.name !== 'class') {
                        h1.setAttribute(attr.name, attr.value);
                    }
                });
                heading.replaceWith(h1);
            });
            const restoredContent = articleDoc.body.innerHTML;
            logger.debug(`[Readability Debug] Restored content contains H1: ${restoredContent.includes('<h1')}`);

            logger.debug(`Sanitizing HTML (${restoredContent.length} chars)`);
            const cleanHtml = sanitizeHtml(restoredContent, {
                 allowedTags: [
                    'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a', 'ul', 'ol',
                    'li', 'b', 'i', 'strong', 'em', 'code', 'pre',
                    'div', 'span', 'table', 'thead', 'tbody', 'tr', 'th', 'td'
                ],
                allowedAttributes: {
                    'a': ['href'],
                    'pre': ['class', 'data-language'],
                    'code': ['class', 'data-language'],
                    'div': ['class'],
                    'span': ['class']
                }
            });

            logger.debug(`Converting HTML to Markdown`);
            let markdown = this.turndownService.turndown(cleanHtml);
            
            // Inject extracted H1s back if they're not in the markdown
            // Readability often strips them as "page chrome"
            // Use article.title as fallback if no H1 was extracted
            const pageTitle = extractedH1s.length > 0 ? extractedH1s[0] : (article.title?.trim() || '');
            if (pageTitle) {
                // Check if markdown already starts with this exact H1 (allowing for leading whitespace)
                const normalizedTitle = pageTitle.replace(/\s+/g, ' ');
                const markdownFirstLine = markdown.trimStart().split('\n')[0] || '';
                const existingH1Match = markdownFirstLine.match(/^#\s+(.+)$/);
                const existingH1Text = existingH1Match ? existingH1Match[1].replace(/\s+/g, ' ').trim() : '';
                
                // Only inject if markdown doesn't already start with this H1
                if (!existingH1Match || existingH1Text !== normalizedTitle) {
                    markdown = `# ${pageTitle}\n\n${markdown}`;
                    logger.debug(`[Readability Debug] Injected page title as H1: "${pageTitle}"`);
                } else {
                    logger.debug(`[Readability Debug] H1 "${pageTitle}" already present in markdown`);
                }
            }
            
            logger.debug(`Markdown conversion complete (${markdown.length} chars)`);
            return { content: markdown, links, finalUrl };
        } catch (error) {
            const status = this.getHttpStatus(error);
            if (status !== undefined && status >= 400) {
                if (onHttpStatus) {
                    onHttpStatus(url, status);
                }
                throw error;
            }
            logger.error(`Error processing page ${url}:`, error);
            return { content: null, links: [], finalUrl: url };
        } finally {
            // Only close the browser if we launched it ourselves (standalone mode)
            if (ownsTheBrowser && browser && browser.isConnected()) {
                 await browser.close();
                 logger.debug(`Browser closed for ${url}`);
            }
        }
    }

    /**
     * Wraps a page.evaluate call with a timeout to prevent indefinite hangs.
     * Some pages have heavy/infinite JavaScript that blocks evaluate calls.
     */
    private evaluateWithTimeout<T>(page: Page, fn: () => T, timeoutMs: number = 30000): Promise<T> {
        return Promise.race([
            page.evaluate(fn),
            new Promise<never>((_, reject) =>
                setTimeout(() => reject(new Error(`page.evaluate timed out after ${timeoutMs}ms`)), timeoutMs)
            )
        ]);
    }

    private getHttpStatus(error: any): number | undefined {
        if (typeof error?.status === 'number') {
            return error.status;
        }

        if (typeof error?.response?.status === 'number') {
            return error.response.status;
        }

        return undefined;
    }

    /**
     * Pre-processes tabbed content in the DOM before Readability runs.
     * 
     * Detects tabs using the standard WAI-ARIA tabs pattern:
     * - Tab buttons: elements with role="tab" and aria-controls pointing to a panel
     * - Tab panels: elements with role="tabpanel" matched by id
     * 
     * For each tab/panel pair, injects the tab label as a bold heading into
     * the panel content, so the context is preserved after conversion to markdown.
     * Also ensures hidden panels are visible so Readability doesn't discard them.
     * 
     * Falls back to positional matching (nth tab -> nth panel) when aria-controls
     * is missing.
     */
    private preprocessTabs(document: Document, logger: Logger) {
        // Find all tab buttons using the WAI-ARIA role="tab" attribute
        const tabButtons = document.querySelectorAll('[role="tab"]');
        if (tabButtons.length === 0) return;

        logger.debug(`[Tab Preprocessing] Found ${tabButtons.length} tab buttons`);

        // Collect all tabpanels for fallback positional matching
        const allPanels = document.querySelectorAll('[role="tabpanel"]');

        // Track panels that have already been labeled to avoid duplicates.
        // Pages can have multiple tab groups that reuse the same panel IDs
        // (e.g., two separate tab sets both using tabs-panel-0, tabs-panel-1, etc.),
        // causing getElementById to return the same panel for different tab buttons.
        const labeledPanels = new Set<Element>();

        tabButtons.forEach((tab: Element, index: number) => {
            const label = tab.textContent?.trim();
            if (!label) {
                logger.debug(`[Tab Preprocessing] Skipping tab[${index}] with empty label`);
                return;
            }

            // Try to find the linked panel via aria-controls -> id
            let panel: Element | null = null;
            const controlsId = tab.getAttribute('aria-controls');
            if (controlsId) {
                panel = document.getElementById(controlsId);
            }

            // Fallback: positional matching (nth tab -> nth panel)
            if (!panel && index < allPanels.length) {
                panel = allPanels[index];
                logger.debug(`[Tab Preprocessing] Using positional fallback for tab[${index}]: "${label}"`);
            }

            if (!panel) {
                logger.debug(`[Tab Preprocessing] No panel found for tab[${index}]: "${label}"`);
                return;
            }

            // Skip if this panel has already been labeled (duplicate panel ID across tab groups)
            if (labeledPanels.has(panel)) {
                logger.debug(`[Tab Preprocessing] Skipping tab[${index}]: "${label}" — panel already labeled`);
                return;
            }
            labeledPanels.add(panel);

            logger.debug(`[Tab Preprocessing] Injecting label for tab[${index}]: "${label}"`);

            // Inject the tab label as bold text at the top of the panel
            const labelElement = document.createElement('p');
            const strong = document.createElement('strong');
            strong.textContent = label + ':';
            labelElement.appendChild(strong);
            panel.insertBefore(labelElement, panel.firstChild);

            // Ensure the panel is visible so Readability doesn't skip it.
            // Remove common hiding patterns:
            // 1. data-state attribute (used by Hextra, Radix, etc.)
            if (panel.getAttribute('data-state') !== 'selected') {
                panel.setAttribute('data-state', 'selected');
            }
            // 2. CSS classes that hide content
            const classList = panel.classList;
            // Common hiding classes across frameworks
            const hidingClasses = ['hidden', 'hx-hidden', 'is-hidden', 'display-none', 'd-none', 'invisible'];
            hidingClasses.forEach(cls => classList.remove(cls));
            // Also remove any class containing 'hidden' as a segment (e.g., 'hx-hidden')
            Array.from(classList).forEach(cls => {
                if (/\bhidden\b/i.test(cls)) {
                    classList.remove(cls);
                }
            });
            // 3. Inline style hiding
            const style = panel.getAttribute('style') || '';
            if (style.includes('display') && style.includes('none')) {
                panel.setAttribute('style', style.replace(/display\s*:\s*none\s*;?/gi, ''));
            }

            // Mark the panel as content so Readability preserves it
            panel.classList.add('article-content');
            panel.setAttribute('data-readable-content-score', '100');
        });

        // Remove the tab buttons since we've injected the labels into panels
        // This prevents the tab labels from appearing twice in the output
        tabButtons.forEach((tab: Element) => {
            tab.remove();
        });

        logger.debug(`[Tab Preprocessing] Completed tab preprocessing`);
    }

    private markCodeParents(node: Element | null) {
        if (!node) return;

        if (node.querySelector('pre, code')) {
            node.classList.add('article-content');
            node.setAttribute('data-readable-content-score', '100');
        }
        this.markCodeParents(node.parentElement);
    }

    private async convertDocToMarkdown(filePath: string, logger: Logger): Promise<string> {
        logger.debug(`Converting DOC to markdown: ${filePath}`);
        
        try {
            // Dynamic import for word-extractor
            const WordExtractor = (await import('word-extractor')).default;
            const extractor = new WordExtractor();
            
            const extracted = await extractor.extract(filePath);
            const text = extracted.getBody();
            
            // Create markdown with filename as title
            let markdown = `# ${path.basename(filePath, '.doc')}\n\n`;
            
            // Clean up the text and add to markdown
            const cleanedText = text
                .replace(/\r\n/g, '\n')  // Normalize line endings
                .replace(/\n{3,}/g, '\n\n')  // Remove excessive line breaks
                .trim();
            
            markdown += cleanedText;
            
            logger.debug(`Converted DOC to ${markdown.length} characters of markdown`);
            return markdown;
            
        } catch (error) {
            logger.error(`Failed to convert DOC ${filePath}:`, error);
            throw error;
        }
    }

    private async convertDocxToMarkdown(filePath: string, logger: Logger): Promise<string> {
        logger.debug(`Converting DOCX to markdown: ${filePath}`);
        
        try {
            // Dynamic import for mammoth
            const mammoth = await import('mammoth');
            
            const result = await mammoth.convertToHtml({ path: filePath });
            const html = result.value;
            
            // Log any warnings from mammoth
            if (result.messages.length > 0) {
                logger.debug(`Mammoth warnings: ${result.messages.map(m => m.message).join(', ')}`);
            }
            
            // Create markdown with filename as title
            let markdown = `# ${path.basename(filePath, '.docx')}\n\n`;
            
            // Convert HTML to Markdown using turndown
            const cleanHtml = sanitizeHtml(html, {
                allowedTags: [
                    'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a', 'ul', 'ol',
                    'li', 'b', 'i', 'strong', 'em', 'code', 'pre',
                    'div', 'span', 'table', 'thead', 'tbody', 'tr', 'th', 'td', 'br'
                ],
                allowedAttributes: {
                    'a': ['href'],
                    'pre': ['class'],
                    'code': ['class']
                }
            });
            
            const convertedContent = this.turndownService.turndown(cleanHtml);
            markdown += convertedContent;
            
            // Clean up excessive line breaks
            markdown = markdown.replace(/\n{3,}/g, '\n\n').trim();
            
            logger.debug(`Converted DOCX to ${markdown.length} characters of markdown`);
            return markdown;
            
        } catch (error) {
            logger.error(`Failed to convert DOCX ${filePath}:`, error);
            throw error;
        }
    }

    private async convertPdfToMarkdown(filePath: string, logger: Logger): Promise<string> {
        logger.debug(`Converting PDF to markdown: ${filePath}`);
        
        try {
            // Dynamic import for PDF.js to handle ES module compatibility
            const pdfjsLib = await import('pdfjs-dist/legacy/build/pdf.mjs');
            
            // Read the PDF file as a buffer and convert to Uint8Array
            const pdfBuffer = fs.readFileSync(filePath);
            const pdfData = new Uint8Array(pdfBuffer);
            
            // Load the PDF document
            const loadingTask = pdfjsLib.getDocument({
                data: pdfData,
                // Disable worker to avoid issues in Node.js environment
                useWorkerFetch: false,
                isEvalSupported: false,
                useSystemFonts: true
            });
            
            const pdfDocument = await loadingTask.promise;
            const numPages = pdfDocument.numPages;
            
            logger.debug(`PDF has ${numPages} pages`);
            
            let markdown = `# ${path.basename(filePath, '.pdf')}\n\n`;
            
            // Extract text from each page
            for (let pageNum = 1; pageNum <= numPages; pageNum++) {
                const page = await pdfDocument.getPage(pageNum);
                const textContent = await page.getTextContent();
                
                // Combine text items into a readable format
                let pageText = '';
                let currentY = -1;
                
                for (const item of textContent.items) {
                    if ('str' in item) {
                        // If this is a new line (different Y position), add a line break
                        if (currentY !== -1 && Math.abs(item.transform[5] - currentY) > 5) {
                            pageText += '\n';
                        }
                        
                        pageText += item.str;
                        
                        // Add space if the next item doesn't start immediately after this one
                        if ('width' in item && item.width > 0) {
                            pageText += ' ';
                        }
                        
                        currentY = item.transform[5];
                    }
                }
                
                // Clean up the text
                pageText = pageText
                    .replace(/\s+/g, ' ') // Replace multiple whitespace with single space
                    .replace(/\n\s+/g, '\n') // Clean up line starts
                    .trim();
                
                if (pageText.length > 0) {
                    if (numPages > 1) {
                        markdown += `## Page ${pageNum}\n\n`;
                    }
                    markdown += pageText + '\n\n';
                }
            }
            
            // Clean up the final markdown
            markdown = markdown.replace(/\n{3,}/g, '\n\n').trim();
            
            logger.debug(`Converted PDF to ${markdown.length} characters of markdown`);
            return markdown;
            
        } catch (error) {
            logger.error(`Failed to convert PDF ${filePath}:`, error);
            throw error;
        }
    }

    private async downloadAndConvertPdfFromUrl(url: string, logger: Logger): Promise<string> {
        logger.debug(`Downloading and converting PDF from URL: ${url}`);
        
        try {
            // Download the PDF file
            const response = await axios.get(url, {
                responseType: 'arraybuffer',
                timeout: 60000, // 60 second timeout
                headers: {
                    'User-Agent': 'Mozilla/5.0 (compatible; doc2vec PDF processor)'
                }
            });
            
            if (response.status !== 200) {
                throw new Error(`Failed to download PDF: HTTP ${response.status}`);
            }
            
            logger.debug(`Downloaded PDF (${response.data.byteLength} bytes)`);
            
            // Dynamic import for PDF.js to handle ES module compatibility
            const pdfjsLib = await import('pdfjs-dist/legacy/build/pdf.mjs');
            
            // Convert ArrayBuffer to Uint8Array
            const pdfData = new Uint8Array(response.data);
            
            // Load the PDF document
            const loadingTask = pdfjsLib.getDocument({
                data: pdfData,
                // Disable worker to avoid issues in Node.js environment
                useWorkerFetch: false,
                isEvalSupported: false,
                useSystemFonts: true
            });
            
            const pdfDocument = await loadingTask.promise;
            const numPages = pdfDocument.numPages;
            
            logger.debug(`PDF has ${numPages} pages`);
            
            // Get the filename from URL for the title
            const urlPath = new URL(url).pathname;
            const filename = path.basename(urlPath, '.pdf') || 'document';
            let markdown = `# ${filename}\n\n`;
            
            // Extract text from each page
            for (let pageNum = 1; pageNum <= numPages; pageNum++) {
                const page = await pdfDocument.getPage(pageNum);
                const textContent = await page.getTextContent();
                
                // Combine text items into a readable format
                let pageText = '';
                let currentY = -1;
                
                for (const item of textContent.items) {
                    if ('str' in item) {
                        // If this is a new line (different Y position), add a line break
                        if (currentY !== -1 && Math.abs(item.transform[5] - currentY) > 5) {
                            pageText += '\n';
                        }
                        
                        pageText += item.str;
                        
                        // Add space if the next item doesn't start immediately after this one
                        if ('width' in item && item.width > 0) {
                            pageText += ' ';
                        }
                        
                        currentY = item.transform[5];
                    }
                }
                
                // Clean up the text
                pageText = pageText
                    .replace(/\s+/g, ' ') // Replace multiple whitespace with single space
                    .replace(/\n\s+/g, '\n') // Clean up line starts
                    .trim();
                
                if (pageText.length > 0) {
                    if (numPages > 1) {
                        markdown += `## Page ${pageNum}\n\n`;
                    }
                    markdown += pageText + '\n\n';
                }
            }
            
            // Clean up the final markdown
            markdown = markdown.replace(/\n{3,}/g, '\n\n').trim();
            
            logger.debug(`Converted PDF to ${markdown.length} characters of markdown`);
            return markdown;
            
        } catch (error) {
            logger.error(`Failed to download and convert PDF from ${url}:`, error);
            throw error;
        }
    }

    async processDirectory(
        dirPath: string,
        config: LocalDirectorySourceConfig,
        processFileContent: (filePath: string, content: string) => Promise<void>,
        parentLogger: Logger,
        visitedPaths: Set<string> = new Set()
    ): Promise<void> {
        const logger = parentLogger.child('directory-processor');
        logger.info(`Processing directory: ${dirPath}`);
        
        const recursive = config.recursive !== undefined ? config.recursive : true;
        const includeExtensions = config.include_extensions || ['.md', '.txt', '.html', '.htm', '.pdf'];
        const excludeExtensions = config.exclude_extensions || [];
        const encoding = config.encoding || 'utf8' as BufferEncoding;
        
        try {
            const files = fs.readdirSync(dirPath);
            let processedFiles = 0;
            let skippedFiles = 0;
            
            for (const file of files) {
                const filePath = path.join(dirPath, file);
                const stat = fs.statSync(filePath);
                
                // Skip already visited paths
                if (visitedPaths.has(filePath)) {
                    logger.debug(`Skipping already visited path: ${filePath}`);
                    continue;
                }
                
                visitedPaths.add(filePath);
                
                if (stat.isDirectory()) {
                    if (recursive) {
                        await this.processDirectory(filePath, config, processFileContent, logger, visitedPaths);
                    } else {
                        logger.debug(`Skipping directory ${filePath} (recursive=false)`);
                    }
                } else if (stat.isFile()) {
                    const extension = path.extname(file).toLowerCase();
                    
                    // Apply extension filters
                    if (excludeExtensions.includes(extension)) {
                        logger.debug(`Skipping file with excluded extension: ${filePath}`);
                        skippedFiles++;
                        continue;
                    }
                    
                    if (includeExtensions.length > 0 && !includeExtensions.includes(extension)) {
                        logger.debug(`Skipping file with non-included extension: ${filePath}`);
                        skippedFiles++;
                        continue;
                    }
                    
                    try {
                        logger.info(`Reading file: ${filePath}`);
                        
                        let content: string;
                        let processedContent: string;
                        
                        if (extension === '.pdf') {
                            // Handle PDF files
                            logger.debug(`Processing PDF file: ${filePath}`);
                            processedContent = await this.convertPdfToMarkdown(filePath, logger);
                        } else if (extension === '.doc') {
                            // Handle legacy Word DOC files
                            logger.debug(`Processing DOC file: ${filePath}`);
                            processedContent = await this.convertDocToMarkdown(filePath, logger);
                        } else if (extension === '.docx') {
                            // Handle modern Word DOCX files
                            logger.debug(`Processing DOCX file: ${filePath}`);
                            processedContent = await this.convertDocxToMarkdown(filePath, logger);
                        } else {
                            // Handle text-based files
                            content = fs.readFileSync(filePath, { encoding: encoding as BufferEncoding });
                            
                            if (content.length > config.max_size) {
                                logger.warn(`File content (${content.length} chars) exceeds max size (${config.max_size}). Skipping ${filePath}.`);
                                skippedFiles++;
                                continue;
                            }
                            
                            // Convert HTML to Markdown if needed
                            if (extension === '.html' || extension === '.htm') {
                                logger.debug(`Converting HTML to Markdown for ${filePath}`);
                                const cleanHtml = sanitizeHtml(content, {
                                    allowedTags: [
                                        'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a', 'ul', 'ol',
                                        'li', 'b', 'i', 'strong', 'em', 'code', 'pre',
                                        'div', 'span', 'table', 'thead', 'tbody', 'tr', 'th', 'td'
                                    ],
                                    allowedAttributes: {
                                        'a': ['href'],
                                        'pre': ['class', 'data-language'],
                                        'code': ['class', 'data-language'],
                                        'div': ['class'],
                                        'span': ['class']
                                    }
                                });
                                processedContent = this.turndownService.turndown(cleanHtml);
                            } else {
                                processedContent = content;
                            }
                        }
                        
                        // Check size limit for processed content
                        if (processedContent.length > config.max_size) {
                            logger.warn(`Processed content (${processedContent.length} chars) exceeds max size (${config.max_size}). Skipping ${filePath}.`);
                            skippedFiles++;
                            continue;
                        }
                        
                        await processFileContent(filePath, processedContent);
                        processedFiles++;
                    } catch (error) {
                        logger.error(`Error processing file ${filePath}:`, error);
                    }
                }
            }
            
            logger.info(`Directory processed. Processed: ${processedFiles}, Skipped: ${skippedFiles}`);
        } catch (error) {
            logger.error(`Error reading directory ${dirPath}:`, error);
        }
    }

    async processCodeDirectory(
        dirPath: string,
        config: CodeSourceConfig,
        processFileContent: (filePath: string, content: string) => Promise<void>,
        parentLogger: Logger,
        visitedPaths: Set<string> = new Set(),
        options?: {
            allowedFiles?: Set<string>;
            mtimeCutoff?: number;
            trackFiles?: Set<string>;
        }
    ): Promise<{ processedFiles: number; skippedFiles: number; maxMtime: number }> {
        const logger = parentLogger.child('code-directory-processor');
        logger.info(`Processing code directory: ${dirPath}`);

        const recursive = config.recursive !== undefined ? config.recursive : true;
        const includeExtensions = config.include_extensions || [
            '.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs',
            '.py', '.go', '.rs', '.java', '.kt', '.kts', '.swift',
            '.c', '.cc', '.cpp', '.h', '.hpp', '.cs',
            '.rb', '.php', '.scala', '.sql', '.sh', '.bash', '.zsh',
            '.html', '.css', '.scss', '.sass', '.less',
            '.json', '.yaml', '.yml', '.md'
        ];
        const excludeExtensions = config.exclude_extensions || [];
        const encoding = config.encoding || ('utf8' as BufferEncoding);

        let maxMtime = 0;

        try {
            const files = fs.readdirSync(dirPath);
            let processedFiles = 0;
            let skippedFiles = 0;

            const allowedFiles = options?.allowedFiles;
            const mtimeCutoff = options?.mtimeCutoff;
            const trackFiles = options?.trackFiles;

            for (const file of files) {
                const filePath = path.join(dirPath, file);
                const stat = fs.statSync(filePath);

                if (visitedPaths.has(filePath)) {
                    logger.debug(`Skipping already visited path: ${filePath}`);
                    continue;
                }

                visitedPaths.add(filePath);

                if (stat.isDirectory()) {
                    if (recursive) {
                        const childResult = await this.processCodeDirectory(
                            filePath,
                            config,
                            processFileContent,
                            logger,
                            visitedPaths,
                            options
                        );
                        processedFiles += childResult.processedFiles;
                        skippedFiles += childResult.skippedFiles;
                        maxMtime = Math.max(maxMtime, childResult.maxMtime);
                    } else {
                        logger.debug(`Skipping directory ${filePath} (recursive=false)`);
                    }
                } else if (stat.isFile()) {
                    const extension = path.extname(file).toLowerCase();

                    if (excludeExtensions.includes(extension)) {
                        logger.debug(`Skipping file with excluded extension: ${filePath}`);
                        skippedFiles++;
                        continue;
                    }

                    if (includeExtensions.length > 0 && !includeExtensions.includes(extension)) {
                        logger.debug(`Skipping file with non-included extension: ${filePath}`);
                        skippedFiles++;
                        continue;
                    }

                    trackFiles?.add(filePath);
                    maxMtime = Math.max(maxMtime, stat.mtimeMs);

                    if (allowedFiles && !allowedFiles.has(filePath)) {
                        skippedFiles++;
                        continue;
                    }

                    if (mtimeCutoff !== undefined && stat.mtimeMs <= mtimeCutoff) {
                        skippedFiles++;
                        continue;
                    }

                    try {
                        logger.info(`Reading file: ${filePath}`);
                        const content = fs.readFileSync(filePath, { encoding: encoding as BufferEncoding });

                        if (content.length > config.max_size) {
                            logger.warn(`File content (${content.length} chars) exceeds max size (${config.max_size}). Skipping ${filePath}.`);
                            skippedFiles++;
                            continue;
                        }

                        await processFileContent(filePath, content);
                        processedFiles++;
                    } catch (error) {
                        logger.error(`Error processing file ${filePath}:`, error);
                    }
                }
            }

            logger.info(`Code directory processed. Processed: ${processedFiles}, Skipped: ${skippedFiles}`);
            return { processedFiles, skippedFiles, maxMtime };
        } catch (error) {
            logger.error(`Error reading code directory ${dirPath}:`, error);
            return { processedFiles: 0, skippedFiles: 0, maxMtime };
        }
    }

    private async getTokenChunker(chunkSize: number | undefined): Promise<TokenChunker> {
        const cacheKey = `${chunkSize || 'default'}`;
        const cached = this.tokenChunkerCache.get(cacheKey);
        if (cached) {
            return cached;
        }

        const chunkerPromise = (async () => {
            const { TokenChunker } = await this.importChonkieModule('@chonkiejs/core');
            return await TokenChunker.create({ chunkSize, tokenizer: 'character' });
        })();

        this.tokenChunkerCache.set(cacheKey, chunkerPromise);
        return chunkerPromise;
    }

    private async getCodeChunker(lang: string, chunkSize: number | undefined): Promise<CodeChunker> {
        const cacheKey = `${lang}:${chunkSize || 'default'}`;
        const cached = this.codeChunkerCache.get(cacheKey);
        if (cached) {
            return cached;
        }

        const chunkerPromise = (async () => {
            const tokenizer = await this.getTokenizer();
            return await CodeChunker.create({
                lang,
                chunkSize,
                tokenCounter: async (text: string) => tokenizer.countTokens(text)
            });
        })();

        this.codeChunkerCache.set(cacheKey, chunkerPromise);
        return chunkerPromise;
    }

    private async getTokenizer(): Promise<Tokenizer> {
        if (!this.tokenizerCache) {
            this.tokenizerCache = (async () => {
                const { Tokenizer } = await this.importChonkieModule('@chonkiejs/core');
                return await Tokenizer.create('character');
            })();
        }

        return this.tokenizerCache;
    }

    private detectCodeLanguage(filePath: string): string | undefined {
        const extension = path.extname(filePath).toLowerCase();
        const languageMap: Record<string, string> = {
            '.ts': 'typescript',
            '.tsx': 'typescript',
            '.js': 'javascript',
            '.jsx': 'javascript',
            '.mjs': 'javascript',
            '.cjs': 'javascript',
            '.py': 'python',
            '.go': 'go',
            '.rs': 'rust',
            '.java': 'java',
            '.kt': 'kotlin',
            '.kts': 'kotlin',
            '.swift': 'swift',
            '.c': 'c',
            '.cc': 'cpp',
            '.cpp': 'cpp',
            '.h': 'cpp',
            '.hpp': 'cpp',
            '.cs': 'csharp',
            '.rb': 'ruby',
            '.php': 'php',
            '.scala': 'scala',
            '.sql': 'sql',
            '.sh': 'bash',
            '.bash': 'bash',
            '.zsh': 'bash',
            '.html': 'html',
            '.css': 'css',
            '.scss': 'scss',
            '.sass': 'scss',
            '.less': 'css',
            '.json': 'json',
            '.yaml': 'yaml',
            '.yml': 'yaml',
            '.md': 'markdown'
        };

        return languageMap[extension];
    }

    private async importChonkieModule(specifier: string): Promise<any> {
        // Use dynamic import() directly - preserved by TypeScript with target ES2020+
        // even in CommonJS mode, as Node.js supports import() in CJS contexts
        return import(specifier);
    }

    async chunkCode(
        code: string,
        sourceConfig: CodeSourceConfig,
        url: string,
        filePath: string,
        branch?: string,
        repo?: string
    ): Promise<DocumentChunk[]> {
        const logger = this.logger.child('code-chunker');
        const normalizedPath = filePath.replace(/\\/g, '/');
        const lang = this.detectCodeLanguage(filePath);
        let chunks: Array<{ text: string }>;

        if (lang === 'markdown') {
            const markdownChunks = await this.chunkMarkdown(code, sourceConfig, url);

            for (const chunk of markdownChunks) {
                if (normalizedPath) {
                    const filePrefix = `[File: ${normalizedPath}]\n`;
                    const searchableText = filePrefix + chunk.content;
                    const chunkId = Utils.generateHash(`${url}::${searchableText}`);

                    chunk.content = searchableText;
                    chunk.metadata.heading_hierarchy = [normalizedPath, ...chunk.metadata.heading_hierarchy.filter(Boolean)];
                    chunk.metadata.section = normalizedPath;
                    chunk.metadata.chunk_id = chunkId;
                    chunk.metadata.hash = chunkId;
                }

                if (branch) {
                    chunk.metadata.branch = branch;
                }

                if (repo) {
                    chunk.metadata.repo = repo;
                }
            }

            logger.debug(`Chunked ${normalizedPath || url}: ${markdownChunks.length} chunks created.`);
            return markdownChunks;
        }

        if (lang) {
            try {
                const codeChunker = await this.getCodeChunker(lang, sourceConfig.chunk_size);
                chunks = await codeChunker.chunk(code);
            } catch (error) {
                logger.warn(`CodeChunker failed for ${normalizedPath || url}, falling back to token chunking:`, error);
                const chunker = await this.getTokenChunker(sourceConfig.chunk_size);
                chunks = await chunker.chunk(code);
            }
        } else {
            const chunker = await this.getTokenChunker(sourceConfig.chunk_size);
            chunks = await chunker.chunk(code);
        }

        const documentChunks: DocumentChunk[] = [];
        let chunkCounter = 0;
        const headingHierarchy = normalizedPath ? [normalizedPath] : [];
        const contextPrefix = normalizedPath ? `[File: ${normalizedPath}]\n` : '';

        for (const chunk of chunks) {
            const content = chunk.text?.trim();
            if (!content) {
                continue;
            }

            const searchableText = contextPrefix + content;
            const chunkId = Utils.generateHash(`${url}::${searchableText}`);

            documentChunks.push({
                content: searchableText,
                metadata: {
                    product_name: sourceConfig.product_name,
                    version: sourceConfig.version,
                    ...(branch ? { branch } : {}),
                    ...(repo ? { repo } : {}),
                    heading_hierarchy: headingHierarchy,
                    section: normalizedPath || 'Code',
                    chunk_id: chunkId,
                    url: url,
                    hash: chunkId,
                    chunk_index: chunkCounter,
                    total_chunks: 0
                }
            });

            chunkCounter++;
        }

        const totalChunks = documentChunks.length;
        for (const chunk of documentChunks) {
            chunk.metadata.total_chunks = totalChunks;
        }

        logger.debug(`Chunked ${normalizedPath || url}: ${documentChunks.length} chunks created.`);
        return documentChunks;
    }

    async chunkMarkdown(markdown: string, sourceConfig: SourceConfig, url: string): Promise<DocumentChunk[]> {
        const logger = this.logger.child('chunker');
        
        // --- Configuration ---
        const MAX_TOKENS = 1000;
        const MIN_TOKENS = 150;      // 💡 Merges "OpenAI-compatible" sentence into the next block
        const OVERLAP_PERCENT = 0.1; // 10% overlap for large splits
        
        const chunks: DocumentChunk[] = [];
        const lines = markdown.split("\n");
        
        let buffer = ""; 
        let headingHierarchy: string[] = [];
        let bufferHeadings: Array<{ level: number; text: string }> = []; // Track headings in current buffer
        let chunkCounter = 0; // Tracks chunk position within this page for ordering
    
        /**
         * Computes the topic hierarchy for merged content.
         * When merging sibling sections (same level), uses their parent heading.
         * Otherwise uses the current hierarchy.
         */
        const computeTopicHierarchy = (): string[] => {
            if (bufferHeadings.length === 0) {
                return headingHierarchy;
            }
            
            // Find the deepest level (most recent headings)
            const deepestLevel = Math.max(...bufferHeadings.map(h => h.level));
            
            // Get all headings at the deepest level
            const deepestHeadings = bufferHeadings.filter(h => h.level === deepestLevel);
            
            // If we have multiple sibling headings at the deepest level, use their parent
            if (deepestHeadings.length > 1 && deepestLevel > 1) {
                // Use parent heading (one level up from the sibling headings)
                // headingHierarchy still contains the parent at index (deepestLevel - 2)
                // We want everything up to (but not including) the deepest level
                return headingHierarchy.slice(0, deepestLevel - 1);
            }
            
            // Single heading or different levels: use the current hierarchy
            // This reflects the most recent heading which is appropriate
            return headingHierarchy;
        };
    
        /**
         * Internal helper to create the final chunk object with injected context.
         */
        const createDocumentChunk = (content: string, hierarchy: string[]): DocumentChunk => {
            // 💡 BREADCRUMB INJECTION
            // We prepend the hierarchy to the text. This makes the vector highly relevant 
            // to searches for parent topics even if the body doesn't mention them.
            const breadcrumbs = hierarchy.filter(h => h).join(" > ");
            const contextPrefix = breadcrumbs ? `[Topic: ${breadcrumbs}]\n` : "";
            const searchableText = contextPrefix + content.trim();
            const chunkId = Utils.generateHash(searchableText);
            
            const chunk: DocumentChunk = {
                content: searchableText,
                metadata: {
                    product_name: sourceConfig.product_name,
                    version: sourceConfig.version,
                    heading_hierarchy: hierarchy.filter(h => h),
                    section: hierarchy[hierarchy.length - 1] || "Introduction",
                    chunk_id: chunkId,
                    url: url,
                    hash: chunkId,
                    chunk_index: Math.floor(chunkCounter),
                    total_chunks: 0  // Placeholder, will be updated after all chunks are created
                }
            };
            chunkCounter++; // Increment for next chunk
            return chunk;
        };
    
        /**
         * Flushes the current buffer into the chunks array.
         * Uses sub-splitting logic if the buffer exceeds MAX_TOKENS.
         */
        const flushBuffer = (force = false) => {
            const trimmedBuffer = buffer.trim();
            if (!trimmedBuffer) return;
    
            const tokenCount = Utils.tokenize(trimmedBuffer).length;
    
            // 💡 SEMANTIC MERGING
            // If the current section is too short (like just a title or a one-liner),
            // we don't flush yet unless it's the end of the file (force=true).
            if (tokenCount < MIN_TOKENS && !force) {
                return; 
            }
    
            // Compute the appropriate topic hierarchy for merged content
            const topicHierarchy = computeTopicHierarchy();
    
            if (tokenCount > MAX_TOKENS) {
                // 💡 RECURSIVE OVERLAP SPLITTING
                // If the section is a massive guide, split it but keep headers on every sub-piece.
                const tokens = Utils.tokenize(trimmedBuffer);
                const overlapSize = Math.floor(MAX_TOKENS * OVERLAP_PERCENT);
                
                for (let i = 0; i < tokens.length; i += (MAX_TOKENS - overlapSize)) {
                    const subTokens = tokens.slice(i, i + MAX_TOKENS);
                    const subContent = subTokens.join("");
                    chunks.push(createDocumentChunk(subContent, topicHierarchy));
                }
            } else {
                chunks.push(createDocumentChunk(trimmedBuffer, topicHierarchy));
            }
            
            buffer = ""; // Reset buffer after successful flush
            bufferHeadings = []; // Reset tracked headings
        };
    
        // --- Main Processing Loop ---
        for (const line of lines) {
            const isHeading = line.startsWith("#");
    
            if (isHeading) {
                // Update Hierarchy Stack for the new heading
                const levelMatch = line.match(/^(#+)/);
                const level = levelMatch ? levelMatch[1].length : 1;
                // Clean heading: remove markdown prefix and anchor links like [](#anchor-id)
                const headingText = line
                    .replace(/^#+\s*/, "")           // Remove ## prefix
                    .replace(/\[.*?\]\(#[^)]*\)/g, "") // Remove [text](#anchor) patterns
                    .replace(/\[\]\(#[^)]*\)/g, "")    // Remove [](#anchor) patterns  
                    .trim();
                
                // Check if we should merge with previous content
                const currentTokenCount = Utils.tokenize(buffer.trim()).length;
                const hasBufferContent = currentTokenCount > 0;
                const bufferIsSmall = currentTokenCount < MIN_TOKENS;
                
                // Only merge if:
                // 1. Buffer has content and is small
                // 2. Buffer has tracked headings (we're merging sections, not just content)
                // 3. New heading is at same or deeper level than the deepest heading in buffer (siblings or children)
                //    If new heading is shallower (e.g., H2 after H3), it's a new section - flush first
                const deepestBufferLevel = bufferHeadings.length > 0 
                    ? Math.max(...bufferHeadings.map(h => h.level)) 
                    : 0;
                const shouldMerge = hasBufferContent && bufferIsSmall && bufferHeadings.length > 0 &&
                    level >= deepestBufferLevel;
                
                if (!shouldMerge && hasBufferContent) {
                    // Buffer is large enough OR new heading starts a new section - flush first
                    flushBuffer();
                }
                // If shouldMerge is true, we keep the buffer and merge the sections
    
                // Reset hierarchy below this level (e.g., H2 reset should clear previous H3s)
                headingHierarchy = headingHierarchy.slice(0, level - 1);
                headingHierarchy[level - 1] = headingText;
    
                // Track this heading in the buffer
                bufferHeadings.push({ level, text: headingText });
    
                buffer += `${line}\n`;
            } else {
                buffer += `${line}\n`;
                
                // Safety valve: if a single section is huge, flush it periodically
                if (Utils.tokenize(buffer).length >= MAX_TOKENS) {
                    flushBuffer();
                }
            }
        }
    
        // Final sweep
        flushBuffer(true); 
        
        // Update all chunks with the final total count
        const totalChunks = Math.floor(chunks.length);
        for (const chunk of chunks) {
            chunk.metadata.total_chunks = totalChunks;
        }
        
        logger.debug(`Chunking complete: ${chunks.length} rich context chunks created.`);
        return chunks;
    }
} 
