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
    DocumentChunk 
} from './types';

export class ContentProcessor {
    private turndownService: TurndownService;
    private logger: Logger;

    constructor(logger: Logger) {
        this.logger = logger;
        this.turndownService = new TurndownService({
            codeBlockStyle: 'fenced',
            headingStyle: 'atx'
        });
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
    ): Promise<{ hasNetworkErrors: boolean }> {
        const logger = parentLogger.child('crawler');
        const queue: string[] = [baseUrl];
        
        // Process sitemap if provided
        if (sourceConfig.sitemap_url) {
            logger.section('SITEMAP PROCESSING');
            const sitemapUrls = await this.parseSitemap(sourceConfig.sitemap_url, logger);
            
            // Add sitemap URLs to the queue if they're within the website scope
            for (const url of sitemapUrls) {
                if (url.startsWith(sourceConfig.url) && !queue.includes(url)) {
                    logger.debug(`Adding URL from sitemap to queue: ${url}`);
                    queue.push(url);
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
                const content = await this.processPage(url, sourceConfig);

                if (content !== null) {
                    await processPageContent(url, content);
                    if (Utils.isPdfUrl(url)) {
                        pdfProcessedCount++;
                    } else {
                        processedCount++;
                    }
                } else {
                    skippedSizeCount++;
                }

                // Only try to extract links from HTML pages, not PDFs
                if (!Utils.isPdfUrl(url)) {
                    const response = await axios.get(url);
                    const $ = load(response.data);

                    logger.debug(`Finding links on page ${url}`);
                    let newLinksFound = 0;

                    $('a[href]').each((_, element) => {
                        const href = $(element).attr('href');
                        if (!href || href.startsWith('#') || href.startsWith('mailto:')) return;

                        const fullUrl = Utils.buildUrl(href, url);
                        if (fullUrl.startsWith(sourceConfig.url) && !visitedUrls.has(Utils.normalizeUrl(fullUrl))) {
                             if (!queue.includes(fullUrl)) {
                                 queue.push(fullUrl);
                                 newLinksFound++;
                             }
                        }
                    });

                    logger.debug(`Found ${newLinksFound} new links on ${url}`);
                }
            } catch (error: any) {
                logger.error(`Failed during processing or link discovery for ${url}:`, error);
                errorCount++;
                
                // Check if this is a network error (DNS resolution, connection issues, etc.)
                if (this.isNetworkError(error)) {
                    hasNetworkErrors = true;
                    logger.warn(`Network error detected for ${url}, this may affect cleanup decisions`);
                }
            }
        }

        logger.info(`Crawl completed. HTML Pages: ${processedCount}, PDFs: ${pdfProcessedCount}, Skipped (Extension): ${skippedCount}, Skipped (Size): ${skippedSizeCount}, Errors: ${errorCount}`);
        
        if (hasNetworkErrors) {
            logger.warn('Network errors were encountered during crawling. Cleanup may be skipped to avoid removing valid chunks.');
        }
        
        return { hasNetworkErrors };
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

    async processPage(url: string, sourceConfig: SourceConfig): Promise<string | null> {
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
                    return null;
                }
                
                return markdown;
            } catch (error) {
                logger.error(`Failed to process PDF ${url}:`, error);
                return null;
            }
        }

        // Original HTML page processing logic
        let browser: Browser | null = null;
        try {
            // Use system Chromium if available (for Docker environments)
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
            });
            const page: Page = await browser.newPage();
            logger.debug(`Navigating to ${url}`);
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

            const htmlContent: string = await page.evaluate(() => {
                // 💡 Try specific content selectors first, then fall back to broader ones
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
                await browser.close();
                return null;
            }

            logger.debug(`Got HTML content (${htmlContent.length} chars), creating DOM`);
            const dom = new JSDOM(htmlContent);
            const document = dom.window.document;

            document.querySelectorAll('pre').forEach((pre: HTMLElement) => {
                pre.classList.add('article-content');
                pre.setAttribute('data-readable-content-score', '100');
                this.markCodeParents(pre.parentElement);
            });

            // 💡 Extract H1s BEFORE Readability - it often strips them as "chrome"
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
                await browser.close();
                return null;
            }
            
            // Debug: Log what Readability extracted
            logger.debug(`[Readability Debug] article.title: "${article.title}"`);
            logger.debug(`[Readability Debug] article.content length: ${article.content?.length}`);
            logger.debug(`[Readability Debug] article.content starts with: "${article.content?.substring(0, 200)}..."`);
            logger.debug(`[Readability Debug] Contains H1 tag: ${article.content?.includes('<h1')}`);
            logger.debug(`[Readability Debug] Contains H2 tag: ${article.content?.includes('<h2')}`);
            logger.debug(`[Readability Debug] Contains original-h1 class: ${article.content?.includes('original-h1')}`);

            // 💡 Restore H1s: find elements with our marker class and convert back from H2
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
            
            // 💡 Inject extracted H1s back if they're not in the markdown
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
            return markdown;
        } catch (error) {
            logger.error(`Error processing page ${url}:`, error);
            return null;
        } finally {
            if (browser && browser.isConnected()) {
                 await browser.close();
                 logger.debug(`Browser closed for ${url}`);
            }
        }
    }

    private markCodeParents(node: Element | null) {
        if (!node) return;

        if (node.querySelector('pre, code')) {
            node.classList.add('article-content');
            node.setAttribute('data-readable-content-score', '100');
        }
        this.markCodeParents(node.parentElement);
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
            console.log(searchableText);
            console.log(hierarchy);
            const chunkId = Utils.generateHash(searchableText);
            
            return {
                content: searchableText,
                metadata: {
                    product_name: sourceConfig.product_name,
                    version: sourceConfig.version,
                    heading_hierarchy: hierarchy.filter(h => h),
                    section: hierarchy[hierarchy.length - 1] || "Introduction",
                    chunk_id: chunkId,
                    url: url,
                    hash: chunkId
                }
            };
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
        
        logger.debug(`Chunking complete: ${chunks.length} rich context chunks created.`);
        return chunks;
    }
} 