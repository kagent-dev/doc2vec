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
            browser = await puppeteer.launch({
                args: ['--no-sandbox', '--disable-setuid-sandbox'],
            });
            const page: Page = await browser.newPage();
            logger.debug(`Navigating to ${url}`);
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

            const htmlContent: string = await page.evaluate(() => {
                const mainContentElement = document.querySelector('div[role="main"].document') || document.querySelector('main') || document.body;
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

            logger.debug(`Applying Readability to extract main content`);
            const reader = new Readability(document, {
                charThreshold: 20,
                classesToPreserve: ['article-content'],
            });
            const article = reader.parse();

            if (!article) {
                logger.warn(`Failed to parse article content with Readability for ${url}`);
                await browser.close();
                return null;
            }

            logger.debug(`Sanitizing HTML (${article.content.length} chars)`);
            const cleanHtml = sanitizeHtml(article.content, {
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
            const markdown = this.turndownService.turndown(cleanHtml);
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
        logger.debug(`Chunking markdown from ${url} (${markdown.length} chars)`);
        
        const MAX_TOKENS = 1000;
        const chunks: DocumentChunk[] = [];
        const lines = markdown.split("\n");
        let currentChunk = "";
        let headingHierarchy: string[] = [];

        const processChunk = () => {
            if (currentChunk.trim()) {
                const tokens = Utils.tokenize(currentChunk);
                if (tokens.length > MAX_TOKENS) {
                    logger.debug(`Chunk exceeds max token count (${tokens.length}), splitting into smaller chunks`);
                    let subChunk = "";
                    let tokenCount = 0;
                    const overlapSize = Math.floor(MAX_TOKENS * 0.05);
                    let lastTokens: string[] = [];

                    for (const token of tokens) {
                        if (tokenCount + 1 > MAX_TOKENS) {
                            chunks.push(createDocumentChunk(subChunk, headingHierarchy));
                            subChunk = lastTokens.join("") + token;
                            tokenCount = lastTokens.length + 1;
                            lastTokens = [];
                        } else {
                            subChunk += token;
                            tokenCount++;
                            lastTokens.push(token);
                            if (lastTokens.length > overlapSize) {
                                lastTokens.shift();
                            }
                        }
                    }
                    if (subChunk) {
                        chunks.push(createDocumentChunk(subChunk, headingHierarchy));
                    }
                } else {
                    chunks.push(createDocumentChunk(currentChunk, headingHierarchy));
                }
            }
            currentChunk = "";
        };

        const createDocumentChunk = (content: string, hierarchy: string[]): DocumentChunk => {
            const chunkId = Utils.generateHash(content);
            logger.debug(`Created chunk ${chunkId.substring(0, 8)}... with ${content.length} chars`);
            
            return {
                content,
                metadata: {
                    product_name: sourceConfig.product_name,
                    version: sourceConfig.version,
                    heading_hierarchy: [...hierarchy],
                    section: hierarchy[hierarchy.length - 1] || "Introduction",
                    chunk_id: chunkId,
                    url: url,
                    hash: Utils.generateHash(content)
                }
            };
        };

        for (const line of lines) {
            if (line.startsWith("#")) {
                processChunk();
                const levelMatch = line.match(/^(#+)/);
                let level = levelMatch ? levelMatch[1].length : 1;
                const heading = line.replace(/^#+\s*/, "").trim();

                logger.debug(`Found heading (level ${level}): ${heading}`);
                
                while (headingHierarchy.length < level - 1) {
                    headingHierarchy.push("");
                }

                if (level <= headingHierarchy.length) {
                    headingHierarchy = headingHierarchy.slice(0, level - 1);
                }
                headingHierarchy[level - 1] = heading;
            } else {
                currentChunk += `${line}\n`;
            }
        }
        processChunk();
        
        logger.debug(`Chunking complete, created ${chunks.length} chunks`);
        return chunks;
    }
} 