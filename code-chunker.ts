import * as fs from 'fs';
import * as path from 'path';
import { Language, Parser } from 'web-tree-sitter';
import type { SyntaxNode } from 'web-tree-sitter';

type TokenCounter = (text: string) => Promise<number>;

export interface CodeChunkerOptions {
    lang: string;
    chunkSize?: number;
    tokenCounter?: TokenCounter;
}

export interface CodeChunk {
    text: string;
    tokenCount: number;
}

export class CodeChunker {
    private readonly lang: string;
    private readonly chunkSize: number;
    private readonly tokenCounter: TokenCounter;
    private static treeSitterInitialized = false;
    private static parserCache: Map<string, Promise<Parser>> = new Map();

    private constructor(lang: string, chunkSize: number, tokenCounter: TokenCounter) {
        this.lang = lang;
        this.chunkSize = chunkSize;
        this.tokenCounter = tokenCounter;
    }

    static async create(options: CodeChunkerOptions): Promise<CodeChunker> {
        if (!CodeChunker.treeSitterInitialized && Parser.init) {
            try {
                await Parser.init();
                CodeChunker.treeSitterInitialized = true;
            } catch (error) {
                console.warn('Failed to initialize tree-sitter parser:', error);
            }
        }

        const chunkSize = options.chunkSize ?? 512;
        if (chunkSize <= 0) {
            throw new Error('chunkSize must be greater than 0');
        }

        const tokenCounter = options.tokenCounter ?? (async (text: string) => text.length);
        const chunker = new CodeChunker(options.lang, chunkSize, tokenCounter);
        return chunker;
    }

    async chunk(text: string): Promise<CodeChunk[]> {
        if (!text.trim()) {
            return [];
        }

        const parser = await CodeChunker.getParser(this.lang);
        const originalTextBytes = Buffer.from(text, 'utf-8');
        const tree = parser.parse(originalTextBytes.toString());
        if (!tree) {
            throw new Error('Failed to parse code');
        }
        const chunks: CodeChunk[] = [];
        await this.recursiveChunk(tree.rootNode, originalTextBytes.toString(), chunks);
        return this.mergeChunks(chunks);
    }

    private static async getParser(lang: string): Promise<Parser> {
        const formattedLang = lang.toLowerCase().replace(/-/g, '_');
        const cached = this.parserCache.get(formattedLang);
        if (cached) {
            return cached;
        }

        const parserPromise = (async () => {
            if (!CodeChunker.treeSitterInitialized && Parser.init) {
                try {
                    await Parser.init();
                    CodeChunker.treeSitterInitialized = true;
                } catch (error) {
                    console.warn('Failed to initialize tree-sitter parser:', error);
                }
            }

            const wasmPath = CodeChunker.resolveWasmPath(formattedLang);
            const wasmBuffer = fs.readFileSync(wasmPath);
            const language = await Language.load(wasmBuffer);
            const parser = new Parser();
            parser.setLanguage(language);
            return parser;
        })();

        this.parserCache.set(formattedLang, parserPromise);
        return parserPromise;
    }

    private static resolveWasmPath(formattedLang: string): string {
        const nodeModulesPath = CodeChunker.findNearestNodeModules(__dirname);
        if (!nodeModulesPath) {
            throw new Error('node_modules directory not found.');
        }

        const wasmPath = path.join(nodeModulesPath, `tree-sitter-wasms/out/tree-sitter-${formattedLang}.wasm`);
        if (!fs.existsSync(wasmPath)) {
            throw new Error(`Tree-sitter WASM file for language "${formattedLang}" not found at ${wasmPath}.`);
        }

        return wasmPath;
    }

    private static findNearestNodeModules(startDir: string): string | null {
        let dir = path.resolve(startDir);
        while (true) {
            const candidate = path.join(dir, 'node_modules');
            if (fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()) {
                return candidate;
            }
            const parent = path.dirname(dir);
            if (parent === dir) break;
            dir = parent;
        }
        return null;
    }

    private async recursiveChunk(node: SyntaxNode, source: string, chunks: CodeChunk[]): Promise<void> {
        const nodeText = source.substring(node.startIndex, node.endIndex);
        const tokenCount = await this.tokenCounter(nodeText);
        const children = (node.children || []).filter((child): child is SyntaxNode => Boolean(child));

        if (tokenCount <= this.chunkSize || children.length === 0) {
            if (nodeText.trim()) {
                chunks.push({ text: nodeText, tokenCount });
            }
            return;
        }

        const beforeCount = chunks.length;
        for (const child of children) {
            await this.recursiveChunk(child, source, chunks);
        }

        if (chunks.length === beforeCount && nodeText.trim()) {
            chunks.push({ text: nodeText, tokenCount });
        }
    }

    private mergeChunks(chunks: CodeChunk[]): CodeChunk[] {
        const merged: CodeChunk[] = [];
        let currentText = '';
        let currentTokens = 0;
        const separatorTokens = 1; // Account for the '\n' separator between merged chunks

        for (const chunk of chunks) {
            if (!chunk.text.trim()) {
                continue;
            }

            const nextTokens = currentTokens + separatorTokens + chunk.tokenCount;

            if (currentTokens === 0) {
                currentText = chunk.text;
                currentTokens = chunk.tokenCount;
                continue;
            }

            if (nextTokens <= this.chunkSize) {
                currentText = `${currentText}\n${chunk.text}`;
                currentTokens = nextTokens;
                continue;
            }

            merged.push({ text: currentText, tokenCount: currentTokens });
            currentText = chunk.text;
            currentTokens = chunk.tokenCount;
        }

        if (currentTokens > 0) {
            merged.push({ text: currentText, tokenCount: currentTokens });
        }

        return merged;
    }
}
