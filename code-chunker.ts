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
    private static hostExports: Set<string> | null = null;
    private static moduleDamaged = false;

    /**
     * Symbols a grammar only calls when it is already aborting. Emscripten
     * resolves side-module imports lazily, so an import that never gets called
     * costs nothing. Ignoring these two keeps python, html, cpp, ruby, php and
     * vue usable — every one of them imports __assert_fail and none of them
     * ever calls it. Any other unresolved import is a normal-operation symbol
     * (bash calls isalpha on real scripts, yaml calls operator new), and those
     * are the ones that bring the whole module down.
     */
    private static readonly ABORT_ONLY_SYMBOLS = new Set(['__assert_fail', 'abort']);

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

        if (CodeChunker.moduleDamaged) {
            throw new Error(
                'tree-sitter module is damaged by an earlier parse failure; refusing to parse'
            );
        }

        const parser = await CodeChunker.getParser(this.lang);
        const source = Buffer.from(text, 'utf-8').toString();

        // A grammar that traps inside parse unwinds out of WASM without letting
        // tree-sitter clean up, and the damage is cumulative: after enough of
        // them every later parse fails with "memory access out of bounds", on
        // files and languages that are perfectly fine. One clear error beats
        // thousands of misleading ones, so stop using the module.
        let tree;
        try {
            tree = parser.parse(source);
        } catch (error) {
            CodeChunker.moduleDamaged = true;
            throw error;
        }
        if (!tree) {
            throw new Error('Failed to parse code');
        }
        // The syntax tree lives in the tree-sitter WASM heap, which the JS
        // garbage collector cannot reach. Without this delete the heap grows by
        // the size of every tree we ever parse, until an allocation fails with
        // "memory access out of bounds" on some unrelated file.
        try {
            const chunks: CodeChunk[] = [];
            await this.recursiveChunk(tree.rootNode, source, chunks);
            return this.mergeChunks(chunks);
        } finally {
            tree.delete();
        }
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
            CodeChunker.assertGrammarLinks(formattedLang, wasmBuffer);
            const language = await Language.load(wasmBuffer);
            const parser = new Parser();
            parser.setLanguage(language);
            return parser;
        })();

        this.parserCache.set(formattedLang, parserPromise);
        return parserPromise;
    }

    /**
     * Refuse a grammar whose imports the tree-sitter runtime cannot satisfy.
     *
     * The grammars ship separately from web-tree-sitter, so a grammar built
     * against a different runtime can import a C symbol this runtime does not
     * export. Emscripten binds side-module imports lazily, so the mismatch
     * stays invisible until the grammar calls the symbol mid-parse — then it
     * throws out of WASM and damages the shared module for every language.
     * Checking the import table up front turns that into a clean load failure,
     * and chunkCode falls back to token chunking as it does for any other
     * unsupported language.
     */
    private static assertGrammarLinks(formattedLang: string, wasmBuffer: Buffer): void {
        if (!CodeChunker.hostExports) {
            const hostWasm = path.join(path.dirname(require.resolve('web-tree-sitter')), 'tree-sitter.wasm');
            CodeChunker.hostExports = CodeChunker.readWasmTables(fs.readFileSync(hostWasm)).exports;
        }

        // Emscripten resolves an import from the host's exports first, then
        // from the side module's own exports.
        const grammar = CodeChunker.readWasmTables(wasmBuffer);
        const unresolved = grammar.functionImports.filter(name =>
            !CodeChunker.hostExports!.has(name) &&
            !grammar.exports.has(name) &&
            !CodeChunker.ABORT_ONLY_SYMBOLS.has(name)
        );

        if (unresolved.length > 0) {
            throw new Error(
                `Tree-sitter grammar "${formattedLang}" is incompatible with the installed ` +
                `web-tree-sitter runtime: unresolved imports ${unresolved.join(', ')}.`
            );
        }
    }

    /**
     * Read a module's function imports and its export names straight out of the
     * WASM binary.
     *
     * WebAssembly.Module.imports() would be shorter, but building a
     * WebAssembly.Module compiles the whole binary, and doing that on top of
     * the compile Language.load already does exhausts V8's compiler zone once a
     * process loads a dozen grammars. Walking the two sections costs nothing.
     */
    private static readWasmTables(buffer: Buffer): { functionImports: string[]; exports: Set<string> } {
        const functionImports: string[] = [];
        const exports = new Set<string>();

        let offset = 8; // magic number and version
        const readVarUint = (): number => {
            let result = 0;
            let shift = 0;
            let byte: number;
            do {
                byte = buffer[offset++];
                result |= (byte & 0x7f) << shift;
                shift += 7;
            } while (byte & 0x80);
            return result >>> 0;
        };
        const readName = (): string => {
            const length = readVarUint();
            const name = buffer.toString('utf8', offset, offset + length);
            offset += length;
            return name;
        };
        const skipLimits = (): void => {
            const flags = readVarUint();
            readVarUint();               // minimum
            if (flags & 0x01) readVarUint(); // maximum
        };

        while (offset < buffer.length) {
            const sectionId = readVarUint();
            const sectionSize = readVarUint();
            const sectionEnd = offset + sectionSize;

            if (sectionId === 2) {           // import section
                const count = readVarUint();
                for (let i = 0; i < count; i++) {
                    readName();              // module
                    const field = readName();
                    const kind = buffer[offset++];
                    switch (kind) {
                        case 0x00:           // function
                            readVarUint();   // type index
                            functionImports.push(field);
                            break;
                        case 0x01:           // table
                            offset++;        // element type
                            skipLimits();
                            break;
                        case 0x02:           // memory
                            skipLimits();
                            break;
                        default:             // global: value type + mutability
                            offset += 2;
                            break;
                    }
                }
            } else if (sectionId === 7) {    // export section
                const count = readVarUint();
                for (let i = 0; i < count; i++) {
                    exports.add(readName());
                    offset++;                // kind
                    readVarUint();           // index
                }
            }

            offset = sectionEnd;
        }

        return { functionImports, exports };
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
