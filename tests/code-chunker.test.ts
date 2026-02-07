import { describe, it, expect, vi } from 'vitest';
import { CodeChunker, CodeChunk } from '../code-chunker';

describe('CodeChunker', () => {
    // ─── Factory / create ───────────────────────────────────────────
    describe('create', () => {
        it('should create a CodeChunker instance', async () => {
            const chunker = await CodeChunker.create({ lang: 'typescript' });
            expect(chunker).toBeDefined();
        });

        it('should use default chunkSize of 512 when not specified', async () => {
            const chunker = await CodeChunker.create({ lang: 'typescript' });
            // We verify indirectly: a small input should produce a single chunk
            const code = 'const x = 1;';
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBe(1);
        });

        it('should throw when chunkSize is 0', async () => {
            await expect(CodeChunker.create({ lang: 'typescript', chunkSize: 0 }))
                .rejects.toThrow('chunkSize must be greater than 0');
        });

        it('should throw when chunkSize is negative', async () => {
            await expect(CodeChunker.create({ lang: 'typescript', chunkSize: -10 }))
                .rejects.toThrow('chunkSize must be greater than 0');
        });

        it('should accept a custom tokenCounter', async () => {
            const counter = vi.fn(async (text: string) => text.split(/\s+/).length);
            const chunker = await CodeChunker.create({
                lang: 'typescript',
                chunkSize: 100,
                tokenCounter: counter,
            });
            await chunker.chunk('const x = 1;');
            expect(counter).toHaveBeenCalled();
        });
    });

    // ─── chunk ──────────────────────────────────────────────────────
    describe('chunk', () => {
        it('should return empty array for empty input', async () => {
            const chunker = await CodeChunker.create({ lang: 'typescript' });
            const chunks = await chunker.chunk('');
            expect(chunks).toEqual([]);
        });

        it('should return empty array for whitespace-only input', async () => {
            const chunker = await CodeChunker.create({ lang: 'typescript' });
            const chunks = await chunker.chunk('   \n\n  \t  ');
            expect(chunks).toEqual([]);
        });

        it('should chunk TypeScript code', async () => {
            const chunker = await CodeChunker.create({ lang: 'typescript', chunkSize: 100 });
            const code = `
interface User {
    name: string;
    age: number;
}

function greet(user: User): string {
    return \`Hello, \${user.name}!\`;
}

class UserService {
    private users: User[] = [];

    addUser(user: User): void {
        this.users.push(user);
    }

    getUsers(): User[] {
        return this.users;
    }
}
            `;
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(0);
            // All chunks should have text content
            for (const chunk of chunks) {
                expect(chunk.text.trim().length).toBeGreaterThan(0);
            }
        });

        it('should chunk JavaScript code', async () => {
            const chunker = await CodeChunker.create({ lang: 'javascript', chunkSize: 50 });
            const code = `
function add(a, b) { return a + b; }
function sub(a, b) { return a - b; }
function mul(a, b) { return a * b; }
function div(a, b) { return a / b; }
            `;
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should chunk Python code', async () => {
            const chunker = await CodeChunker.create({ lang: 'python', chunkSize: 100 });
            const code = `
def hello():
    print("hello world")

class MyClass:
    def __init__(self):
        self.value = 42

    def get_value(self):
        return self.value
            `;
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should include tokenCount for each chunk', async () => {
            const chunker = await CodeChunker.create({ lang: 'typescript' });
            const code = 'const x = 1;\nconst y = 2;\n';
            const chunks = await chunker.chunk(code);
            for (const chunk of chunks) {
                expect(chunk.tokenCount).toBeGreaterThan(0);
            }
        });

        it('should respect chunkSize limit (approximately)', async () => {
            const chunkSize = 200;
            const chunker = await CodeChunker.create({
                lang: 'typescript',
                chunkSize,
                tokenCounter: async (text: string) => text.length,
            });

            // Generate enough code to require multiple chunks
            const code = Array.from({ length: 20 }, (_, i) =>
                `function fn${i}(a: number, b: number): number {\n  return a + b + ${i};\n}\n`
            ).join('\n');

            const chunks = await chunker.chunk(code);
            // Most chunks should be within chunkSize (some may exceed due to indivisible AST nodes)
            const withinLimit = chunks.filter(c => c.tokenCount <= chunkSize * 1.5);
            expect(withinLimit.length).toBe(chunks.length);
        });

        it('should preserve code content (no data loss)', async () => {
            const chunker = await CodeChunker.create({ lang: 'typescript', chunkSize: 50 });
            const code = 'const x = 1;\nconst y = 2;\nconst z = 3;';
            const chunks = await chunker.chunk(code);
            const reconstructed = chunks.map(c => c.text).join('\n');
            // All original content should be present
            expect(reconstructed).toContain('const x = 1');
            expect(reconstructed).toContain('const y = 2');
            expect(reconstructed).toContain('const z = 3');
        });
    });

    // ─── mergeChunks behavior ───────────────────────────────────────
    describe('merge behavior', () => {
        it('should merge small adjacent chunks', async () => {
            const chunker = await CodeChunker.create({
                lang: 'typescript',
                chunkSize: 500,
                tokenCounter: async (text: string) => text.length,
            });
            // Small declarations should be merged together
            const code = 'const a = 1;\nconst b = 2;\nconst c = 3;';
            const chunks = await chunker.chunk(code);
            // All three declarations fit in one chunk of size 500
            expect(chunks.length).toBe(1);
        });

        it('should not merge chunks that would exceed chunkSize', async () => {
            const chunker = await CodeChunker.create({
                lang: 'typescript',
                chunkSize: 30,
                tokenCounter: async (text: string) => text.length,
            });
            const code = 'const variable_one = "hello";\nconst variable_two = "world";\nconst variable_three = "test";';
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(1);
        });

        it('should skip empty/whitespace chunks during merge', async () => {
            const chunker = await CodeChunker.create({
                lang: 'typescript',
                chunkSize: 500,
            });
            const code = 'const x = 1;\n\n\n\n\nconst y = 2;';
            const chunks = await chunker.chunk(code);
            // No chunk should be empty
            for (const chunk of chunks) {
                expect(chunk.text.trim().length).toBeGreaterThan(0);
            }
        });
    });

    // ─── Language support ───────────────────────────────────────────
    describe('language support', () => {
        it('should handle Go code', async () => {
            const chunker = await CodeChunker.create({ lang: 'go', chunkSize: 200 });
            const code = `
package main

import "fmt"

func main() {
    fmt.Println("Hello, World!")
}
            `;
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should handle Rust code', async () => {
            const chunker = await CodeChunker.create({ lang: 'rust', chunkSize: 200 });
            const code = `
fn main() {
    println!("Hello, world!");
}

fn add(a: i32, b: i32) -> i32 {
    a + b
}
            `;
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should handle JSON', async () => {
            const chunker = await CodeChunker.create({ lang: 'json', chunkSize: 100 });
            const code = JSON.stringify({ name: 'test', version: '1.0', dependencies: { a: '1.0', b: '2.0' } }, null, 2);
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should handle CSS', async () => {
            const chunker = await CodeChunker.create({ lang: 'css', chunkSize: 100 });
            const code = `
body { margin: 0; padding: 0; }
.container { max-width: 1200px; margin: 0 auto; }
h1 { font-size: 2em; color: #333; }
            `;
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(0);
        });

        it('should throw for unsupported languages', async () => {
            const chunker = await CodeChunker.create({ lang: 'brainfuck' });
            await expect(chunker.chunk('++++++++[>++++[>++>+++>+++>+<<<<-]>+>+>->>+[<]<-]'))
                .rejects.toThrow();
        });
    });
});
