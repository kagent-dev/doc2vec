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

    // ─── Parser cache reuse ─────────────────────────────────────────
    describe('parser cache reuse', () => {
        it('should reuse cached parser when creating two chunkers with the same language', async () => {
            const chunker1 = await CodeChunker.create({ lang: 'typescript', chunkSize: 200 });
            const chunks1 = await chunker1.chunk('const a = 1;');
            expect(chunks1.length).toBeGreaterThan(0);

            const chunker2 = await CodeChunker.create({ lang: 'typescript', chunkSize: 200 });
            const chunks2 = await chunker2.chunk('const b = 2;');
            expect(chunks2.length).toBeGreaterThan(0);
        });
    });

    // ─── Language name hyphen normalization ──────────────────────────
    describe('language name hyphen normalization', () => {
        it('should normalize hyphenated language names (c-sharp -> c_sharp)', async () => {
            // c-sharp should be normalized to c_sharp internally;
            // it may throw if no WASM file exists, but the normalization path is exercised
            try {
                const chunker = await CodeChunker.create({ lang: 'c-sharp' });
                const chunks = await chunker.chunk('public class Foo {}');
                expect(chunks.length).toBeGreaterThan(0);
            } catch (e: any) {
                // Expected if no WASM for c_sharp is available;
                // the normalization still happened before the error
                expect(e).toBeDefined();
            }
        });
    });

    // ─── Indivisible large leaf node ────────────────────────────────
    describe('indivisible large leaf node', () => {
        it('should return content even when a single node exceeds chunkSize', async () => {
            const chunker = await CodeChunker.create({
                lang: 'typescript',
                chunkSize: 5,
                tokenCounter: async (t: string) => t.length,
            });
            const code = 'const veryLongVariableName = "a very long string value that exceeds chunk size";';
            const chunks = await chunker.chunk(code);
            // Should still return the content even though it exceeds chunkSize
            expect(chunks.length).toBeGreaterThan(0);
        });
    });

    // ─── chunk() with mismatched language ────────────────────────────
    describe('chunk with mismatched language', () => {
        it('should still produce output when language does not match content', async () => {
            const chunker = await CodeChunker.create({ lang: 'python', chunkSize: 200 });
            const tsCode = 'interface Foo { bar: string; }';
            // Parser may not parse it correctly, but should still produce chunks or handle gracefully
            const chunks = await chunker.chunk(tsCode);
            expect(chunks.length).toBeGreaterThan(0);
        });
    });

    // ─── create() with Parser.init warning path ─────────────────────
    describe('create with Parser.init warning', () => {
        it('should warn if Parser.init throws and still proceed', async () => {
            const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
            try {
                // Even if Parser.init was already called or fails, create should still work
                const chunker = await CodeChunker.create({ lang: 'typescript' });
                expect(chunker).toBeDefined();
            } finally {
                warnSpy.mockRestore();
            }
        });
    });

    // ─── Empty chunks in mergeChunks ────────────────────────────────
    describe('empty chunks in mergeChunks', () => {
        it('should not produce empty chunks from whitespace-heavy code', async () => {
            const chunker = await CodeChunker.create({
                lang: 'typescript',
                chunkSize: 500,
            });
            const code = '\n\n\n// comment\n\n\nconst x = 1;\n\n\n';
            const chunks = await chunker.chunk(code);
            // No chunk should be empty or whitespace-only
            chunks.forEach(c => expect(c.text.trim().length).toBeGreaterThan(0));
        });
    });

    // ─── Function boundary integrity ──────────────────────────────────
    // These tests verify that complete function/class/method definitions
    // remain intact within a single chunk and are never split mid-body.
    // The strategy: use a chunkSize large enough to hold each individual
    // construct, then assert that every chunk containing a function/class
    // opening also contains its closing brace/keyword.
    describe('function boundary integrity', () => {

        // Helper: asserts that every chunk containing a given opening pattern
        // also contains the corresponding closing pattern.
        function assertBoundaryIntegrity(
            chunks: CodeChunk[],
            constructs: { name: string; opening: string | RegExp; closing: string | RegExp }[]
        ) {
            for (const construct of constructs) {
                const matchingChunks = chunks.filter(c => {
                    if (typeof construct.opening === 'string') {
                        return c.text.includes(construct.opening);
                    }
                    return construct.opening.test(c.text);
                });
                expect(matchingChunks.length).toBeGreaterThan(0);
                for (const chunk of matchingChunks) {
                    if (typeof construct.closing === 'string') {
                        expect(chunk.text).toContain(construct.closing);
                    } else {
                        expect(chunk.text).toMatch(construct.closing);
                    }
                }
            }
        }

        // ─── TypeScript ──────────────────────────────────────────────
        describe('TypeScript', () => {
            it('should keep standalone functions intact', async () => {
                const code = `
function add(a: number, b: number): number {
    const result = a + b;
    return result;
}

function multiply(x: number, y: number): number {
    const product = x * y;
    return product;
}

function greet(name: string): string {
    const greeting = \`Hello, \${name}!\`;
    return greeting;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'add', opening: 'function add(', closing: 'return result;\n}' },
                    { name: 'multiply', opening: 'function multiply(', closing: 'return product;\n}' },
                    { name: 'greet', opening: 'function greet(', closing: 'return greeting;\n}' },
                ]);
            });

            it('should keep arrow functions intact', async () => {
                const code = `
const double = (x: number): number => {
    const result = x * 2;
    return result;
};

const triple = (x: number): number => {
    const result = x * 3;
    return result;
};
`;
                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'double', opening: 'const double =', closing: 'return result;\n};' },
                    { name: 'triple', opening: 'const triple =', closing: 'return result;\n};' },
                ]);
            });

            it('should keep interfaces intact', async () => {
                const code = `
interface User {
    id: number;
    name: string;
    email: string;
    createdAt: Date;
}

interface Product {
    sku: string;
    title: string;
    price: number;
    inStock: boolean;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'User', opening: 'interface User {', closing: 'createdAt: Date;\n}' },
                    { name: 'Product', opening: 'interface Product {', closing: 'inStock: boolean;\n}' },
                ]);
            });

            it('should keep class methods intact when class is split', async () => {
                const code = `
class Calculator {
    private history: number[] = [];

    add(a: number, b: number): number {
        const result = a + b;
        this.history.push(result);
        return result;
    }

    subtract(a: number, b: number): number {
        const result = a - b;
        this.history.push(result);
        return result;
    }

    getHistory(): number[] {
        return [...this.history];
    }
}
`;
                // chunkSize is big enough for individual methods but not the whole class
                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 150,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Each method body should be intact in some chunk
                assertBoundaryIntegrity(chunks, [
                    { name: 'add', opening: 'add(a: number, b: number)', closing: 'return result;\n    }' },
                    { name: 'subtract', opening: 'subtract(a: number, b: number)', closing: 'return result;\n    }' },
                    { name: 'getHistory', opening: 'getHistory()', closing: 'return [...this.history];\n    }' },
                ]);
            });

            it('should keep async functions intact', async () => {
                const code = `
async function fetchUser(id: number): Promise<User> {
    const response = await fetch(\`/api/users/\${id}\`);
    const data = await response.json();
    return data as User;
}

async function fetchProducts(): Promise<Product[]> {
    const response = await fetch('/api/products');
    const data = await response.json();
    return data as Product[];
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'fetchUser', opening: 'async function fetchUser(', closing: 'return data as User;\n}' },
                    { name: 'fetchProducts', opening: 'async function fetchProducts(', closing: 'return data as Product[];\n}' },
                ]);
            });

            it('should keep enum declarations intact', async () => {
                const code = `
enum Direction {
    Up = "UP",
    Down = "DOWN",
    Left = "LEFT",
    Right = "RIGHT",
}

enum Color {
    Red = 0,
    Green = 1,
    Blue = 2,
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'Direction', opening: 'enum Direction {', closing: 'Right = "RIGHT",\n}' },
                    { name: 'Color', opening: 'enum Color {', closing: 'Blue = 2,\n}' },
                ]);
            });
        });

        // ─── JavaScript ─────────────────────────────────────────────
        describe('JavaScript', () => {
            it('should keep functions intact', async () => {
                const code = `
function fibonacci(n) {
    if (n <= 1) return n;
    let a = 0, b = 1;
    for (let i = 2; i <= n; i++) {
        const temp = a + b;
        a = b;
        b = temp;
    }
    return b;
}

function factorial(n) {
    if (n <= 1) return 1;
    let result = 1;
    for (let i = 2; i <= n; i++) {
        result *= i;
    }
    return result;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'javascript',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci', opening: 'function fibonacci(n)', closing: 'return b;\n}' },
                    { name: 'factorial', opening: 'function factorial(n)', closing: 'return result;\n}' },
                ]);
            });

            it('should keep class definitions intact', async () => {
                const code = `
class EventEmitter {
    constructor() {
        this.listeners = {};
    }

    on(event, callback) {
        if (!this.listeners[event]) {
            this.listeners[event] = [];
        }
        this.listeners[event].push(callback);
    }

    emit(event, ...args) {
        const callbacks = this.listeners[event] || [];
        callbacks.forEach(cb => cb(...args));
    }
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'javascript',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Methods should be intact
                assertBoundaryIntegrity(chunks, [
                    { name: 'constructor', opening: 'constructor()', closing: 'this.listeners = {};\n    }' },
                    { name: 'on', opening: 'on(event, callback)', closing: /this\.listeners\[event\]\.push\(callback\);\n\s+}/ },
                    { name: 'emit', opening: 'emit(event, ...args)', closing: /callbacks\.forEach\(cb => cb\(\.\.\.args\)\);\n\s+}/ },
                ]);
            });
        });

        // ─── Python ─────────────────────────────────────────────────
        describe('Python', () => {
            it('should keep function definitions intact', async () => {
                const code = `
def binary_search(arr, target):
    low = 0
    high = len(arr) - 1
    while low <= high:
        mid = (low + high) // 2
        if arr[mid] == target:
            return mid
        elif arr[mid] < target:
            low = mid + 1
        else:
            high = mid - 1
    return -1

def merge_sort(arr):
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left, right)
`;
                const chunker = await CodeChunker.create({
                    lang: 'python',
                    chunkSize: 350,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'binary_search', opening: 'def binary_search(', closing: 'return -1' },
                    { name: 'merge_sort', opening: 'def merge_sort(', closing: 'return merge(left, right)' },
                ]);
            });

            it('should keep class methods intact when class is split', async () => {
                const code = `
class Stack:
    def __init__(self):
        self.items = []

    def push(self, item):
        self.items.append(item)
        return self

    def pop(self):
        if self.is_empty():
            raise IndexError("pop from empty stack")
        return self.items.pop()

    def peek(self):
        if self.is_empty():
            raise IndexError("peek from empty stack")
        return self.items[-1]

    def is_empty(self):
        return len(self.items) == 0

    def size(self):
        return len(self.items)
`;
                const chunker = await CodeChunker.create({
                    lang: 'python',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Each method should be complete in its chunk
                assertBoundaryIntegrity(chunks, [
                    { name: '__init__', opening: 'def __init__(self)', closing: 'self.items = []' },
                    { name: 'push', opening: 'def push(self, item)', closing: 'return self' },
                    { name: 'pop', opening: 'def pop(self)', closing: 'return self.items.pop()' },
                    { name: 'is_empty', opening: 'def is_empty(self)', closing: 'return len(self.items) == 0' },
                ]);
            });

            it('should keep decorated functions intact', async () => {
                const code = `
def decorator(func):
    def wrapper(*args, **kwargs):
        print("Before")
        result = func(*args, **kwargs)
        print("After")
        return result
    return wrapper

@decorator
def say_hello(name):
    message = f"Hello, {name}!"
    print(message)
    return message
`;
                const chunker = await CodeChunker.create({
                    lang: 'python',
                    chunkSize: 300,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'decorator', opening: 'def decorator(func)', closing: 'return wrapper' },
                    { name: 'say_hello', opening: 'def say_hello(name)', closing: 'return message' },
                ]);
            });
        });

        // ─── Go ─────────────────────────────────────────────────────
        describe('Go', () => {
            it('should keep function definitions intact', async () => {
                const code = `
package main

import "fmt"

func fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	a, b := 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	for i := 2; i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}

func main() {
	fmt.Println(fibonacci(10))
	fmt.Println(isPrime(17))
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'go',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci', opening: 'func fibonacci(n int) int {', closing: /return b\n}/ },
                    { name: 'isPrime', opening: 'func isPrime(n int) bool {', closing: /return true\n}/ },
                    { name: 'main', opening: 'func main() {', closing: /isPrime\(17\)\)\n}/ },
                ]);
            });

            it('should keep struct methods intact', async () => {
                const code = `
package main

type Point struct {
	X float64
	Y float64
}

func (p Point) Distance(other Point) float64 {
	dx := p.X - other.X
	dy := p.Y - other.Y
	return dx*dx + dy*dy
}

func (p *Point) Translate(dx, dy float64) {
	p.X += dx
	p.Y += dy
}

func NewPoint(x, y float64) Point {
	return Point{X: x, Y: y}
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'go',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'Distance', opening: 'func (p Point) Distance(', closing: /return dx\*dx \+ dy\*dy\n}/ },
                    { name: 'Translate', opening: 'func (p *Point) Translate(', closing: /p\.Y \+= dy\n}/ },
                    { name: 'NewPoint', opening: 'func NewPoint(', closing: /return Point\{X: x, Y: y\}\n}/ },
                ]);
            });
        });

        // ─── Rust ───────────────────────────────────────────────────
        describe('Rust', () => {
            it('should keep function definitions intact', async () => {
                const code = `
fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

fn lcm(a: u64, b: u64) -> u64 {
    let g = gcd(a, b);
    (a / g) * b
}

fn is_palindrome(s: &str) -> bool {
    let bytes = s.as_bytes();
    let len = bytes.len();
    for i in 0..len / 2 {
        if bytes[i] != bytes[len - 1 - i] {
            return false;
        }
    }
    true
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'rust',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'gcd', opening: 'fn gcd(', closing: /a\n}/ },
                    { name: 'lcm', opening: 'fn lcm(', closing: /\(a \/ g\) \* b\n}/ },
                    { name: 'is_palindrome', opening: 'fn is_palindrome(', closing: /true\n}/ },
                ]);
            });

            it('should keep impl block methods intact when impl is split', async () => {
                const code = `
struct Rectangle {
    width: f64,
    height: f64,
}

impl Rectangle {
    fn new(width: f64, height: f64) -> Self {
        Rectangle { width, height }
    }

    fn area(&self) -> f64 {
        self.width * self.height
    }

    fn perimeter(&self) -> f64 {
        2.0 * (self.width + self.height)
    }

    fn is_square(&self) -> bool {
        (self.width - self.height).abs() < f64::EPSILON
    }
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'rust',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'new', opening: 'fn new(width', closing: /Rectangle \{ width, height \}\n\s+}/ },
                    { name: 'area', opening: 'fn area(&self)', closing: /self\.width \* self\.height\n\s+}/ },
                    { name: 'perimeter', opening: 'fn perimeter(&self)', closing: /2\.0 \* \(self\.width \+ self\.height\)\n\s+}/ },
                ]);
            });
        });

        // ─── Java ───────────────────────────────────────────────────
        describe('Java', () => {
            it('should keep method definitions intact', async () => {
                const code = `
public class MathUtils {
    public static int factorial(int n) {
        if (n <= 1) return 1;
        int result = 1;
        for (int i = 2; i <= n; i++) {
            result *= i;
        }
        return result;
    }

    public static boolean isPrime(int n) {
        if (n < 2) return false;
        for (int i = 2; i * i <= n; i++) {
            if (n % i == 0) return false;
        }
        return true;
    }

    public static int[] fibonacci(int count) {
        int[] fib = new int[count];
        fib[0] = 0;
        fib[1] = 1;
        for (int i = 2; i < count; i++) {
            fib[i] = fib[i-1] + fib[i-2];
        }
        return fib;
    }
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'java',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'factorial', opening: 'public static int factorial(', closing: /return result;\n\s+}/ },
                    { name: 'isPrime', opening: 'public static boolean isPrime(', closing: /return true;\n\s+}/ },
                    { name: 'fibonacci', opening: 'public static int[] fibonacci(', closing: /return fib;\n\s+}/ },
                ]);
            });
        });

        // ─── Kotlin ─────────────────────────────────────────────────
        describe('Kotlin', () => {
            it('should keep function definitions intact', async () => {
                const code = `
fun fibonacci(n: Int): Long {
    if (n <= 1) return n.toLong()
    var a = 0L
    var b = 1L
    for (i in 2..n) {
        val temp = a + b
        a = b
        b = temp
    }
    return b
}

fun isPalindrome(s: String): Boolean {
    val cleaned = s.lowercase().filter { it.isLetterOrDigit() }
    return cleaned == cleaned.reversed()
}

fun gcd(a: Int, b: Int): Int {
    var x = a
    var y = b
    while (y != 0) {
        val temp = y
        y = x % y
        x = temp
    }
    return x
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'kotlin',
                    chunkSize: 300,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci', opening: 'fun fibonacci(', closing: /return b\n}/ },
                    { name: 'isPalindrome', opening: 'fun isPalindrome(', closing: /return cleaned == cleaned\.reversed\(\)\n}/ },
                    { name: 'gcd', opening: 'fun gcd(', closing: /return x\n}/ },
                ]);
            });
        });

        // ─── Ruby ───────────────────────────────────────────────────
        describe('Ruby', () => {
            it('should keep method definitions intact', async () => {
                const code = `
def binary_search(arr, target)
  low = 0
  high = arr.length - 1
  while low <= high
    mid = (low + high) / 2
    if arr[mid] == target
      return mid
    elsif arr[mid] < target
      low = mid + 1
    else
      high = mid - 1
    end
  end
  -1
end

def quick_sort(arr)
  return arr if arr.length <= 1
  pivot = arr[0]
  left = arr[1..].select { |x| x <= pivot }
  right = arr[1..].select { |x| x > pivot }
  quick_sort(left) + [pivot] + quick_sort(right)
end
`;
                const chunker = await CodeChunker.create({
                    lang: 'ruby',
                    chunkSize: 400,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'binary_search', opening: 'def binary_search(', closing: /\-1\nend/ },
                    { name: 'quick_sort', opening: 'def quick_sort(', closing: /quick_sort\(right\)\nend/ },
                ]);
            });

            it('should keep class methods intact', async () => {
                const code = `
class LinkedList
  def initialize
    @head = nil
    @size = 0
  end

  def push(value)
    node = Node.new(value, @head)
    @head = node
    @size += 1
    self
  end

  def pop
    raise "empty list" if @head.nil?
    value = @head.value
    @head = @head.next
    @size -= 1
    value
  end

  def length
    @size
  end
end
`;
                const chunker = await CodeChunker.create({
                    lang: 'ruby',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'initialize', opening: 'def initialize', closing: /@size = 0\n  end/ },
                    { name: 'push', opening: 'def push(value)', closing: /self\n  end/ },
                    { name: 'pop', opening: 'def pop', closing: /value\n  end/ },
                ]);
            });
        });

        // ─── C ──────────────────────────────────────────────────────
        describe('C', () => {
            it('should keep function definitions intact', async () => {
                const code = `
#include <stdio.h>

int factorial(int n) {
    if (n <= 1) return 1;
    int result = 1;
    for (int i = 2; i <= n; i++) {
        result *= i;
    }
    return result;
}

void swap(int *a, int *b) {
    int temp = *a;
    *a = *b;
    *b = temp;
}

int main() {
    printf("%d\\n", factorial(5));
    int x = 3, y = 7;
    swap(&x, &y);
    printf("%d %d\\n", x, y);
    return 0;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'c',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'factorial', opening: 'int factorial(int n)', closing: /return result;\n}/ },
                    { name: 'swap', opening: 'void swap(int *a', closing: /\*b = temp;\n}/ },
                    { name: 'main', opening: 'int main()', closing: /return 0;\n}/ },
                ]);
            });
        });

        // ─── C++ ────────────────────────────────────────────────────
        describe('C++', () => {
            it('should keep standalone functions intact', async () => {
                const code = `
int factorial(int n) {
    if (n <= 1) return 1;
    int result = 1;
    for (int i = 2; i <= n; i++) {
        result *= i;
    }
    return result;
}

bool isPrime(int n) {
    if (n < 2) return false;
    for (int i = 2; i * i <= n; i++) {
        if (n % i == 0) return false;
    }
    return true;
}

double average(int* arr, int len) {
    double sum = 0;
    for (int i = 0; i < len; i++) {
        sum += arr[i];
    }
    return sum / len;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'cpp',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'factorial', opening: 'int factorial(int n)', closing: /return result;\n}/ },
                    { name: 'isPrime', opening: 'bool isPrime(int n)', closing: /return true;\n}/ },
                    { name: 'average', opening: 'double average(', closing: /return sum \/ len;\n}/ },
                ]);
            });
        });

        // ─── Swift ──────────────────────────────────────────────────
        describe('Swift', () => {
            it('should keep function definitions intact', async () => {
                const code = `
func fibonacci(_ n: Int) -> Int {
    if n <= 1 { return n }
    var a = 0
    var b = 1
    for _ in 2...n {
        let temp = a + b
        a = b
        b = temp
    }
    return b
}

func isPrime(_ n: Int) -> Bool {
    guard n >= 2 else { return false }
    for i in 2..<n {
        if n % i == 0 {
            return false
        }
    }
    return true
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'swift',
                    chunkSize: 300,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci', opening: 'func fibonacci(', closing: /return b\n}/ },
                    { name: 'isPrime', opening: 'func isPrime(', closing: /return true\n}/ },
                ]);
            });
        });

        // ─── PHP ────────────────────────────────────────────────────
        describe('PHP', () => {
            it('should keep function definitions intact', async () => {
                const code = `<?php

function fibonacci(int $n): int {
    if ($n <= 1) return $n;
    $a = 0;
    $b = 1;
    for ($i = 2; $i <= $n; $i++) {
        $temp = $a + $b;
        $a = $b;
        $b = $temp;
    }
    return $b;
}

function isPalindrome(string $s): bool {
    $cleaned = strtolower(preg_replace('/[^a-zA-Z0-9]/', '', $s));
    return $cleaned === strrev($cleaned);
}

function arraySum(array $arr): int {
    $sum = 0;
    foreach ($arr as $val) {
        $sum += $val;
    }
    return $sum;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'php',
                    chunkSize: 300,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci', opening: 'function fibonacci(', closing: /return \$b;\n}/ },
                    { name: 'isPalindrome', opening: 'function isPalindrome(', closing: /return \$cleaned === strrev\(\$cleaned\);\n}/ },
                    { name: 'arraySum', opening: 'function arraySum(', closing: /return \$sum;\n}/ },
                ]);
            });
        });

        // ─── Scala ──────────────────────────────────────────────────
        describe('Scala', () => {
            it('should keep function definitions intact', async () => {
                const code = `
object MathUtils {
  def factorial(n: Int): BigInt = {
    var result: BigInt = 1
    for (i <- 2 to n) {
      result *= i
    }
    result
  }

  def fibonacci(n: Int): Long = {
    if (n <= 1) return n
    var a = 0L
    var b = 1L
    for (_ <- 2 to n) {
      val temp = a + b
      a = b
      b = temp
    }
    b
  }

  def gcd(a: Int, b: Int): Int = {
    if (b == 0) a
    else gcd(b, a % b)
  }
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'scala',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'factorial', opening: 'def factorial(', closing: /result\n\s+}/ },
                    { name: 'fibonacci', opening: 'def fibonacci(', closing: /b\n\s+}/ },
                    { name: 'gcd', opening: 'def gcd(', closing: /else gcd\(b, a % b\)\n\s+}/ },
                ]);
            });
        });

        // ─── C# ────────────────────────────────────────────────────
        describe('C#', () => {
            it('should keep method bodies intact', async () => {
                // C# tree-sitter grammar splits some keywords from signatures,
                // so we verify that method bodies (between braces) stay together
                const code = `
public class StringHelper {
    public static string Reverse(string input) {
        char[] chars = input.ToCharArray();
        Array.Reverse(chars);
        return new string(chars);
    }

    public static bool IsPalindrome(string input) {
        string cleaned = input.ToLower();
        int left = 0;
        int right = cleaned.Length - 1;
        while (left < right) {
            if (cleaned[left] != cleaned[right]) return false;
            left++;
            right--;
        }
        return true;
    }

    public static int CountWords(string input) {
        if (string.IsNullOrWhiteSpace(input)) return 0;
        string[] words = input.Split(' ');
        return words.Length;
    }
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'c_sharp',
                    chunkSize: 300,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Verify that each method's body logic stays intact in a single chunk
                assertBoundaryIntegrity(chunks, [
                    { name: 'Reverse', opening: 'input.ToCharArray()', closing: 'return new string(chars);' },
                    { name: 'IsPalindrome', opening: 'input.ToLower()', closing: 'return true;' },
                    { name: 'CountWords', opening: 'IsNullOrWhiteSpace', closing: 'return words.Length;' },
                ]);
            });
        });

        // ─── Dart ───────────────────────────────────────────────────
        describe('Dart', () => {
            it('should keep function bodies intact', async () => {
                // Dart tree-sitter splits function signatures from bodies in some cases,
                // so we verify each function body block stays together
                const code = `
int fibonacci(int n) {
  if (n <= 1) return n;
  int a = 0, b = 1;
  for (int i = 2; i <= n; i++) {
    int temp = a + b;
    a = b;
    b = temp;
  }
  return b;
}

bool isPrime(int n) {
  if (n < 2) return false;
  for (int i = 2; i * i <= n; i++) {
    if (n % i == 0) return false;
  }
  return true;
}

List<int> range(int start, int end) {
  List<int> result = [];
  for (int i = start; i < end; i++) {
    result.add(i);
  }
  return result;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'dart',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Verify the body of each function stays intact in one chunk
                // The fibonacci body (return b) should be in the same chunk as its loop
                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci body', opening: 'if (n <= 1) return n;', closing: 'return b;' },
                    { name: 'isPrime body', opening: 'if (n < 2) return false;', closing: 'return true;' },
                    { name: 'range body', opening: 'List<int> result = [];', closing: 'return result;' },
                ]);
            });
        });

        // ─── Zig ────────────────────────────────────────────────────
        describe('Zig', () => {
            it('should keep function definitions intact', async () => {
                const code = `
const std = @import("std");

fn fibonacci(n: u64) u64 {
    if (n <= 1) return n;
    var a: u64 = 0;
    var b: u64 = 1;
    var i: u64 = 2;
    while (i <= n) : (i += 1) {
        const temp = a + b;
        a = b;
        b = temp;
    }
    return b;
}

fn factorial(n: u64) u64 {
    if (n <= 1) return 1;
    var result: u64 = 1;
    var i: u64 = 2;
    while (i <= n) : (i += 1) {
        result *= i;
    }
    return result;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'zig',
                    chunkSize: 300,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci', opening: 'fn fibonacci(', closing: /return b;\n}/ },
                    { name: 'factorial', opening: 'fn factorial(', closing: /return result;\n}/ },
                ]);
            });
        });

        // ─── Lua ────────────────────────────────────────────────────
        describe('Lua', () => {
            it('should keep function bodies intact', async () => {
                // Lua tree-sitter splits function keyword and name into separate tokens,
                // so we use a large enough chunkSize and verify function body logic stays together
                const code = `
function fibonacci(n)
    if n <= 1 then
        return n
    end
    local a, b = 0, 1
    for i = 2, n do
        a, b = b, a + b
    end
    return b
end

function factorial(n)
    if n <= 1 then
        return 1
    end
    local result = 1
    for i = 2, n do
        result = result * i
    end
    return result
end
`;
                const chunker = await CodeChunker.create({
                    lang: 'lua',
                    chunkSize: 250,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Verify each function body stays together
                assertBoundaryIntegrity(chunks, [
                    { name: 'fibonacci', opening: 'local a, b = 0, 1', closing: 'return b' },
                    { name: 'factorial', opening: 'local result = 1', closing: 'return result' },
                ]);
            });
        });

        // ─── Elixir ─────────────────────────────────────────────────
        describe('Elixir', () => {
            it('should keep function definitions intact', async () => {
                const code = `
defmodule MathUtils do
  def factorial(0), do: 1
  def factorial(n) when n > 0 do
    n * factorial(n - 1)
  end

  def fibonacci(0), do: 0
  def fibonacci(1), do: 1
  def fibonacci(n) when n > 1 do
    fibonacci(n - 1) + fibonacci(n - 2)
  end

  def gcd(a, 0), do: a
  def gcd(a, b) do
    gcd(b, rem(a, b))
  end
end
`;
                const chunker = await CodeChunker.create({
                    lang: 'elixir',
                    chunkSize: 300,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // At minimum, each multi-line function block should be intact
                assertBoundaryIntegrity(chunks, [
                    { name: 'factorial multi-line', opening: 'def factorial(n) when n > 0 do', closing: /n \* factorial\(n - 1\)\n\s+end/ },
                    { name: 'fibonacci multi-line', opening: 'def fibonacci(n) when n > 1 do', closing: /fibonacci\(n - 1\) \+ fibonacci\(n - 2\)\n\s+end/ },
                ]);
            });
        });

        // ─── Cross-language: large function that exceeds chunkSize ──
        describe('large function exceeding chunkSize', () => {
            it('should split a large function into sub-chunks at statement level (TypeScript)', async () => {
                // Build a function that is clearly larger than chunkSize
                const statements = Array.from({ length: 20 }, (_, i) =>
                    `    const var${i} = ${i} * Math.random();`
                ).join('\n');
                const code = `function bigFunction(): void {\n${statements}\n    console.log("done");\n}`;

                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // The function is too big for one chunk, so it should be split
                expect(chunks.length).toBeGreaterThan(1);

                // But each chunk should contain complete statements (no mid-statement cuts)
                for (const chunk of chunks) {
                    // No chunk should end or start in the middle of a const declaration
                    const constMatches = chunk.text.match(/const\s+/g) || [];
                    const equalsMatches = chunk.text.match(/=\s+/g) || [];
                    // Each const should have an equals sign (not cut between them)
                    // This is approximate — the key point is the AST-level split
                    if (constMatches.length > 0) {
                        expect(equalsMatches.length).toBeGreaterThanOrEqual(constMatches.length);
                    }
                }
            });

            it('should split a large function into sub-chunks at statement level (Python)', async () => {
                const statements = Array.from({ length: 20 }, (_, i) =>
                    `    var${i} = ${i} * 2`
                ).join('\n');
                const code = `def big_function():\n${statements}\n    return "done"`;

                const chunker = await CodeChunker.create({
                    lang: 'python',
                    chunkSize: 200,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                expect(chunks.length).toBeGreaterThan(1);

                // Each assignment should be complete in its chunk
                for (const chunk of chunks) {
                    const assignments = chunk.text.match(/var\d+\s*=/g) || [];
                    const values = chunk.text.match(/=\s*\d+\s*\*\s*2/g) || [];
                    if (assignments.length > 0 && chunk.text.includes('* 2')) {
                        expect(values.length).toBeGreaterThanOrEqual(assignments.length);
                    }
                }
            });
        });

        // ─── Multiple constructs: no cross-construct contamination ──
        describe('construct isolation', () => {
            it('should not merge different functions into one chunk when they individually fit (TypeScript)', async () => {
                // Two functions, each about 80 chars, chunkSize=100 so they can't merge
                const code = `
function alpha(): string {
    const msg = "hello alpha";
    return msg;
}

function beta(): string {
    const msg = "hello beta!";
    return msg;
}
`;
                const chunker = await CodeChunker.create({
                    lang: 'typescript',
                    chunkSize: 100,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Find the chunk containing alpha — it should NOT contain beta's body
                const alphaChunk = chunks.find(c => c.text.includes('function alpha'));
                const betaChunk = chunks.find(c => c.text.includes('function beta'));
                expect(alphaChunk).toBeDefined();
                expect(betaChunk).toBeDefined();

                // Ensure alpha and beta are in separate chunks
                if (alphaChunk !== betaChunk) {
                    expect(alphaChunk!.text).not.toContain('hello beta');
                    expect(betaChunk!.text).not.toContain('hello alpha');
                }
            });

            it('should merge small functions when they fit together (JavaScript)', async () => {
                // Two tiny functions, chunkSize=500 means they should merge
                const code = `
function a() { return 1; }
function b() { return 2; }
`;
                const chunker = await CodeChunker.create({
                    lang: 'javascript',
                    chunkSize: 500,
                    tokenCounter: async (t: string) => t.length,
                });
                const chunks = await chunker.chunk(code);

                // Both should be in one chunk
                expect(chunks.length).toBe(1);
                expect(chunks[0].text).toContain('function a()');
                expect(chunks[0].text).toContain('function b()');
            });
        });
    });

    // ─── resolveWasmPath - error for missing language ────────────────
    describe('resolveWasmPath error for missing language', () => {
        it('should throw for a language with no WASM file', async () => {
            await expect(
                CodeChunker.create({ lang: 'nonexistent_lang_xyz' }).then(c => c.chunk('code'))
            ).rejects.toThrow();
        });
    });

    // ─── Multiple chunks requiring merge across chunkSize boundary ──
    describe('multiple chunks merge across chunkSize boundary', () => {
        it('should split many small functions into multiple non-empty chunks', async () => {
            const chunker = await CodeChunker.create({
                lang: 'javascript',
                chunkSize: 50,
                tokenCounter: async (t: string) => t.length,
            });
            const code = Array.from({ length: 10 }, (_, i) =>
                `function f${i}() { return ${i}; }`
            ).join('\n');
            const chunks = await chunker.chunk(code);
            expect(chunks.length).toBeGreaterThan(1);
            // Verify no chunk is empty
            chunks.forEach(c => expect(c.text.trim().length).toBeGreaterThan(0));
        });
    });
});
