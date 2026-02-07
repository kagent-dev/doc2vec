declare module 'web-tree-sitter' {
    export class Language {
        static load(input: ArrayBuffer | Uint8Array | Buffer): Promise<Language>;
    }

    export interface SyntaxNode {
        type: string;
        text: string;
        startIndex: number;
        endIndex: number;
        children: Array<SyntaxNode | null>;
    }

    export interface Tree {
        rootNode: SyntaxNode;
    }

    export class Parser {
        static init?: () => Promise<void>;
        setLanguage(language: Language): void;
        parse(input: string): Tree | null;
    }
}
