declare module 'word-extractor' {
    interface ExtractedDocument {
        getBody(): string;
        getHeaders(): { text: string }[];
        getFooters(): { text: string }[];
        getFootnotes(): { text: string }[];
        getEndnotes(): { text: string }[];
        getAnnotations(): { text: string }[];
    }

    class WordExtractor {
        extract(filePath: string): Promise<ExtractedDocument>;
    }

    export default WordExtractor;
}
