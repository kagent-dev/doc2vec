// Declarative description of the doc2vec config format (mirrors ../../types.ts).
// The ConfigForm renders entirely from these definitions, so adding a field here
// is all it takes to expose it in the UI.

export type FieldType = 'string' | 'number' | 'boolean' | 'string_list' | 'select';

export interface FieldDef {
  key: string;
  label: string;
  type: FieldType;
  required?: boolean;
  options?: string[];      // for select fields
  placeholder?: string;
  help?: string;
  secret?: boolean;        // suggests using a ${ENV_VAR} placeholder
}

/** Fields shared by every source type (type itself is handled separately). */
export const BASE_SOURCE_FIELDS: FieldDef[] = [
  { key: 'product_name', label: 'Product name', type: 'string', required: true, placeholder: 'istio', help: 'Stored in chunk metadata; used to identify the source' },
  { key: 'version', label: 'Version', type: 'string', required: true, placeholder: 'latest', help: 'Required for all types except code (defaults to branch)' },
  { key: 'max_size', label: 'Max page size (bytes)', type: 'number', placeholder: '1048576', help: 'Pages/files larger than this are skipped' },
];

export const SOURCE_TYPES: Record<string, { label: string; fields: FieldDef[] }> = {
  website: {
    label: 'Website',
    fields: [
      { key: 'url', label: 'Base URL', type: 'string', required: true, placeholder: 'https://docs.example.com/' },
      { key: 'sitemap_url', label: 'Sitemap URL', type: 'string', placeholder: 'https://docs.example.com/sitemap.xml', help: 'Discovers pages not linked in navigation; lastmod dates enable cheap change detection' },
      { key: 'markdown_store', label: 'Store markdown in Postgres', type: 'boolean', help: 'Requires the top-level markdown store to be configured' },
    ],
  },
  github: {
    label: 'GitHub issues',
    fields: [
      { key: 'repo', label: 'Repository', type: 'string', required: true, placeholder: 'owner/repo' },
      { key: 'start_date', label: 'Start date', type: 'string', placeholder: '2025-01-01', help: 'Only issues updated after this date (first run)' },
    ],
  },
  local_directory: {
    label: 'Local directory',
    fields: [
      { key: 'path', label: 'Directory path', type: 'string', required: true, placeholder: '/data/docs' },
      { key: 'include_extensions', label: 'Include extensions', type: 'string_list', placeholder: '.md, .txt, .pdf' },
      { key: 'exclude_extensions', label: 'Exclude extensions', type: 'string_list', placeholder: '.log, .tmp' },
      { key: 'recursive', label: 'Recurse into subdirectories', type: 'boolean' },
      { key: 'encoding', label: 'File encoding', type: 'string', placeholder: 'utf8' },
      { key: 'url_rewrite_prefix', label: 'URL rewrite prefix', type: 'string', placeholder: 'https://mydomain.com', help: 'Rewrites file:// URLs in chunk metadata' },
    ],
  },
  zendesk: {
    label: 'Zendesk',
    fields: [
      { key: 'zendesk_subdomain', label: 'Subdomain', type: 'string', required: true, placeholder: 'mycompany', help: 'mycompany.zendesk.com' },
      { key: 'email', label: 'User email', type: 'string', required: true, placeholder: 'agent@company.com' },
      { key: 'api_token', label: 'API token', type: 'string', required: true, secret: true, placeholder: '${ZENDESK_API_TOKEN}' },
      { key: 'fetch_tickets', label: 'Fetch tickets', type: 'boolean' },
      { key: 'fetch_articles', label: 'Fetch help center articles', type: 'boolean' },
      { key: 'start_date', label: 'Start date', type: 'string', placeholder: '2025-01-01' },
      { key: 'ticket_status', label: 'Ticket statuses', type: 'string_list', placeholder: 'new, open, pending, hold, solved' },
      { key: 'ticket_priority', label: 'Ticket priorities', type: 'string_list', placeholder: 'urgent, high' },
      { key: 'excluded_organizations', label: 'Excluded organizations', type: 'string_list' },
      { key: 'include_internal_comments', label: 'Include internal comments', type: 'boolean' },
    ],
  },
  code: {
    label: 'Code',
    fields: [
      { key: 'source', label: 'Code source', type: 'select', required: true, options: ['github', 'local_directory'] },
      { key: 'repo', label: 'Repository', type: 'string', placeholder: 'owner/repo', help: 'When source is github' },
      { key: 'branch', label: 'Branch', type: 'string', placeholder: 'main', help: 'When source is github' },
      { key: 'path', label: 'Directory path', type: 'string', placeholder: '/data/src', help: 'When source is local_directory' },
      { key: 'include_extensions', label: 'Include extensions', type: 'string_list', placeholder: '.ts, .go, .py' },
      { key: 'exclude_extensions', label: 'Exclude extensions', type: 'string_list' },
      { key: 'exclude_paths', label: 'Exclude paths', type: 'string_list', placeholder: 'vendor/**, **/*_test.go', help: 'Glob patterns relative to the source root' },
      { key: 'recursive', label: 'Recurse into subdirectories', type: 'boolean' },
      { key: 'chunk_size', label: 'Chunk size (tokens)', type: 'number', placeholder: '1500' },
      { key: 'url_rewrite_prefix', label: 'URL rewrite prefix', type: 'string' },
    ],
  },
  s3: {
    label: 'S3 bucket',
    fields: [
      { key: 'bucket', label: 'Bucket', type: 'string', required: true, placeholder: 'my-docs-bucket' },
      { key: 'prefix', label: 'Key prefix', type: 'string', placeholder: 'docs/' },
      { key: 'region', label: 'Region', type: 'string', placeholder: 'us-east-1' },
      { key: 'endpoint', label: 'Custom endpoint', type: 'string', placeholder: 'https://minio.local:9000', help: 'For S3-compatible services (MinIO, LocalStack)' },
      { key: 'include_extensions', label: 'Include extensions', type: 'string_list', placeholder: '.md, .txt, .pdf' },
      { key: 'exclude_extensions', label: 'Exclude extensions', type: 'string_list' },
      { key: 'encoding', label: 'Text encoding', type: 'string', placeholder: 'utf8' },
      { key: 'url_rewrite_prefix', label: 'URL rewrite prefix', type: 'string', help: 'Rewrites s3:// URLs in chunk metadata' },
    ],
  },
};

export const DATABASE_TYPES: Record<string, { label: string; fields: FieldDef[] }> = {
  sqlite: {
    label: 'SQLite (sqlite-vec)',
    fields: [
      { key: 'db_path', label: 'Database file', type: 'string', placeholder: './my-product.db' },
    ],
  },
  qdrant: {
    label: 'Qdrant',
    fields: [
      { key: 'qdrant_url', label: 'Qdrant URL', type: 'string', placeholder: 'http://localhost' },
      { key: 'qdrant_port', label: 'Port', type: 'number', placeholder: '6333' },
      { key: 'collection_name', label: 'Collection', type: 'string', placeholder: 'my-product' },
    ],
  },
};

export const EMBEDDING_PROVIDERS: Record<string, { label: string; fields: FieldDef[] }> = {
  openai: {
    label: 'OpenAI (or compatible)',
    fields: [
      { key: 'api_key', label: 'API key', type: 'string', secret: true, placeholder: '${OPENAI_API_KEY}', help: 'Falls back to the OPENAI_API_KEY env var' },
      { key: 'model', label: 'Model', type: 'string', placeholder: 'text-embedding-3-large' },
      { key: 'base_url', label: 'Base URL', type: 'string', placeholder: 'http://localhost:11434/v1', help: 'For Ollama or other OpenAI-compatible endpoints' },
    ],
  },
  azure: {
    label: 'Azure OpenAI',
    fields: [
      { key: 'api_key', label: 'API key', type: 'string', secret: true, placeholder: '${AZURE_OPENAI_KEY}' },
      { key: 'endpoint', label: 'Endpoint', type: 'string', placeholder: 'https://myresource.openai.azure.com' },
      { key: 'deployment_name', label: 'Deployment name', type: 'string', placeholder: 'text-embedding-3-large' },
      { key: 'api_version', label: 'API version', type: 'string', placeholder: '2024-10-21' },
    ],
  },
};

export const MARKDOWN_STORE_FIELDS: FieldDef[] = [
  { key: 'connection_string', label: 'Connection string', type: 'string', secret: true, placeholder: 'postgres://user:${PG_PASSWORD}@host:5432/db', help: 'Either this, or the discrete fields below' },
  { key: 'host', label: 'Host', type: 'string' },
  { key: 'port', label: 'Port', type: 'number', placeholder: '5432' },
  { key: 'database', label: 'Database', type: 'string' },
  { key: 'user', label: 'User', type: 'string' },
  { key: 'password', label: 'Password', type: 'string', secret: true, placeholder: '${PG_PASSWORD}' },
  { key: 'table_name', label: 'Table name', type: 'string', placeholder: 'markdown_pages' },
];

/** A minimal new source of the given type, pre-filled with sensible defaults. */
export function newSource(type: string): Record<string, any> {
  const source: Record<string, any> = { type, product_name: '', version: 'latest', max_size: 1048576 };
  if (type === 'code') delete source.version;
  source.database_config = { type: 'sqlite', params: { db_path: '' } };
  return source;
}
