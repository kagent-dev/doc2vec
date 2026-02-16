# Postgres Markdown Store

Optional feature that stores the generated markdown for each crawled website URL in a Postgres table. This provides a searchable, raw-text copy of all documentation pages alongside the vector embeddings.

## How it works

| Sync | URL in Postgres? | Lastmod/ETag unchanged? | What happens |
|------|-----------------|------------------------|--------------|
| 1st  | No              | Yes or No              | Force-processed, markdown stored |
| 2nd+ | Yes             | Yes                    | Skipped (normal caching) |
| 2nd+ | Yes             | No (change detected)   | Processed, markdown updated |
| Any  | N/A             | HEAD returns 404       | Skipped, deleted from Postgres if present |

On the first sync, all pages are force-processed (bypassing lastmod/ETag skip logic) because no URLs exist in the Postgres table yet. This ensures the table is fully populated. On subsequent syncs, the normal caching layers apply and only pages with detected changes get their rows updated.

## Postgres setup

### 1. Create a user and database

Connect as a Postgres superuser (e.g., `postgres`):

```sql
CREATE USER doc2vec WITH PASSWORD 'your_password_here';
CREATE DATABASE doc2vec OWNER doc2vec;
```

Or if the database already exists:

```sql
CREATE USER doc2vec WITH PASSWORD 'your_password_here';
GRANT ALL PRIVILEGES ON DATABASE doc2vec TO doc2vec;
```

Then connect to the `doc2vec` database and grant schema permissions:

```sql
\c doc2vec
GRANT USAGE, CREATE ON SCHEMA public TO doc2vec;
```

The `CREATE` grant on `public` schema is required so that the application can create the `markdown_pages` table automatically on the first run.

### 2. Table creation

The table is created automatically via `CREATE TABLE IF NOT EXISTS` when the application starts. You do **not** need to create it manually. The schema is:

```sql
CREATE TABLE IF NOT EXISTS markdown_pages (
    url          TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    markdown     TEXT NOT NULL,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);
```

The table name defaults to `markdown_pages` but can be overridden via `table_name` in the config.

## Configuration

### Top-level Postgres connection (`config.yaml`)

Using a connection string:

```yaml
markdown_store:
  connection_string: 'postgres://doc2vec:${PG_PASSWORD}@localhost:5432/doc2vec'
```

Or using individual fields:

```yaml
markdown_store:
  host: 'localhost'
  port: 5432
  database: 'doc2vec'
  user: 'doc2vec'
  password: '${PG_PASSWORD}'
  # table_name: 'markdown_pages'  # Optional, defaults to 'markdown_pages'
```

`connection_string` takes priority if both are provided. Environment variable substitution (`${VAR_NAME}`) works in all fields.

### Per-source opt-in

Enable the markdown store on individual website sources:

```yaml
sources:
  - type: 'website'
    product_name: 'istio'
    version: 'latest'
    url: 'https://istio.io/latest/docs/'
    sitemap_url: 'https://istio.io/latest/docs/sitemap.xml'
    markdown_store: true  # Enable storing markdown in Postgres
    database_config:
      type: 'sqlite'
      params:
        db_path: './vector-dbs/istio.db'
```

Only website sources with `markdown_store: true` will store their markdown. The feature is disabled by default and has no effect on non-website source types.

## Full example

```yaml
markdown_store:
  host: 'localhost'
  port: 5432
  database: 'doc2vec'
  user: 'doc2vec'
  password: '${PG_PASSWORD}'

sources:
  - type: 'website'
    product_name: 'argo'
    version: 'stable'
    url: 'https://argo-cd.readthedocs.io/en/stable/'
    sitemap_url: 'https://argo-cd.readthedocs.io/en/stable/sitemap.xml'
    markdown_store: true
    max_size: 1048576
    database_config:
      type: 'sqlite'
      params:
        db_path: './vector-dbs/argo-cd.db'

  - type: 'website'
    product_name: 'istio'
    version: 'latest'
    url: 'https://istio.io/latest/docs/'
    markdown_store: true
    max_size: 1048576
    database_config:
      type: 'sqlite'
      params:
        db_path: './vector-dbs/istio.db'

  # This source does NOT store markdown (markdown_store not set)
  - type: 'website'
    product_name: 'kubernetes'
    version: '1.30'
    url: 'https://kubernetes.io/docs/'
    max_size: 1048576
    database_config:
      type: 'sqlite'
      params:
        db_path: './vector-dbs/k8s.db'
```
