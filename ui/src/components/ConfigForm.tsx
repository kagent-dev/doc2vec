import { useMemo, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import * as yaml from 'js-yaml';
import { api, ApiError, ConfigRecord } from '../api';
import {
  BASE_SOURCE_FIELDS,
  DATABASE_TYPES,
  EMBEDDING_PROVIDERS,
  FieldDef,
  MARKDOWN_STORE_FIELDS,
  SOURCE_TYPES,
  newSource,
} from '../lib/config-schema';
import { humanizeCron } from '../lib/format';

type Path = Array<string | number>;

function getPath(obj: any, path: Path): any {
  return path.reduce((acc, key) => (acc == null ? undefined : acc[key]), obj);
}

/** Immutable deep-set; empty-ish values delete the key so the YAML stays clean. */
function setPath(obj: any, path: Path, value: any): any {
  if (path.length === 0) return value;
  const [head, ...rest] = path;
  const clone: any = Array.isArray(obj) ? [...(obj ?? [])] : { ...(obj ?? {}) };
  if (rest.length === 0) {
    if (value === undefined || value === '' || (Array.isArray(value) && value.length === 0)) {
      delete clone[head];
    } else {
      clone[head] = value;
    }
  } else {
    clone[head] = setPath(clone[head], rest, value);
  }
  return clone;
}

const inputClass =
  'w-full rounded-md border border-edge bg-page px-3 py-1.5 text-sm outline-none focus:border-accent placeholder:text-ink-muted/60';

function FieldInput({ field, value, onChange }: { field: FieldDef; value: any; onChange: (v: any) => void }) {
  // string_list keeps local text state so typing commas/spaces doesn't fight the parser
  const [listText, setListText] = useState<string | null>(null);

  if (field.type === 'boolean') {
    return (
      <label className="flex items-center gap-2 py-1.5 text-sm text-ink-secondary">
        <input
          type="checkbox"
          checked={!!value}
          onChange={e => onChange(e.target.checked || undefined)}
          className="h-4 w-4 accent-(--accent)"
        />
        {field.label}
        {field.help && <span className="text-xs text-ink-muted">— {field.help}</span>}
      </label>
    );
  }

  return (
    <label className="block">
      <span className="mb-1 flex items-baseline gap-2 text-xs font-medium text-ink-secondary">
        {field.label}
        {field.required && <span className="text-critical">*</span>}
        {field.secret && (
          <span className="text-ink-muted">
            use a <code>{'${ENV_VAR}'}</code> placeholder
          </span>
        )}
      </span>
      {field.type === 'select' ? (
        <select value={value ?? ''} onChange={e => onChange(e.target.value || undefined)} className={inputClass}>
          <option value="">—</option>
          {field.options!.map(opt => (
            <option key={opt} value={opt}>{opt}</option>
          ))}
        </select>
      ) : field.type === 'string_list' ? (
        <input
          value={listText ?? (Array.isArray(value) ? value.join(', ') : '')}
          placeholder={field.placeholder}
          onChange={e => setListText(e.target.value)}
          onBlur={() => {
            if (listText !== null) {
              onChange(listText.split(',').map(s => s.trim()).filter(Boolean));
              setListText(null);
            }
          }}
          className={inputClass}
        />
      ) : (
        <input
          type="text"
          inputMode={field.type === 'number' ? 'numeric' : undefined}
          value={value ?? ''}
          placeholder={field.placeholder}
          onChange={e => {
            const raw = e.target.value;
            if (field.type === 'number') {
              onChange(raw === '' ? undefined : Number(raw));
            } else {
              onChange(raw);
            }
          }}
          className={inputClass}
        />
      )}
      {field.help && <span className="mt-1 block text-xs text-ink-muted">{field.help}</span>}
    </label>
  );
}

function FieldGrid({ fields, doc, basePath, onSet }: {
  fields: FieldDef[];
  doc: any;
  basePath: Path;
  onSet: (path: Path, value: any) => void;
}) {
  return (
    <div className="grid gap-x-4 gap-y-3 sm:grid-cols-2">
      {fields.map(field => (
        <div key={field.key} className={field.type === 'boolean' ? 'sm:col-span-2' : ''}>
          <FieldInput
            field={field}
            value={getPath(doc, [...basePath, field.key])}
            onChange={v => onSet([...basePath, field.key], v)}
          />
        </div>
      ))}
    </div>
  );
}

function SectionCard({ title, subtitle, action, children }: {
  title: string;
  subtitle?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-lg border border-edge bg-surface p-4">
      <div className="mb-3 flex items-center gap-2">
        <h3 className="text-sm font-semibold">{title}</h3>
        {subtitle && <span className="text-xs text-ink-muted">{subtitle}</span>}
        <div className="ml-auto">{action}</div>
      </div>
      {children}
    </section>
  );
}

export default function ConfigForm(props: {
  mode: 'create' | 'edit';
  config?: ConfigRecord;             // required for edit
  onSaved?: (record: ConfigRecord) => void;
  onCancel?: () => void;
}) {
  const queryClient = useQueryClient();

  const initial = useMemo(() => {
    if (props.mode === 'edit' && props.config) {
      try {
        const parsed = yaml.load(props.config.content);
        if (parsed && typeof parsed === 'object') return { doc: parsed as any, parseError: null };
        return { doc: null, parseError: 'config is not a YAML mapping' };
      } catch (err) {
        return { doc: null, parseError: err instanceof Error ? err.message : String(err) };
      }
    }
    return { doc: { name: '', schedule: '', sources: [newSource('website')] }, parseError: null };
  }, [props.mode, props.config]);

  const [doc, setDoc] = useState<any>(initial.doc);
  const [filename, setFilename] = useState('');
  const [apiErrorMessage, setApiErrorMessage] = useState<string | null>(null);

  const save = useMutation({
    mutationFn: async () => {
      const content = yaml.dump(doc, { lineWidth: 120, noRefs: true });
      const check = await api.validateConfig(content);
      if (!check.valid) throw new ApiError(400, check.error ?? 'invalid config');
      if (props.mode === 'create') {
        const name = filename.trim() || `${(doc.name || 'config').toString().replace(/[^A-Za-z0-9._-]+/g, '-')}.yaml`;
        return api.createConfig(name, content);
      }
      return api.updateConfig(props.config!.id, content, props.config!.content_hash);
    },
    onSuccess: record => {
      setApiErrorMessage(null);
      queryClient.invalidateQueries({ queryKey: ['configs'] });
      props.onSaved?.(record);
    },
    onError: err => setApiErrorMessage((err as ApiError).message),
  });

  if (initial.parseError || !doc) {
    return (
      <p className="rounded-md border border-warning/50 bg-surface px-3 py-2 text-sm text-ink-secondary">
        ⚠ This config can't be edited as a form ({initial.parseError}) — use the YAML view.
      </p>
    );
  }

  const set = (path: Path, value: any) => setDoc((prev: any) => setPath(prev, path, value));
  const sources: any[] = Array.isArray(doc.sources) ? doc.sources : [];

  const missingRequired: string[] = [];
  sources.forEach((source, i) => {
    const typeDef = SOURCE_TYPES[source?.type];
    for (const field of [...BASE_SOURCE_FIELDS, ...(typeDef?.fields ?? [])]) {
      if (!field.required) continue;
      if (source?.type === 'code' && field.key === 'version') continue;
      const value = source?.[field.key];
      if (value === undefined || value === '') missingRequired.push(`source ${i + 1}: ${field.label}`);
    }
  });

  const embeddingEnabled = doc.embedding !== undefined;
  const markdownStoreEnabled = doc.markdown_store !== undefined;
  const conflict = save.error instanceof ApiError && save.error.status === 409;
  const scheduleValue = doc.schedule ?? '';

  return (
    <div className="space-y-4">
      {conflict && (
        <div className="rounded-md border border-warning/50 bg-surface px-3 py-2 text-sm text-ink-secondary">
          ⚠ The file changed on disk since you loaded it. Reload the page and reapply your edits.
        </div>
      )}
      {apiErrorMessage && !conflict && (
        <div className="rounded-md border border-critical/50 bg-surface px-3 py-2 text-sm text-critical">{apiErrorMessage}</div>
      )}

      <SectionCard title="General">
        <div className="grid gap-x-4 gap-y-3 sm:grid-cols-2">
          {props.mode === 'create' && (
            <label className="block">
              <span className="mb-1 block text-xs font-medium text-ink-secondary">Filename</span>
              <input
                value={filename}
                onChange={e => setFilename(e.target.value)}
                placeholder={`${(doc.name || 'my-config').toString().replace(/[^A-Za-z0-9._-]+/g, '-')}.yaml`}
                className={`${inputClass} font-mono`}
              />
            </label>
          )}
          <FieldInput
            field={{ key: 'name', label: 'Display name', type: 'string', placeholder: 'product-docs' }}
            value={doc.name}
            onChange={v => set(['name'], v)}
          />
          <div>
            <FieldInput
              field={{ key: 'schedule', label: 'Schedule (cron)', type: 'string', placeholder: '0 2 * * *', help: 'Leave empty for manual runs only' }}
              value={scheduleValue}
              onChange={v => set(['schedule'], v)}
            />
            {scheduleValue && (
              <p className="mt-1 text-xs text-accent">{humanizeCron(scheduleValue)}</p>
            )}
          </div>
        </div>
      </SectionCard>

      {sources.map((source, i) => {
        const typeDef = SOURCE_TYPES[source?.type] ?? SOURCE_TYPES.website;
        const dbType = source?.database_config?.type ?? 'sqlite';
        const dbDef = DATABASE_TYPES[dbType] ?? DATABASE_TYPES.sqlite;
        return (
          <SectionCard
            key={i}
            title={`Source ${i + 1}`}
            subtitle={source?.product_name || undefined}
            action={
              <button
                onClick={() => set(['sources'], sources.filter((_, j) => j !== i))}
                className="text-xs text-ink-muted transition hover:text-critical"
              >
                Remove
              </button>
            }
          >
            <div className="space-y-4">
              <div className="grid gap-x-4 gap-y-3 sm:grid-cols-2">
                <label className="block">
                  <span className="mb-1 block text-xs font-medium text-ink-secondary">Type</span>
                  <select
                    value={source?.type ?? 'website'}
                    onChange={e => {
                      // Changing type keeps the shared fields, drops type-specific ones
                      const kept = {
                        ...newSource(e.target.value),
                        product_name: source?.product_name ?? '',
                        ...(source?.version !== undefined && e.target.value !== 'code' && { version: source.version }),
                        ...(source?.max_size !== undefined && { max_size: source.max_size }),
                        database_config: source?.database_config ?? newSource(e.target.value).database_config,
                      };
                      set(['sources', i], kept);
                    }}
                    className={inputClass}
                  >
                    {Object.entries(SOURCE_TYPES).map(([key, def]) => (
                      <option key={key} value={key}>{def.label}</option>
                    ))}
                  </select>
                </label>
                {BASE_SOURCE_FIELDS.map(field => (
                  <FieldInput
                    key={field.key}
                    field={source?.type === 'code' && field.key === 'version' ? { ...field, required: false, help: 'Defaults to the branch name' } : field}
                    value={source?.[field.key]}
                    onChange={v => set(['sources', i, field.key], v)}
                  />
                ))}
              </div>

              <div>
                <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-muted">{typeDef.label} options</p>
                <FieldGrid fields={typeDef.fields} doc={doc} basePath={['sources', i]} onSet={set} />
              </div>

              <div>
                <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-muted">Vector database</p>
                <div className="grid gap-x-4 gap-y-3 sm:grid-cols-2">
                  <label className="block">
                    <span className="mb-1 block text-xs font-medium text-ink-secondary">Type</span>
                    <select
                      value={dbType}
                      onChange={e => set(['sources', i, 'database_config'], { type: e.target.value, params: {} })}
                      className={inputClass}
                    >
                      {Object.entries(DATABASE_TYPES).map(([key, def]) => (
                        <option key={key} value={key}>{def.label}</option>
                      ))}
                    </select>
                  </label>
                  {dbDef.fields.map(field => (
                    <FieldInput
                      key={field.key}
                      field={field}
                      value={source?.database_config?.params?.[field.key]}
                      onChange={v => set(['sources', i, 'database_config', 'params', field.key], v)}
                    />
                  ))}
                </div>
              </div>
            </div>
          </SectionCard>
        );
      })}

      <button
        onClick={() => set(['sources'], [...sources, newSource('website')])}
        className="w-full rounded-lg border border-dashed border-edge py-2.5 text-sm font-medium text-ink-muted transition hover:border-accent hover:text-accent"
      >
        + Add source
      </button>

      <SectionCard
        title="Embedding"
        subtitle="optional — defaults to OpenAI text-embedding-3-large"
        action={
          <label className="flex items-center gap-2 text-xs text-ink-secondary">
            <input
              type="checkbox"
              checked={embeddingEnabled}
              onChange={e => set(['embedding'], e.target.checked ? { provider: 'openai' } : undefined)}
              className="h-4 w-4 accent-(--accent)"
            />
            customize
          </label>
        }
      >
        {embeddingEnabled ? (
          <div className="space-y-3">
            <div className="grid gap-x-4 gap-y-3 sm:grid-cols-2">
              <label className="block">
                <span className="mb-1 block text-xs font-medium text-ink-secondary">Provider</span>
                <select
                  value={doc.embedding?.provider ?? 'openai'}
                  onChange={e => set(['embedding'], { provider: e.target.value })}
                  className={inputClass}
                >
                  {Object.entries(EMBEDDING_PROVIDERS).map(([key, def]) => (
                    <option key={key} value={key}>{def.label}</option>
                  ))}
                </select>
              </label>
              <FieldInput
                field={{ key: 'dimension', label: 'Dimension', type: 'number', placeholder: '3072' }}
                value={doc.embedding?.dimension}
                onChange={v => set(['embedding', 'dimension'], v)}
              />
            </div>
            <FieldGrid
              fields={EMBEDDING_PROVIDERS[doc.embedding?.provider ?? 'openai'].fields}
              doc={doc}
              basePath={['embedding', doc.embedding?.provider ?? 'openai']}
              onSet={set}
            />
          </div>
        ) : (
          <p className="text-xs text-ink-muted">Using defaults (OPENAI_API_KEY env var, text-embedding-3-large, 3072 dimensions).</p>
        )}
      </SectionCard>

      <SectionCard
        title="Markdown store"
        subtitle="optional — store crawled markdown in Postgres"
        action={
          <label className="flex items-center gap-2 text-xs text-ink-secondary">
            <input
              type="checkbox"
              checked={markdownStoreEnabled}
              onChange={e => set(['markdown_store'], e.target.checked ? {} : undefined)}
              className="h-4 w-4 accent-(--accent)"
            />
            enable
          </label>
        }
      >
        {markdownStoreEnabled ? (
          <FieldGrid fields={MARKDOWN_STORE_FIELDS} doc={doc} basePath={['markdown_store']} onSet={set} />
        ) : (
          <p className="text-xs text-ink-muted">Disabled.</p>
        )}
      </SectionCard>

      <div className="flex items-center gap-3">
        <button
          onClick={() => save.mutate()}
          disabled={save.isPending || sources.length === 0 || missingRequired.length > 0}
          className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {save.isPending ? 'Saving…' : props.mode === 'create' ? 'Create config' : 'Save changes'}
        </button>
        {props.onCancel && (
          <button onClick={props.onCancel} className="text-sm text-ink-muted hover:text-ink-secondary">
            Cancel
          </button>
        )}
        {missingRequired.length > 0 && (
          <span className="text-xs text-warning">missing: {missingRequired.slice(0, 3).join(', ')}{missingRequired.length > 3 ? '…' : ''}</span>
        )}
        {sources.length === 0 && <span className="text-xs text-warning">add at least one source</span>}
        {props.mode === 'edit' && (
          <span className="ml-auto text-xs text-ink-muted">Saving rewrites the YAML file (comments are not preserved).</span>
        )}
      </div>
    </div>
  );
}
