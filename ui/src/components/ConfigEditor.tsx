import { useMemo, useState } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { yaml } from '@codemirror/lang-yaml';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, ApiError, ConfigRecord } from '../api';

type Props =
  | { mode: 'view'; config: ConfigRecord }
  | { mode: 'edit'; config: ConfigRecord; onSaved?: (record: ConfigRecord) => void }
  | { mode: 'create'; initialContent: string; onSaved?: (record: ConfigRecord) => void; onCancel?: () => void };

export default function ConfigEditor(props: Props) {
  const queryClient = useQueryClient();
  const initial = props.mode === 'create' ? props.initialContent : props.config.content;
  const [content, setContent] = useState(initial);
  const [filename, setFilename] = useState('new-config.yaml');
  const [validation, setValidation] = useState<string | null>(null);

  const dirty = content !== initial;
  const dark = useMemo(() => window.matchMedia('(prefers-color-scheme: dark)').matches, []);

  const save = useMutation({
    mutationFn: async () => {
      const check = await api.validateConfig(content);
      if (!check.valid) {
        throw new ApiError(400, check.error ?? 'invalid config');
      }
      if (props.mode === 'create') {
        return api.createConfig(filename, content);
      }
      if (props.mode === 'edit') {
        return api.updateConfig(props.config.id, content, props.config.content_hash);
      }
      throw new Error('unreachable');
    },
    onSuccess: record => {
      setValidation(null);
      queryClient.invalidateQueries({ queryKey: ['configs'] });
      if (props.mode !== 'view') props.onSaved?.(record);
    },
    onError: err => setValidation((err as ApiError).message),
  });

  const conflict = save.error instanceof ApiError && save.error.status === 409;
  const readOnly = props.mode === 'view';

  return (
    <div className="space-y-3">
      {props.mode === 'create' && (
        <label className="block text-sm">
          <span className="mb-1 block text-xs uppercase tracking-wide text-ink-muted">Filename</span>
          <input
            value={filename}
            onChange={e => setFilename(e.target.value)}
            className="w-72 rounded-md border border-edge bg-page px-3 py-1.5 font-mono text-sm outline-none focus:border-accent"
            placeholder="my-config.yaml"
          />
        </label>
      )}

      {conflict && (
        <div className="rounded-md border border-warning/50 bg-surface px-3 py-2 text-sm text-ink-secondary">
          ⚠ The file changed on disk since you loaded it. Reload the page and reapply your edits.
        </div>
      )}
      {validation && !conflict && (
        <div className="rounded-md border border-critical/50 bg-surface px-3 py-2 text-sm text-critical">{validation}</div>
      )}

      <div className="overflow-hidden rounded-md border border-edge">
        <CodeMirror
          value={content}
          onChange={setContent}
          extensions={[yaml()]}
          editable={!readOnly}
          theme={dark ? 'dark' : 'light'}
          minHeight="240px"
          maxHeight="560px"
        />
      </div>

      {!readOnly && (
        <div className="flex items-center gap-3">
          <button
            onClick={() => save.mutate()}
            disabled={save.isPending || (props.mode === 'edit' && !dirty)}
            className="rounded-md bg-accent px-3.5 py-1.5 text-sm font-medium text-white transition hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {save.isPending ? 'Saving…' : props.mode === 'create' ? 'Create config' : 'Save changes'}
          </button>
          {props.mode === 'create' && (
            <button onClick={() => props.onCancel?.()} className="text-sm text-ink-muted hover:text-ink-secondary">
              Cancel
            </button>
          )}
          {props.mode === 'edit' && dirty && <span className="text-xs text-ink-muted">unsaved changes</span>}
          <span className="ml-auto text-xs text-ink-muted">
            Secrets stay as <code>{'${ENV_VAR}'}</code> placeholders — they are resolved only inside sync jobs.
          </span>
        </div>
      )}
    </div>
  );
}
