import { useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, Route, Routes } from 'react-router-dom';
import { api } from './api';
import Dashboard from './pages/Dashboard';
import ConfigDetail from './pages/ConfigDetail';
import RunDetail from './pages/RunDetail';

export default function App() {
  const queryClient = useQueryClient();
  const { data: health } = useQuery({ queryKey: ['health'], queryFn: api.health });

  // Global event stream: any run/config change refreshes the relevant queries
  useEffect(() => {
    const source = new EventSource('/api/events');
    const invalidate = () => {
      queryClient.invalidateQueries({ queryKey: ['configs'] });
      queryClient.invalidateQueries({ queryKey: ['runs'] });
    };
    source.addEventListener('run:update', invalidate);
    source.addEventListener('config:update', invalidate);
    return () => source.close();
  }, [queryClient]);

  return (
    <div className="min-h-screen">
      <header className="border-b border-edge bg-surface">
        <div className="mx-auto flex max-w-6xl items-center gap-3 px-6 py-4">
          <Link to="/" className="flex items-center gap-2 text-lg font-semibold tracking-tight">
            <span aria-hidden>📚</span> doc2vec
          </Link>
          <span className="text-sm text-ink-muted">controller</span>
          <div className="ml-auto flex items-center gap-3 text-sm">
            {health && (
              <>
                <span
                  className="rounded-full border border-edge px-2.5 py-0.5 text-xs font-medium text-ink-secondary"
                  title={health.mode === 'rw' ? 'Configs can be created and edited from this UI' : 'Configs are managed from files only'}
                >
                  {health.mode === 'rw' ? 'read-write' : 'read-only'}
                </span>
                <span className="text-ink-muted">v{health.version}</span>
              </>
            )}
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-6xl px-6 py-8">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/configs/:id" element={<ConfigDetail />} />
          <Route path="/runs/:id" element={<RunDetail />} />
        </Routes>
      </main>
    </div>
  );
}
