import { ApiError } from '@/api/client';
import { Alert } from './Alert';

export interface ErrorStateProps {
  error: unknown;
  title?: string;
}

export function ErrorState({ error, title }: ErrorStateProps) {
  if (error instanceof ApiError) {
    return (
      <Alert tone="danger" title={title ?? error.title}>
        <p>{error.detail ?? 'Não foi possível concluir a operação.'}</p>
        {error.problem?.correlation_id ? (
          <p className="mt-1 text-xs opacity-70">
            correlation_id: <span className="font-mono">{error.problem.correlation_id}</span>
          </p>
        ) : null}
      </Alert>
    );
  }
  const message = error instanceof Error ? error.message : 'Erro desconhecido';
  return (
    <Alert tone="danger" title={title ?? 'Erro'}>
      {message}
    </Alert>
  );
}
