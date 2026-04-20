import { useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { StatusBadge } from '@/components/Badge';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { CodeBlock } from '@/components/CodeBlock';
import { ErrorState } from '@/components/ErrorState';
import { Modal } from '@/components/Modal';
import { Spinner } from '@/components/Spinner';
import { Header } from '@/features/layout/Header';
import { useCancelarExecucao, useObterExecucao } from '@/hooks/useExecucoes';
import { useObterJob } from '@/hooks/useJobs';
import { formatDateTime } from '@/lib/format';

export default function ExecucaoDetalhePage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [confirmOpen, setConfirmOpen] = useState(false);

  const execucao = useObterExecucao(id);
  const jobId = execucao.data?.job_id ?? undefined;
  const job = useObterJob(jobId);
  const cancelar = useCancelarExecucao();

  const podeCancelar = execucao.data?.status === 'pending';

  return (
    <>
      <Header
        title="Execução"
        breadcrumbs={[
          { label: 'Execuções', to: '/execucoes' },
          { label: id ?? 'Detalhe' },
        ]}
        actions={
          execucao.data ? (
            <div className="flex gap-2">
              {podeCancelar ? (
                <Button variant="danger" onClick={() => setConfirmOpen(true)}>
                  Cancelar execução
                </Button>
              ) : null}
              <Button
                variant="secondary"
                onClick={() => navigate(`/resultados?execucao_id=${execucao.data!.id}`)}
              >
                Ver resultados
              </Button>
            </div>
          ) : null
        }
      />
      <div className="grid gap-6 p-8 lg:grid-cols-2">
        <Card title="Execução">
          {execucao.isLoading ? (
            <Spinner />
          ) : execucao.error ? (
            <ErrorState error={execucao.error} title="Falha ao carregar execução" />
          ) : execucao.data ? (
            <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-3 text-sm">
              <dt className="text-slate-500">ID</dt>
              <dd className="font-mono text-xs text-slate-900">{execucao.data.id}</dd>
              <dt className="text-slate-500">Cenário</dt>
              <dd>{execucao.data.cenario}</dd>
              <dt className="text-slate-500">Variável</dt>
              <dd>{execucao.data.variavel}</dd>
              <dt className="text-slate-500">Tipo</dt>
              <dd>{execucao.data.tipo}</dd>
              <dt className="text-slate-500">Arquivo</dt>
              <dd className="break-all font-mono text-xs">{execucao.data.arquivo_origem}</dd>
              <dt className="text-slate-500">Status</dt>
              <dd>
                <StatusBadge status={execucao.data.status} />
              </dd>
              <dt className="text-slate-500">Criado em</dt>
              <dd>{formatDateTime(execucao.data.criado_em)}</dd>
              <dt className="text-slate-500">Concluído em</dt>
              <dd>{formatDateTime(execucao.data.concluido_em)}</dd>
              <dt className="text-slate-500">Job</dt>
              <dd className="font-mono text-xs">{execucao.data.job_id ?? '—'}</dd>
            </dl>
          ) : null}
        </Card>

        <Card title="Job associado">
          {!jobId ? (
            <p className="text-sm text-slate-500">Execução não possui job associado.</p>
          ) : job.isLoading ? (
            <Spinner />
          ) : job.error ? (
            <ErrorState error={job.error} title="Falha ao carregar job" />
          ) : job.data ? (
            <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-3 text-sm">
              <dt className="text-slate-500">Tipo</dt>
              <dd>{job.data.tipo}</dd>
              <dt className="text-slate-500">Status</dt>
              <dd>
                <StatusBadge status={job.data.status} />
              </dd>
              <dt className="text-slate-500">Tentativas</dt>
              <dd>
                {job.data.tentativas} / {job.data.max_tentativas}
              </dd>
              <dt className="text-slate-500">Iniciado em</dt>
              <dd>{formatDateTime(job.data.iniciado_em)}</dd>
              <dt className="text-slate-500">Concluído em</dt>
              <dd>{formatDateTime(job.data.concluido_em)}</dd>
              <dt className="text-slate-500">Heartbeat</dt>
              <dd>{formatDateTime(job.data.heartbeat)}</dd>
              <dt className="text-slate-500">Próxima tentativa</dt>
              <dd>{formatDateTime(job.data.proxima_tentativa_em)}</dd>
              {job.data.erro ? (
                <>
                  <dt className="text-slate-500">Erro</dt>
                  <dd className="text-danger">{job.data.erro}</dd>
                </>
              ) : null}
              <dt className="text-slate-500 self-start">Payload</dt>
              <dd className="min-w-0">
                <CodeBlock value={job.data.payload} />
              </dd>
            </dl>
          ) : null}
        </Card>
      </div>

      <Modal
        open={confirmOpen}
        onClose={() => setConfirmOpen(false)}
        title="Cancelar execução?"
        footer={
          <>
            <Button variant="secondary" onClick={() => setConfirmOpen(false)}>
              Voltar
            </Button>
            <Button
              variant="danger"
              loading={cancelar.isPending}
              onClick={() => {
                if (!id) return;
                cancelar.mutate(id, {
                  onSuccess: () => setConfirmOpen(false),
                });
              }}
            >
              Cancelar execução
            </Button>
          </>
        }
      >
        <p className="text-sm text-slate-700">
          Esta ação marca a execução como <code className="font-mono">canceled</code>. Não afeta
          resultados já gravados.
        </p>
        {cancelar.error ? (
          <div className="mt-4">
            <ErrorState error={cancelar.error} title="Falha ao cancelar" />
          </div>
        ) : null}
      </Modal>
    </>
  );
}
