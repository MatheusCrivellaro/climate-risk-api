import { RefreshCw } from 'lucide-react';
import { useState } from 'react';
import { Alert } from '@/components/Alert';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Modal } from '@/components/Modal';
import { Spinner } from '@/components/Spinner';
import { Header } from '@/features/layout/Header';
import { useAdminStats, useAtualizarIbge } from '@/hooks/useAdmin';
import { formatNumber } from '@/lib/format';

export default function AdminPage() {
  const stats = useAdminStats();
  const refresh = useAtualizarIbge();
  const [confirmOpen, setConfirmOpen] = useState(false);

  const contadores = stats.data?.contadores;

  return (
    <>
      <Header
        title="Admin"
        breadcrumbs={[{ label: 'Admin' }]}
        description="Operações administrativas e contadores globais."
        actions={
          <Button
            variant="secondary"
            onClick={() => setConfirmOpen(true)}
            disabled={stats.isLoading}
          >
            <RefreshCw className="h-4 w-4" aria-hidden />
            Atualizar catálogo IBGE
          </Button>
        }
      />
      <div className="flex flex-col gap-6 p-8">
        {stats.error ? (
          <ErrorState error={stats.error} title="Falha ao obter estatísticas" />
        ) : stats.isLoading || !stats.data ? (
          <div className="flex justify-center py-12">
            <Spinner />
          </div>
        ) : (
          <>
            <Card title="Contadores" bodyClassName="grid gap-4 md:grid-cols-4">
              <Metric label="Fornecedores" value={contadores?.fornecedores} />
              <Metric label="Municípios" value={contadores?.municipios} />
              <Metric label="Jobs" value={contadores?.jobs} />
              <Metric label="Execuções" value={contadores?.execucoes} />
            </Card>

            <Card title="Resultados indexados" bodyClassName="grid gap-4 md:grid-cols-2">
              <Metric
                label="Execuções com resultados"
                value={stats.data.total_execucoes_com_resultados}
              />
              <Metric label="Total de resultados" value={stats.data.total_resultados} />
            </Card>

            <Card title="Dimensões observadas" bodyClassName="grid gap-4 md:grid-cols-2">
              <Dimensao titulo="Cenários" valores={stats.data.cenarios} />
              <Dimensao titulo="Variáveis" valores={stats.data.variaveis} />
              <Dimensao titulo="Nomes de índices" valores={stats.data.nomes_indices} />
              <Dimensao
                titulo="Anos"
                valores={stats.data.anos?.map((a) => String(a))}
              />
            </Card>

            {refresh.data ? (
              <Alert tone="success" title="Catálogo IBGE atualizado">
                {formatNumber(refresh.data.total_municipios)} municípios ·{' '}
                {formatNumber(refresh.data.com_centroide)} com centroide ·{' '}
                {formatNumber(refresh.data.sem_centroide)} sem centroide.
              </Alert>
            ) : null}
          </>
        )}
      </div>

      <Modal
        open={confirmOpen}
        onClose={() => setConfirmOpen(false)}
        title="Atualizar catálogo IBGE?"
        footer={
          <>
            <Button variant="secondary" onClick={() => setConfirmOpen(false)}>
              Cancelar
            </Button>
            <Button
              loading={refresh.isPending}
              onClick={() => {
                refresh.mutate(undefined, {
                  onSuccess: () => setConfirmOpen(false),
                });
              }}
            >
              Atualizar
            </Button>
          </>
        }
      >
        <p className="text-sm text-slate-700">
          Rebaixará a tabela de municípios a partir da API pública do IBGE. A operação pode
          levar alguns minutos e invalida caches de geocodificação.
        </p>
        {refresh.error ? (
          <div className="mt-4">
            <ErrorState error={refresh.error} title="Falha ao atualizar" />
          </div>
        ) : null}
      </Modal>
    </>
  );
}

interface MetricProps {
  label: string;
  value: number | undefined;
}

function Metric({ label, value }: MetricProps) {
  return (
    <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
      <p className="mt-1 text-2xl font-semibold text-slate-900">{formatNumber(value)}</p>
    </div>
  );
}

interface DimensaoProps {
  titulo: string;
  valores: string[] | undefined;
}

function Dimensao({ titulo, valores }: DimensaoProps) {
  return (
    <div className="flex flex-col gap-2">
      <p className="text-sm font-semibold text-slate-700">{titulo}</p>
      {valores && valores.length > 0 ? (
        <div className="flex flex-wrap gap-1.5">
          {valores.map((v) => (
            <span
              key={v}
              className="rounded-full border border-slate-200 bg-white px-2 py-0.5 font-mono text-xs text-slate-700"
            >
              {v}
            </span>
          ))}
        </div>
      ) : (
        <p className="text-sm text-slate-500">—</p>
      )}
    </div>
  );
}
