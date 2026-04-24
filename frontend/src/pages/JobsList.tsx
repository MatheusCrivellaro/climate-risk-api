import { useMemo, useState } from 'react';
import type { JobResponse } from '@/api/endpoints/jobs';
import { StatusBadge } from '@/components/Badge';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Modal } from '@/components/Modal';
import { Select } from '@/components/Select';
import { Table, type TableColumn } from '@/components/Table';
import { Header } from '@/features/layout/Header';
import { useListarJobs, useReprocessarJob } from '@/hooks/useJobs';
import { formatDateTime, truncateId } from '@/lib/format';

const STATUS_OPTIONS = [
  { value: '', label: 'Todos' },
  { value: 'pending', label: 'pending' },
  { value: 'running', label: 'running' },
  { value: 'completed', label: 'completed' },
  { value: 'failed', label: 'failed' },
  { value: 'canceled', label: 'canceled' },
];

const TIPO_OPTIONS = [
  { value: '', label: 'Todos' },
  { value: 'cordex', label: 'cordex' },
  { value: 'calcular_pontos', label: 'calcular_pontos' },
  { value: 'noop', label: 'noop' },
];

const LIMIT = 20;

export default function JobsListPage() {
  const [filters, setFilters] = useState({ status: '', tipo: '' });
  const [offset, setOffset] = useState(0);
  const [retryTarget, setRetryTarget] = useState<JobResponse | null>(null);

  const params = useMemo(
    () => ({
      limit: LIMIT,
      offset,
      status: filters.status || undefined,
      tipo: filters.tipo || undefined,
    }),
    [filters, offset],
  );

  const { data, isLoading, error } = useListarJobs(params);
  const retry = useReprocessarJob();

  const columns: TableColumn<JobResponse>[] = [
    {
      key: 'id',
      header: 'ID',
      render: (row) => (
        <span className="font-mono text-xs text-slate-800" title={row.id}>
          {truncateId(row.id)}
        </span>
      ),
    },
    { key: 'tipo', header: 'Tipo', render: (row) => row.tipo },
    { key: 'status', header: 'Status', render: (row) => <StatusBadge status={row.status} /> },
    {
      key: 'tentativas',
      header: 'Tentativas',
      render: (row) => `${row.tentativas}/${row.max_tentativas}`,
    },
    {
      key: 'heartbeat',
      header: 'Heartbeat',
      render: (row) => (
        <span className="text-xs text-slate-600">{formatDateTime(row.heartbeat)}</span>
      ),
    },
    {
      key: 'criado_em',
      header: 'Criado em',
      render: (row) => (
        <span className="text-xs text-slate-600">{formatDateTime(row.criado_em)}</span>
      ),
    },
    {
      key: 'acao',
      header: 'Ações',
      render: (row) =>
        row.status === 'failed' ? (
          <Button size="sm" variant="secondary" onClick={() => setRetryTarget(row)}>
            Retry
          </Button>
        ) : (
          <span className="text-xs text-slate-400">—</span>
        ),
    },
  ];

  const updateFilter = (key: keyof typeof filters, value: string) => {
    setOffset(0);
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  return (
    <>
      <Header title="Jobs" breadcrumbs={[{ label: 'Jobs' }]} />
      <div className="p-8">
        <Card title="Filtros" bodyClassName="grid gap-4 md:grid-cols-2">
          <Select
            label="Status"
            value={filters.status}
            onChange={(e) => updateFilter('status', e.target.value)}
            options={STATUS_OPTIONS}
          />
          <Select
            label="Tipo"
            value={filters.tipo}
            onChange={(e) => updateFilter('tipo', e.target.value)}
            options={TIPO_OPTIONS}
          />
        </Card>

        <div className="mt-6">
          {error ? (
            <ErrorState error={error} title="Falha ao listar jobs" />
          ) : (
            <Table
              columns={columns}
              data={data?.items}
              rowKey={(row) => row.id}
              loading={isLoading}
              emptyTitle="Nenhum job encontrado"
              pagination={
                data
                  ? {
                      total: data.total,
                      limit: data.limit,
                      offset: data.offset,
                      onChange: setOffset,
                    }
                  : undefined
              }
            />
          )}
        </div>
      </div>

      <Modal
        open={retryTarget !== null}
        onClose={() => setRetryTarget(null)}
        title="Reprocessar job?"
        footer={
          <>
            <Button variant="secondary" onClick={() => setRetryTarget(null)}>
              Voltar
            </Button>
            <Button
              loading={retry.isPending}
              onClick={() => {
                if (!retryTarget) return;
                retry.mutate(retryTarget.id, {
                  onSuccess: () => setRetryTarget(null),
                });
              }}
            >
              Reprocessar
            </Button>
          </>
        }
      >
        <p className="text-sm text-slate-700">
          O job será reenfileirado para execução com as mesmas entradas. Útil quando a falha foi
          transitória.
        </p>
        {retry.error ? (
          <div className="mt-4">
            <ErrorState error={retry.error} title="Falha ao reprocessar" />
          </div>
        ) : null}
      </Modal>
    </>
  );
}
