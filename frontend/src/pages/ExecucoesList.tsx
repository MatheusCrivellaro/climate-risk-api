import { Plus } from 'lucide-react';
import { useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import type { ExecucaoResumo } from '@/api/endpoints/execucoes';
import { StatusBadge } from '@/components/Badge';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Select } from '@/components/Select';
import { Table, type TableColumn } from '@/components/Table';
import { Header } from '@/features/layout/Header';
import { useListarExecucoes } from '@/hooks/useExecucoes';
import { formatDateTime, truncateId } from '@/lib/format';

const STATUS_OPTIONS = [
  { value: '', label: 'Todos' },
  { value: 'pending', label: 'pending' },
  { value: 'running', label: 'running' },
  { value: 'completed', label: 'completed' },
  { value: 'failed', label: 'failed' },
  { value: 'canceled', label: 'canceled' },
];

const CENARIO_OPTIONS = [
  { value: '', label: 'Todos' },
  { value: 'rcp45', label: 'rcp45' },
  { value: 'rcp85', label: 'rcp85' },
];

const VARIAVEL_OPTIONS = [
  { value: '', label: 'Todas' },
  { value: 'pr', label: 'pr' },
];

const LIMIT = 20;

export default function ExecucoesListPage() {
  const navigate = useNavigate();
  const [filters, setFilters] = useState({ status: '', cenario: '', variavel: '' });
  const [offset, setOffset] = useState(0);

  const params = useMemo(
    () => ({
      limit: LIMIT,
      offset,
      status: filters.status || undefined,
      cenario: filters.cenario || undefined,
      variavel: filters.variavel || undefined,
    }),
    [filters, offset],
  );

  const { data, isLoading, error } = useListarExecucoes(params);

  const columns: TableColumn<ExecucaoResumo>[] = [
    {
      key: 'id',
      header: 'ID',
      render: (row) => (
        <Link
          to={`/execucoes/${row.id}`}
          className="font-mono text-xs text-primary-700 hover:underline"
          title={row.id}
        >
          {truncateId(row.id)}
        </Link>
      ),
    },
    { key: 'cenario', header: 'Cenário', render: (row) => row.cenario },
    { key: 'variavel', header: 'Variável', render: (row) => row.variavel },
    { key: 'tipo', header: 'Tipo', render: (row) => row.tipo },
    { key: 'status', header: 'Status', render: (row) => <StatusBadge status={row.status} /> },
    {
      key: 'criado_em',
      header: 'Criado em',
      render: (row) => (
        <span className="text-xs text-slate-600">{formatDateTime(row.criado_em)}</span>
      ),
    },
    {
      key: 'concluido_em',
      header: 'Concluído em',
      render: (row) => (
        <span className="text-xs text-slate-600">{formatDateTime(row.concluido_em)}</span>
      ),
    },
  ];

  const updateFilter = (key: keyof typeof filters, value: string) => {
    setOffset(0);
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  return (
    <>
      <Header
        title="Execuções"
        breadcrumbs={[{ label: 'Execuções' }]}
        actions={
          <Button onClick={() => navigate('/execucoes/nova')}>
            <Plus className="h-4 w-4" aria-hidden />
            Nova execução
          </Button>
        }
      />
      <div className="p-8">
        <Card
          title="Filtros"
          bodyClassName="grid gap-4 md:grid-cols-3"
        >
          <Select
            label="Cenário"
            value={filters.cenario}
            onChange={(e) => updateFilter('cenario', e.target.value)}
            options={CENARIO_OPTIONS}
          />
          <Select
            label="Variável"
            value={filters.variavel}
            onChange={(e) => updateFilter('variavel', e.target.value)}
            options={VARIAVEL_OPTIONS}
          />
          <Select
            label="Status"
            value={filters.status}
            onChange={(e) => updateFilter('status', e.target.value)}
            options={STATUS_OPTIONS}
          />
        </Card>

        <div className="mt-6">
          {error ? (
            <ErrorState error={error} title="Falha ao listar execuções" />
          ) : (
            <Table
              columns={columns}
              data={data?.items}
              rowKey={(row) => row.id}
              loading={isLoading}
              emptyTitle="Nenhuma execução encontrada"
              emptyDescription="Ajuste os filtros ou crie uma nova execução."
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
    </>
  );
}
