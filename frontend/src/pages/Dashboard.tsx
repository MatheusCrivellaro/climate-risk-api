import { Activity, Database, Package, PlayCircle } from 'lucide-react';
import { Link } from 'react-router-dom';
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { StatusBadge } from '@/components/Badge';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Spinner } from '@/components/Spinner';
import { Table, type TableColumn } from '@/components/Table';
import { MetricCard } from '@/features/dashboard/MetricCard';
import { Header } from '@/features/layout/Header';
import { useAdminStats } from '@/hooks/useAdmin';
import { useListarExecucoes } from '@/hooks/useExecucoes';
import { useListarJobs } from '@/hooks/useJobs';
import { formatDateTime, formatNumber, truncateId } from '@/lib/format';
import type { ExecucaoResumo } from '@/api/endpoints/execucoes';

export default function DashboardPage() {
  const stats = useAdminStats();
  const execucoes = useListarExecucoes({ limit: 5, offset: 0 });
  const execucoesPorTipo = useListarExecucoes({ limit: 500, offset: 0 });
  const jobsPending = useListarJobs({ status: 'pending', limit: 1, offset: 0 });
  const jobsRunning = useListarJobs({ status: 'running', limit: 1, offset: 0 });
  const jobsCompleted = useListarJobs({ status: 'completed', limit: 1, offset: 0 });
  const jobsFailed = useListarJobs({ status: 'failed', limit: 1, offset: 0 });
  const jobsCanceled = useListarJobs({ status: 'canceled', limit: 1, offset: 0 });

  const jobBuckets = [
    { status: 'pending', query: jobsPending },
    { status: 'running', query: jobsRunning },
    { status: 'completed', query: jobsCompleted },
    { status: 'failed', query: jobsFailed },
    { status: 'canceled', query: jobsCanceled },
  ];

  return (
    <>
      <Header
        title="Dashboard"
        description="Visão operacional das execuções, jobs e resultados."
      />
      <div className="flex flex-col gap-6 p-8">
        <MetricsSection
          loading={stats.isLoading}
          error={stats.error}
          data={stats.data}
          breakdownTipos={breakdownPorTipo(execucoesPorTipo.data?.items ?? [])}
        />

        <div className="grid gap-6 lg:grid-cols-3">
          <Card title="Jobs por status" className="lg:col-span-2">
            <JobsChart
              buckets={jobBuckets.map((bucket) => ({
                status: bucket.status,
                total: bucket.query.data?.total ?? 0,
                loading: bucket.query.isLoading,
                error: bucket.query.error,
              }))}
            />
          </Card>

          <Card
            title="Últimas execuções"
            action={
              <Link to="/execucoes" className="text-sm font-medium text-primary-700 hover:underline">
                Ver todas →
              </Link>
            }
          >
            <UltimasExecucoes
              loading={execucoes.isLoading}
              error={execucoes.error}
              items={execucoes.data?.items ?? []}
            />
          </Card>
        </div>
      </div>
    </>
  );
}

function breakdownPorTipo(items: ExecucaoResumo[]): { precipitacao: number; estresse: number } {
  let precipitacao = 0;
  let estresse = 0;
  for (const item of items) {
    if (item.tipo === 'estresse_hidrico') {
      estresse += 1;
    } else {
      precipitacao += 1;
    }
  }
  return { precipitacao, estresse };
}

interface MetricsSectionProps {
  loading: boolean;
  error: Error | null;
  data:
    | {
        contadores: { fornecedores: number; municipios: number; jobs: number; execucoes: number };
        total_resultados: number;
      }
    | undefined;
  breakdownTipos: { precipitacao: number; estresse: number };
}

function MetricsSection({ loading, error, data, breakdownTipos }: MetricsSectionProps) {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-10">
        <Spinner />
      </div>
    );
  }
  if (error) {
    return <ErrorState error={error} title="Falha ao carregar métricas" />;
  }
  if (!data) {
    return null;
  }
  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      <MetricCard
        label="Execuções"
        value={formatNumber(data.contadores.execucoes)}
        icon={PlayCircle}
        accent="primary"
        helper={`precipitação extrema: ${formatNumber(breakdownTipos.precipitacao)} · estresse hídrico: ${formatNumber(breakdownTipos.estresse)}`}
      />
      <MetricCard
        label="Jobs"
        value={formatNumber(data.contadores.jobs)}
        icon={Activity}
        accent="amber"
      />
      <MetricCard
        label="Fornecedores"
        value={formatNumber(data.contadores.fornecedores)}
        icon={Package}
        accent="green"
      />
      <MetricCard
        label="Resultados"
        value={formatNumber(data.total_resultados)}
        icon={Database}
        accent="primary"
      />
    </div>
  );
}

interface JobsChartProps {
  buckets: { status: string; total: number; loading: boolean; error: Error | null }[];
}

function JobsChart({ buckets }: JobsChartProps) {
  const anyLoading = buckets.some((b) => b.loading);
  const firstError = buckets.find((b) => b.error)?.error;

  if (anyLoading) {
    return (
      <div className="flex h-64 items-center justify-center">
        <Spinner />
      </div>
    );
  }
  if (firstError) {
    return <ErrorState error={firstError} title="Falha ao consultar jobs" />;
  }

  const data = buckets.map((b) => ({ status: b.status, total: b.total }));

  return (
    <div className="h-64 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
          <XAxis dataKey="status" tick={{ fill: '#475569', fontSize: 12 }} />
          <YAxis allowDecimals={false} tick={{ fill: '#475569', fontSize: 12 }} />
          <Tooltip
            cursor={{ fill: '#f1f5f9' }}
            contentStyle={{ borderRadius: 6, borderColor: '#cbd5e1', fontSize: 12 }}
          />
          <Bar dataKey="total" fill="#0369a1" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

interface UltimasExecucoesProps {
  loading: boolean;
  error: Error | null;
  items: ExecucaoResumo[];
}

function UltimasExecucoes({ loading, error, items }: UltimasExecucoesProps) {
  if (error) return <ErrorState error={error} title="Falha ao carregar execuções" />;
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
    { key: 'status', header: 'Status', render: (row) => <StatusBadge status={row.status} /> },
    {
      key: 'criado_em',
      header: 'Criado em',
      render: (row) => (
        <span className="text-xs text-slate-600">{formatDateTime(row.criado_em)}</span>
      ),
    },
  ];
  return (
    <Table
      columns={columns}
      data={items}
      rowKey={(row) => row.id}
      loading={loading}
      emptyTitle="Nenhuma execução ainda"
      emptyDescription="Crie uma execução para ver dados aqui."
    />
  );
}
