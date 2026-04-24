import { useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type {
  AgregacaoTipo,
  FiltrosResultados,
  ResultadoResponse,
} from '@/api/endpoints/resultados';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Select } from '@/components/Select';
import { Spinner } from '@/components/Spinner';
import { Table, type TableColumn } from '@/components/Table';
import { FiltrosPanel } from '@/features/resultados/FiltrosPanel';
import { Header } from '@/features/layout/Header';
import { useListarAgregados, useListarResultados } from '@/hooks/useResultados';
import { formatDecimal, formatNumber, truncateId } from '@/lib/format';

const LIMIT = 25;

const AGREGACAO_OPTIONS: { value: AgregacaoTipo; label: string }[] = [
  { value: 'media', label: 'Média' },
  { value: 'min', label: 'Mínimo' },
  { value: 'max', label: 'Máximo' },
  { value: 'p50', label: 'P50' },
  { value: 'p95', label: 'P95' },
];

const AGRUPAR_OPTIONS = [
  { value: 'ano', label: 'Por ano' },
  { value: 'cenario', label: 'Por cenário' },
  { value: 'variavel', label: 'Por variável' },
  { value: 'nome_indice', label: 'Por índice' },
  { value: 'municipio_id', label: 'Por município' },
];

function parseFilters(params: URLSearchParams): FiltrosResultados {
  const asStr = (k: string) => params.get(k) ?? undefined;
  const asNum = (k: string) => {
    const raw = params.get(k);
    return raw === null || raw === '' ? undefined : Number(raw);
  };
  return {
    execucao_id: asStr('execucao_id'),
    cenario: asStr('cenario'),
    variavel: asStr('variavel'),
    ano: asNum('ano'),
    ano_min: asNum('ano_min'),
    ano_max: asNum('ano_max'),
    nomes_indices: asStr('nomes_indices'),
    lat_min: asNum('lat_min'),
    lat_max: asNum('lat_max'),
    lon_min: asNum('lon_min'),
    lon_max: asNum('lon_max'),
    centro_lat: asNum('centro_lat'),
    centro_lon: asNum('centro_lon'),
    raio_km: asNum('raio_km'),
    uf: asStr('uf'),
    municipio_id: asNum('municipio_id'),
  };
}

function filtersToParams(filters: FiltrosResultados): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(filters)) {
    if (value === undefined || value === null || value === '') continue;
    out[key] = String(value);
  }
  return out;
}

export default function ResultadosPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [offset, setOffset] = useState(0);
  const [mostrarAgregados, setMostrarAgregados] = useState(false);
  const [agregacao, setAgregacao] = useState<AgregacaoTipo>('media');
  const [agruparPor, setAgruparPor] = useState('ano');

  const filters = useMemo(() => parseFilters(searchParams), [searchParams]);

  const updateFilters = (next: FiltrosResultados) => {
    setOffset(0);
    setSearchParams(filtersToParams(next), { replace: true });
  };

  const resultados = useListarResultados(
    { ...filters, limit: LIMIT, offset },
    !mostrarAgregados,
  );

  const agregados = useListarAgregados(
    { ...filters, agregacao, agrupar_por: agruparPor },
    mostrarAgregados,
  );

  const columns: TableColumn<ResultadoResponse>[] = [
    {
      key: 'execucao_id',
      header: 'Execução',
      render: (row) => (
        <span className="font-mono text-xs" title={row.execucao_id}>
          {truncateId(row.execucao_id)}
        </span>
      ),
    },
    { key: 'ano', header: 'Ano', render: (row) => row.ano },
    { key: 'nome_indice', header: 'Índice', render: (row) => row.nome_indice },
    { key: 'valor', header: 'Valor', render: (row) => formatDecimal(row.valor) },
    { key: 'unidade', header: 'Unidade', render: (row) => row.unidade ?? '—' },
    {
      key: 'lat_lon',
      header: 'lat/lon',
      render: (row) => (
        <span className="font-mono text-xs">
          {formatDecimal(row.lat)} / {formatDecimal(row.lon)}
        </span>
      ),
    },
    {
      key: 'municipio_id',
      header: 'Município',
      render: (row) => row.municipio_id ?? '—',
    },
  ];

  return (
    <>
      <Header
        title="Resultados"
        breadcrumbs={[{ label: 'Resultados' }]}
        description="Consulta paginada com filtros e agregações configuráveis."
        actions={
          <label className="flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={mostrarAgregados}
              onChange={(e) => setMostrarAgregados(e.target.checked)}
              className="h-4 w-4"
            />
            Ver agregados
          </label>
        }
      />
      <div className="grid gap-6 p-8 lg:grid-cols-[18rem_1fr]">
        <FiltrosPanel value={filters} onChange={updateFilters} />

        <div className="flex flex-col gap-6">
          {mostrarAgregados ? (
            <Card
              title="Agregados"
              description="Função aplicada sobre os resultados filtrados."
              bodyClassName="flex flex-col gap-4"
            >
              <div className="grid gap-4 md:grid-cols-2">
                <Select
                  label="Agregação"
                  value={agregacao}
                  onChange={(e) => setAgregacao(e.target.value as AgregacaoTipo)}
                  options={AGREGACAO_OPTIONS}
                />
                <Select
                  label="Agrupar por"
                  value={agruparPor}
                  onChange={(e) => setAgruparPor(e.target.value)}
                  options={AGRUPAR_OPTIONS}
                />
              </div>
              <AgregadosChart
                loading={agregados.isLoading}
                error={agregados.error}
                data={agregados.data}
              />
            </Card>
          ) : (
            <Card title="Resultados">
              {resultados.error ? (
                <ErrorState error={resultados.error} title="Falha ao consultar resultados" />
              ) : (
                <Table
                  columns={columns}
                  data={resultados.data?.items}
                  rowKey={(row) => row.id}
                  loading={resultados.isLoading}
                  emptyTitle="Nenhum resultado encontrado"
                  emptyDescription="Ajuste os filtros à esquerda."
                  pagination={
                    resultados.data
                      ? {
                          total: resultados.data.total,
                          limit: resultados.data.limit,
                          offset: resultados.data.offset,
                          onChange: setOffset,
                        }
                      : undefined
                  }
                />
              )}
            </Card>
          )}
        </div>
      </div>
    </>
  );
}

interface AgregadosChartProps {
  loading: boolean;
  error: Error | null;
  data:
    | {
        agregacao: string;
        agrupar_por?: string[];
        grupos: { grupo?: Record<string, string | number>; valor: number | null; n_amostras: number }[];
      }
    | undefined;
}

function AgregadosChart({ loading, error, data }: AgregadosChartProps) {
  if (loading) return <Spinner />;
  if (error) return <ErrorState error={error} title="Falha ao calcular agregados" />;
  if (!data) return null;
  if (data.grupos.length === 0) {
    return <p className="text-sm text-slate-500">Sem dados para os filtros aplicados.</p>;
  }

  const dimensao = data.agrupar_por?.[0] ?? 'grupo';

  const chartData = data.grupos.map((g) => {
    const label = g.grupo ? String(Object.values(g.grupo)[0] ?? '—') : '—';
    return { label, valor: g.valor ?? 0, amostras: g.n_amostras };
  });

  const useLine = dimensao === 'ano';

  return (
    <div className="h-80 w-full">
      <ResponsiveContainer width="100%" height="100%">
        {useLine ? (
          <LineChart data={chartData} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis dataKey="label" tick={{ fill: '#475569', fontSize: 12 }} />
            <YAxis tick={{ fill: '#475569', fontSize: 12 }} />
            <Tooltip contentStyle={{ borderRadius: 6, borderColor: '#cbd5e1', fontSize: 12 }} />
            <Legend />
            <Line
              type="monotone"
              dataKey="valor"
              name={data.agregacao}
              stroke="#0369a1"
              strokeWidth={2}
              dot={{ r: 3 }}
            />
          </LineChart>
        ) : (
          <BarChart data={chartData} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
            <XAxis dataKey="label" tick={{ fill: '#475569', fontSize: 12 }} />
            <YAxis tick={{ fill: '#475569', fontSize: 12 }} />
            <Tooltip contentStyle={{ borderRadius: 6, borderColor: '#cbd5e1', fontSize: 12 }} />
            <Legend />
            <Bar dataKey="valor" name={data.agregacao} fill="#0369a1" radius={[4, 4, 0, 0]} />
          </BarChart>
        )}
      </ResponsiveContainer>
      <p className="mt-2 text-xs text-slate-500">
        {formatNumber(chartData.length)} grupo{chartData.length === 1 ? '' : 's'} · dimensão:{' '}
        <code className="font-mono">{dimensao}</code>
      </p>
    </div>
  );
}
