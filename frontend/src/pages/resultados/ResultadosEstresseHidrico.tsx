import { useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type {
  FiltrosEstresseHidrico,
  ResultadoEstresseHidrico,
} from '@/api/endpoints/estresseHidrico';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Input } from '@/components/Input';
import { Select } from '@/components/Select';
import { Table, type TableColumn } from '@/components/Table';
import { Header } from '@/features/layout/Header';
import { useListarResultadosEstresseHidrico } from '@/hooks/useEstresseHidrico';
import { formatDecimal, truncateId } from '@/lib/format';

const LIMIT = 25;

const CENARIO_OPTIONS = [
  { value: '', label: 'Todos' },
  { value: 'rcp45', label: 'rcp45' },
  { value: 'rcp85', label: 'rcp85' },
];

function parseFilters(params: URLSearchParams): FiltrosEstresseHidrico {
  const asStr = (k: string) => params.get(k) ?? undefined;
  const asNum = (k: string) => {
    const raw = params.get(k);
    return raw === null || raw === '' ? undefined : Number(raw);
  };
  return {
    execucao_id: asStr('execucao_id'),
    cenario: asStr('cenario'),
    ano: asNum('ano'),
    ano_min: asNum('ano_min'),
    ano_max: asNum('ano_max'),
    municipio_id: asNum('municipio_id'),
    uf: asStr('uf'),
  };
}

function filtersToParams(filters: FiltrosEstresseHidrico): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(filters)) {
    if (value === undefined || value === null || value === '') continue;
    out[key] = String(value);
  }
  return out;
}

export default function ResultadosEstresseHidricoPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [offset, setOffset] = useState(0);

  const filters = useMemo(() => parseFilters(searchParams), [searchParams]);

  const updateFilters = (next: FiltrosEstresseHidrico) => {
    setOffset(0);
    setSearchParams(filtersToParams(next), { replace: true });
  };

  const num = (v: string): number | undefined => (v === '' ? undefined : Number(v));

  const resultados = useListarResultadosEstresseHidrico({
    ...filters,
    limit: LIMIT,
    offset,
  });

  const columns: TableColumn<ResultadoEstresseHidrico>[] = [
    {
      key: 'municipio',
      header: 'Município',
      render: (row) =>
        row.nome_municipio ? (
          <span>{row.nome_municipio}</span>
        ) : (
          <span className="font-mono text-xs">{row.municipio_id}</span>
        ),
    },
    { key: 'uf', header: 'UF', render: (row) => row.uf ?? '—' },
    { key: 'cenario', header: 'Cenário', render: (row) => row.cenario },
    { key: 'ano', header: 'Ano', render: (row) => row.ano },
    {
      key: 'frequencia',
      header: 'Frequência (dias)',
      render: (row) => row.frequencia_dias_secos_quentes,
    },
    {
      key: 'intensidade',
      header: 'Intensidade (mm/dia)',
      render: (row) => formatDecimal(row.intensidade_mm_dia),
    },
    {
      key: 'execucao_id',
      header: 'Execução',
      render: (row) => (
        <span className="font-mono text-xs" title={row.execucao_id}>
          {truncateId(row.execucao_id)}
        </span>
      ),
    },
  ];

  const mostraLinha =
    filters.ano_min !== undefined && filters.ano_max !== undefined && resultados.data;

  const chartData = useMemo(() => {
    if (!mostraLinha || !resultados.data) return [];
    const porAno = new Map<number, { soma: number; n: number }>();
    for (const item of resultados.data.items) {
      const existente = porAno.get(item.ano);
      if (existente) {
        existente.soma += item.frequencia_dias_secos_quentes;
        existente.n += 1;
      } else {
        porAno.set(item.ano, { soma: item.frequencia_dias_secos_quentes, n: 1 });
      }
    }
    return Array.from(porAno.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([ano, { soma, n }]) => ({ ano, freq_media: soma / n }));
  }, [mostraLinha, resultados.data]);

  return (
    <>
      <Header
        title="Resultados — estresse hídrico"
        breadcrumbs={[{ label: 'Resultados', to: '/resultados' }, { label: 'Estresse hídrico' }]}
        description="Frequência e intensidade anuais por município."
      />
      <div className="grid gap-6 p-8 lg:grid-cols-[18rem_1fr]">
        <Card title="Filtros" bodyClassName="flex flex-col gap-4">
          <Input
            label="Execução ID"
            value={filters.execucao_id ?? ''}
            onChange={(e) => updateFilters({ ...filters, execucao_id: e.target.value || undefined })}
            placeholder="exec_..."
          />
          <Select
            label="Cenário"
            value={filters.cenario ?? ''}
            onChange={(e) => updateFilters({ ...filters, cenario: e.target.value || undefined })}
            options={CENARIO_OPTIONS}
          />
          <Input
            label="Ano"
            type="number"
            value={filters.ano ?? ''}
            onChange={(e) => updateFilters({ ...filters, ano: num(e.target.value) })}
            hint="Deixe vazio para usar range."
          />
          <div className="grid grid-cols-2 gap-2">
            <Input
              label="Ano mín."
              type="number"
              value={filters.ano_min ?? ''}
              onChange={(e) => updateFilters({ ...filters, ano_min: num(e.target.value) })}
            />
            <Input
              label="Ano máx."
              type="number"
              value={filters.ano_max ?? ''}
              onChange={(e) => updateFilters({ ...filters, ano_max: num(e.target.value) })}
            />
          </div>
          <div className="grid grid-cols-2 gap-2">
            <Input
              label="UF"
              value={filters.uf ?? ''}
              onChange={(e) =>
                updateFilters({ ...filters, uf: e.target.value.toUpperCase() || undefined })
              }
              placeholder="SP"
              maxLength={2}
            />
            <Input
              label="Município ID"
              type="number"
              value={filters.municipio_id ?? ''}
              onChange={(e) => updateFilters({ ...filters, municipio_id: num(e.target.value) })}
            />
          </div>
        </Card>

        <div className="flex flex-col gap-6">
          {mostraLinha && chartData.length > 1 ? (
            <Card title="Frequência média por ano">
              <div className="h-64 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                    <XAxis dataKey="ano" tick={{ fill: '#475569', fontSize: 12 }} />
                    <YAxis tick={{ fill: '#475569', fontSize: 12 }} />
                    <Tooltip
                      contentStyle={{ borderRadius: 6, borderColor: '#cbd5e1', fontSize: 12 }}
                    />
                    <Line
                      type="monotone"
                      dataKey="freq_media"
                      stroke="#0369a1"
                      strokeWidth={2}
                      dot={{ r: 3 }}
                      name="Frequência média (dias)"
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </Card>
          ) : null}
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
        </div>
      </div>
    </>
  );
}
