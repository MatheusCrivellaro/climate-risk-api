import { useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import type {
  CalculoPontosAsyncResponse,
  CalculoPorPontosRequest,
  CalculoPorPontosResponse,
} from '@/api/endpoints/calculos';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Input } from '@/components/Input';
import { Select } from '@/components/Select';
import { Table, type TableColumn } from '@/components/Table';
import { Textarea } from '@/components/Textarea';
import { PontosEditor, type PontoRow } from '@/features/calculos/PontosEditor';
import { parsePontosCSV } from '@/features/calculos/pontosCsv';
import { Header } from '@/features/layout/Header';
import { useCalcularPontos } from '@/hooks/useCalculos';
import { formatDecimal, formatNumber } from '@/lib/format';

type PontoResultadoRow = CalculoPorPontosResponse['resultados'][number];

function isAsync(
  value: CalculoPorPontosResponse | CalculoPontosAsyncResponse,
): value is CalculoPontosAsyncResponse {
  return 'execucao_id' in value;
}

export default function CalculosPontosPage() {
  const navigate = useNavigate();
  const calcular = useCalcularPontos();
  const [pontos, setPontos] = useState<PontoRow[]>([
    { lat: '-23.55', lon: '-46.63', identificador: 'forn-001' },
  ]);
  const [arquivo, setArquivo] = useState('');
  const [cenario, setCenario] = useState('rcp45');
  const [variavel, setVariavel] = useState('pr');
  const [csv, setCsv] = useState('');
  const [formError, setFormError] = useState<string | null>(null);
  const [result, setResult] = useState<CalculoPorPontosResponse | null>(null);

  const importCsv = () => {
    const parsed = parsePontosCSV(csv);
    if (parsed.length === 0) {
      setFormError('Nenhum ponto encontrado no CSV.');
      return;
    }
    setPontos(parsed);
    setCsv('');
    setFormError(null);
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setFormError(null);
    setResult(null);

    if (!arquivo.trim()) {
      setFormError('Informe o arquivo .nc.');
      return;
    }

    const pontosValidos = pontos
      .filter((p) => p.lat.trim() && p.lon.trim())
      .map((p) => ({
        lat: Number(p.lat),
        lon: Number(p.lon),
        identificador: p.identificador.trim() || null,
      }));

    if (pontosValidos.length === 0) {
      setFormError('Informe ao menos um ponto com latitude e longitude.');
      return;
    }

    const body: CalculoPorPontosRequest = {
      arquivo_nc: arquivo.trim(),
      cenario,
      variavel,
      pontos: pontosValidos,
      parametros_indices: {
        freq_thr_mm: 20,
        heavy20: 20,
        heavy50: 50,
        p95_wet_thr: 1,
        p95_baseline: { inicio: 2026, fim: 2035 },
      },
    };

    calcular.mutate(body, {
      onSuccess: (response) => {
        if (isAsync(response)) {
          navigate(`/execucoes/${response.execucao_id}`);
        } else {
          setResult(response);
        }
      },
    });
  };

  const resultColumns: TableColumn<PontoResultadoRow>[] = [
    { key: 'identificador', header: 'ID', render: (row) => row.identificador ?? '—' },
    { key: 'ano', header: 'Ano', render: (row) => row.ano },
    {
      key: 'lat_grid',
      header: 'lat/lon grid',
      render: (row) => (
        <span className="font-mono text-xs">
          {formatDecimal(row.lat_grid)} / {formatDecimal(row.lon_grid)}
        </span>
      ),
    },
    { key: 'wet_days', header: 'wet_days', render: (row) => formatNumber(row.indices.wet_days) },
    { key: 'sdii', header: 'sdii', render: (row) => formatDecimal(row.indices.sdii) },
    { key: 'rx1day', header: 'rx1day', render: (row) => formatDecimal(row.indices.rx1day) },
    { key: 'rx5day', header: 'rx5day', render: (row) => formatDecimal(row.indices.rx5day) },
    { key: 'r20mm', header: 'r20mm', render: (row) => formatNumber(row.indices.r20mm) },
    { key: 'r50mm', header: 'r50mm', render: (row) => formatNumber(row.indices.r50mm) },
  ];

  return (
    <>
      <Header
        title="Cálculo por pontos"
        breadcrumbs={[{ label: 'Cálculos' }]}
        description="Executa síncrono se ≤100 pontos; acima disso cria execução assíncrona."
      />
      <form onSubmit={handleSubmit} className="flex flex-col gap-6 p-8">
        <Card title="Dataset">
          <div className="grid gap-4 md:grid-cols-3">
            <div className="md:col-span-3">
              <Input
                label="Arquivo .nc"
                placeholder="/dados/cordex/rcp45/pr_day_BR_2026-2030.nc"
                value={arquivo}
                onChange={(e) => setArquivo(e.target.value)}
              />
            </div>
            <Select
              label="Cenário"
              value={cenario}
              onChange={(e) => setCenario(e.target.value)}
              options={[
                { value: 'rcp45', label: 'rcp45' },
                { value: 'rcp85', label: 'rcp85' },
              ]}
            />
            <Select
              label="Variável"
              value={variavel}
              onChange={(e) => setVariavel(e.target.value)}
              options={[{ value: 'pr', label: 'pr' }]}
            />
          </div>
        </Card>

        <Card
          title="Pontos"
          description={`${pontos.length} ponto${pontos.length === 1 ? '' : 's'} configurado${pontos.length === 1 ? '' : 's'}.`}
        >
          <div className="flex flex-col gap-4">
            <PontosEditor pontos={pontos} onChange={setPontos} />

            <details className="rounded-md border border-slate-200 bg-slate-50 p-3">
              <summary className="cursor-pointer text-sm font-medium text-slate-700">
                Importar de CSV
              </summary>
              <div className="mt-3 flex flex-col gap-2">
                <Textarea
                  rows={6}
                  value={csv}
                  onChange={(e) => setCsv(e.target.value)}
                  placeholder="lat,lon,identificador&#10;-23.55,-46.63,forn-001"
                />
                <div>
                  <Button size="sm" type="button" variant="secondary" onClick={importCsv}>
                    Importar
                  </Button>
                </div>
              </div>
            </details>
          </div>
        </Card>

        {formError ? <ErrorState error={new Error(formError)} /> : null}
        {calcular.error ? <ErrorState error={calcular.error} title="Falha ao calcular" /> : null}

        <div className="flex justify-end">
          <Button type="submit" loading={calcular.isPending}>
            Calcular
          </Button>
        </div>
      </form>

      {result ? (
        <div className="px-8 pb-8">
          <Card
            title="Resultados"
            description={`${result.total_resultados} linha${result.total_resultados === 1 ? '' : 's'} para ${result.total_pontos} ponto${result.total_pontos === 1 ? '' : 's'}.`}
          >
            <Table
              columns={resultColumns}
              data={result.resultados}
              rowKey={(row) => `${row.identificador ?? ''}-${row.ano}-${row.lat_input}-${row.lon_input}`}
            />
          </Card>
        </div>
      ) : null}
    </>
  );
}
