import { Download, MapPin } from 'lucide-react';
import { useState } from 'react';
import type { LocalizacaoGeocodificadaSchema } from '@/api/endpoints/geocoding';
import { Alert } from '@/components/Alert';
import { Badge } from '@/components/Badge';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Table, type TableColumn } from '@/components/Table';
import { Textarea } from '@/components/Textarea';
import { Header } from '@/features/layout/Header';
import { parseCidadeUfText } from '@/features/localizacoes/parseCidadeUf';
import { useGeocodificar } from '@/hooks/useGeocoding';
import { downloadCSV, toCSV } from '@/lib/csv';
import { formatDecimal, formatNumber } from '@/lib/format';

const PLACEHOLDER = `São Paulo/SP
Rio de Janeiro/RJ
Campinas/SP`;

export default function GeocodificacaoPage() {
  const [texto, setTexto] = useState('');
  const [erro, setErro] = useState<string | null>(null);
  const geocodificar = useGeocodificar();

  const submit = () => {
    setErro(null);
    const localizacoes = parseCidadeUfText(texto);
    if (localizacoes.length === 0) {
      setErro('Informe ao menos uma linha no formato CIDADE/UF.');
      return;
    }
    geocodificar.mutate({ localizacoes });
  };

  const data = geocodificar.data;

  const columns: TableColumn<LocalizacaoGeocodificadaSchema>[] = [
    {
      key: 'cidade_entrada',
      header: 'Cidade',
      render: (row) => row.cidade_entrada,
    },
    { key: 'uf', header: 'UF', render: (row) => row.uf },
    {
      key: 'nome_canonico',
      header: 'Canônico',
      render: (row) => row.nome_canonico ?? '—',
    },
    {
      key: 'municipio_id',
      header: 'IBGE',
      render: (row) => row.municipio_id ?? '—',
    },
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
      key: 'metodo',
      header: 'Método',
      render: (row) => <MetodoBadge metodo={row.metodo} />,
    },
  ];

  const exportar = () => {
    if (!data) return;
    const csv = toCSV(
      data.itens.map((i) => ({
        cidade_entrada: i.cidade_entrada,
        uf: i.uf,
        nome_canonico: i.nome_canonico ?? '',
        municipio_id: i.municipio_id ?? '',
        lat: i.lat ?? '',
        lon: i.lon ?? '',
        metodo: i.metodo,
      })),
      [
        { key: 'cidade_entrada', header: 'cidade_entrada' },
        { key: 'uf', header: 'uf' },
        { key: 'nome_canonico', header: 'nome_canonico' },
        { key: 'municipio_id', header: 'municipio_id' },
        { key: 'lat', header: 'lat' },
        { key: 'lon', header: 'lon' },
        { key: 'metodo', header: 'metodo' },
      ],
    );
    downloadCSV('geocodificacao.csv', csv);
  };

  return (
    <>
      <Header
        title="Geocodificação"
        breadcrumbs={[{ label: 'Geocodificação' }]}
        description="Resolve pares CIDADE/UF para código IBGE + centroide (lat/lon)."
      />
      <div className="flex flex-col gap-6 p-8">
        <Card
          title="Entrada"
          description="Uma linha por localização no formato CIDADE/UF. Linhas sem '/' são ignoradas."
        >
          <div className="flex flex-col gap-3">
            <Textarea
              rows={8}
              value={texto}
              onChange={(e) => setTexto(e.target.value)}
              placeholder={PLACEHOLDER}
              aria-label="Localizações CIDADE/UF"
            />
            {erro ? <ErrorState error={new Error(erro)} /> : null}
            {geocodificar.error ? (
              <ErrorState error={geocodificar.error} title="Falha ao geocodificar" />
            ) : null}
            <div className="flex justify-end">
              <Button onClick={submit} loading={geocodificar.isPending}>
                <MapPin className="h-4 w-4" aria-hidden />
                Geocodificar
              </Button>
            </div>
          </div>
        </Card>

        {data ? (
          <Card
            title="Resultado"
            action={
              <Button size="sm" variant="secondary" onClick={exportar}>
                <Download className="h-4 w-4" aria-hidden />
                Exportar CSV
              </Button>
            }
          >
            <div className="flex flex-col gap-4">
              <Alert tone={data.nao_encontrados === 0 ? 'success' : 'warning'}>
                {formatNumber(data.encontrados)} encontrado
                {data.encontrados === 1 ? '' : 's'} ·{' '}
                {formatNumber(data.nao_encontrados)} não encontrado
                {data.nao_encontrados === 1 ? '' : 's'} em {formatNumber(data.total)} entrada
                {data.total === 1 ? '' : 's'}.
              </Alert>
              <Table
                columns={columns}
                data={data.itens}
                rowKey={(row) => `${row.cidade_entrada}-${row.uf}-${row.municipio_id ?? ''}`}
                emptyTitle="Nenhum resultado"
              />
            </div>
          </Card>
        ) : null}
      </div>
    </>
  );
}

function MetodoBadge({ metodo }: { metodo: string }) {
  const tone =
    metodo === 'nao_encontrado' || metodo === 'api_falhou'
      ? 'red'
      : metodo === 'cache_exato' || metodo === 'ibge'
        ? 'green'
        : 'blue';
  return <Badge tone={tone}>{metodo}</Badge>;
}
