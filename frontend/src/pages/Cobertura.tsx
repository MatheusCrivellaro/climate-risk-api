import { Download, Radar } from 'lucide-react';
import { useMemo, useState } from 'react';
import type { FornecedorCoberturaResponse } from '@/api/endpoints/cobertura';
import { Alert } from '@/components/Alert';
import { Badge } from '@/components/Badge';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Table, type TableColumn } from '@/components/Table';
import { Textarea } from '@/components/Textarea';
import { Header } from '@/features/layout/Header';
import { useConsultarCobertura } from '@/hooks/useCobertura';
import { downloadCSV, toCSV } from '@/lib/csv';
import { formatNumber } from '@/lib/format';

const PLACEHOLDER = `forn-001;São Paulo/SP
forn-002;Rio de Janeiro/RJ
forn-003;Campinas/SP`;

interface EntradaCobertura {
  identificador: string;
  cidade: string;
  uf: string;
}

function parseEntradas(text: string): EntradaCobertura[] {
  const linhas = text.split(/\r?\n/);
  const out: EntradaCobertura[] = [];
  let auto = 1;
  for (const linha of linhas) {
    const bruta = linha.trim();
    if (!bruta || !bruta.includes('/')) continue;
    const [idOuCidadeRaw, restoRaw] = bruta.includes(';')
      ? bruta.split(/;(.+)/)
      : [null, bruta];
    const cidadeUfRaw = restoRaw ?? idOuCidadeRaw;
    if (!cidadeUfRaw || !cidadeUfRaw.includes('/')) continue;
    const [cidadeRaw, ufRaw] = cidadeUfRaw.split('/');
    const cidade = (cidadeRaw ?? '').trim();
    const uf = (ufRaw ?? '').trim().toUpperCase();
    if (!cidade || uf.length !== 2) continue;
    const identificador = (idOuCidadeRaw ?? '').trim() || `linha-${auto}`;
    out.push({ identificador, cidade, uf });
    auto += 1;
  }
  return out;
}

export default function CoberturaPage() {
  const [texto, setTexto] = useState('');
  const [erroLocal, setErroLocal] = useState<string | null>(null);
  const consultar = useConsultarCobertura();

  const preview = useMemo(() => parseEntradas(texto), [texto]);

  const submit = () => {
    setErroLocal(null);
    if (preview.length === 0) {
      setErroLocal(
        'Informe ao menos uma linha no formato IDENTIFICADOR;CIDADE/UF (identificador opcional).',
      );
      return;
    }
    consultar.mutate({ fornecedores: preview });
  };

  const data = consultar.data;

  const columns: TableColumn<FornecedorCoberturaResponse>[] = [
    { key: 'identificador', header: 'ID', render: (row) => row.identificador },
    { key: 'cidade', header: 'Cidade', render: (row) => row.cidade_entrada },
    { key: 'uf', header: 'UF', render: (row) => row.uf_entrada },
    {
      key: 'tem_cobertura',
      header: 'Cobertura',
      render: (row) =>
        row.tem_cobertura ? (
          <Badge tone="green">sim</Badge>
        ) : (
          <Badge tone="red">não</Badge>
        ),
    },
    {
      key: 'municipio_id',
      header: 'IBGE',
      render: (row) => row.municipio_id ?? '—',
    },
    {
      key: 'nome_canonico',
      header: 'Canônico',
      render: (row) => row.nome_canonico ?? '—',
    },
    {
      key: 'motivo',
      header: 'Motivo',
      render: (row) =>
        row.motivo_nao_encontrado ? (
          <code className="font-mono text-xs text-slate-600">
            {row.motivo_nao_encontrado}
          </code>
        ) : (
          '—'
        ),
    },
  ];

  const exportar = () => {
    if (!data) return;
    const csv = toCSV(
      data.itens.map((i) => ({
        identificador: i.identificador,
        cidade_entrada: i.cidade_entrada,
        uf_entrada: i.uf_entrada,
        tem_cobertura: i.tem_cobertura,
        municipio_id: i.municipio_id ?? '',
        nome_canonico: i.nome_canonico ?? '',
        motivo_nao_encontrado: i.motivo_nao_encontrado ?? '',
      })),
      [
        { key: 'identificador', header: 'identificador' },
        { key: 'cidade_entrada', header: 'cidade_entrada' },
        { key: 'uf_entrada', header: 'uf_entrada' },
        { key: 'tem_cobertura', header: 'tem_cobertura' },
        { key: 'municipio_id', header: 'municipio_id' },
        { key: 'nome_canonico', header: 'nome_canonico' },
        { key: 'motivo_nao_encontrado', header: 'motivo_nao_encontrado' },
      ],
    );
    downloadCSV('cobertura.csv', csv);
  };

  return (
    <>
      <Header
        title="Cobertura"
        breadcrumbs={[{ label: 'Cobertura' }]}
        description="Verifica se cada fornecedor tem resultados climáticos disponíveis."
      />
      <div className="flex flex-col gap-6 p-8">
        <Card
          title="Entrada"
          description="Uma linha por fornecedor no formato IDENTIFICADOR;CIDADE/UF. Sem identificador usa-se 'linha-N'."
        >
          <div className="flex flex-col gap-3">
            <Textarea
              rows={8}
              value={texto}
              onChange={(e) => setTexto(e.target.value)}
              placeholder={PLACEHOLDER}
              aria-label="Fornecedores CIDADE/UF"
            />
            <p className="text-xs text-slate-500">
              {formatNumber(preview.length)} entrada{preview.length === 1 ? '' : 's'} válida
              {preview.length === 1 ? '' : 's'} detectada{preview.length === 1 ? '' : 's'}.
            </p>
            {erroLocal ? <ErrorState error={new Error(erroLocal)} /> : null}
            {consultar.error ? (
              <ErrorState error={consultar.error} title="Falha ao consultar cobertura" />
            ) : null}
            <div className="flex justify-end">
              <Button onClick={submit} loading={consultar.isPending}>
                <Radar className="h-4 w-4" aria-hidden />
                Consultar
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
              <Alert tone={data.sem_cobertura === 0 ? 'success' : 'warning'}>
                {formatNumber(data.com_cobertura)} com cobertura ·{' '}
                {formatNumber(data.sem_cobertura)} sem cobertura em{' '}
                {formatNumber(data.total)} fornecedor{data.total === 1 ? '' : 'es'}.
              </Alert>
              <Table
                columns={columns}
                data={data.itens}
                rowKey={(row) => row.identificador}
                emptyTitle="Nenhum resultado"
              />
            </div>
          </Card>
        ) : null}
      </div>
    </>
  );
}
