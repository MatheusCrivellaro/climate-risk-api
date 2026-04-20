import { Plus, Trash2, Upload } from 'lucide-react';
import { useMemo, useRef, useState, type FormEvent } from 'react';
import type {
  FornecedorRequest,
  FornecedorResponse,
  ResultadoImportacaoResponse,
} from '@/api/endpoints/fornecedores';
import { Alert } from '@/components/Alert';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Input } from '@/components/Input';
import { Modal } from '@/components/Modal';
import { Table, type TableColumn } from '@/components/Table';
import { Header } from '@/features/layout/Header';
import {
  useCriarFornecedor,
  useDeletarFornecedor,
  useImportarFornecedores,
  useListarFornecedores,
} from '@/hooks/useFornecedores';
import { formatDateTime, truncateId } from '@/lib/format';

const LIMIT = 20;
const INITIAL_FORM: FornecedorRequest = {
  nome: '',
  cidade: '',
  uf: '',
  identificador_externo: null,
  municipio_id: null,
};

export default function FornecedoresPage() {
  const [filters, setFilters] = useState({ uf: '', cidade: '' });
  const [offset, setOffset] = useState(0);
  const [criarAberto, setCriarAberto] = useState(false);
  const [importarAberto, setImportarAberto] = useState(false);
  const [deletarTarget, setDeletarTarget] = useState<FornecedorResponse | null>(null);

  const params = useMemo(
    () => ({
      limit: LIMIT,
      offset,
      uf: filters.uf || undefined,
      cidade: filters.cidade || undefined,
    }),
    [filters, offset],
  );

  const { data, isLoading, error } = useListarFornecedores(params);
  const criar = useCriarFornecedor();
  const deletar = useDeletarFornecedor();

  const columns: TableColumn<FornecedorResponse>[] = [
    {
      key: 'id',
      header: 'ID',
      render: (row) => (
        <span className="font-mono text-xs" title={row.id}>
          {truncateId(row.id)}
        </span>
      ),
    },
    { key: 'nome', header: 'Nome', render: (row) => row.nome },
    { key: 'cidade', header: 'Cidade', render: (row) => row.cidade },
    { key: 'uf', header: 'UF', render: (row) => row.uf },
    {
      key: 'municipio_id',
      header: 'Município',
      render: (row) => row.municipio_id ?? '—',
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
      render: (row) => (
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            setDeletarTarget(row);
          }}
          className="text-slate-500 hover:text-danger"
          aria-label="Deletar"
        >
          <Trash2 className="h-4 w-4" />
        </button>
      ),
    },
  ];

  return (
    <>
      <Header
        title="Fornecedores"
        breadcrumbs={[{ label: 'Fornecedores' }]}
        actions={
          <div className="flex gap-2">
            <Button variant="secondary" onClick={() => setImportarAberto(true)}>
              <Upload className="h-4 w-4" aria-hidden />
              Importar CSV/XLSX
            </Button>
            <Button onClick={() => setCriarAberto(true)}>
              <Plus className="h-4 w-4" aria-hidden />
              Novo fornecedor
            </Button>
          </div>
        }
      />
      <div className="p-8">
        <Card title="Filtros" bodyClassName="grid gap-4 md:grid-cols-2">
          <Input
            label="UF"
            value={filters.uf}
            onChange={(e) => {
              setOffset(0);
              setFilters((prev) => ({ ...prev, uf: e.target.value.toUpperCase() }));
            }}
            maxLength={2}
            placeholder="SP"
          />
          <Input
            label="Cidade"
            value={filters.cidade}
            onChange={(e) => {
              setOffset(0);
              setFilters((prev) => ({ ...prev, cidade: e.target.value }));
            }}
            placeholder="São Paulo"
          />
        </Card>

        <div className="mt-6">
          {error ? (
            <ErrorState error={error} title="Falha ao listar fornecedores" />
          ) : (
            <Table
              columns={columns}
              data={data?.itens}
              rowKey={(row) => row.id}
              loading={isLoading}
              emptyTitle="Nenhum fornecedor encontrado"
              emptyDescription="Cadastre manualmente ou importe um CSV/XLSX."
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

      <NovoFornecedorModal
        open={criarAberto}
        onClose={() => setCriarAberto(false)}
        onSubmit={(body) =>
          criar.mutate(body, { onSuccess: () => setCriarAberto(false) })
        }
        loading={criar.isPending}
        error={criar.error}
      />

      <ImportarModal
        open={importarAberto}
        onClose={() => {
          setImportarAberto(false);
        }}
      />

      <Modal
        open={deletarTarget !== null}
        onClose={() => setDeletarTarget(null)}
        title="Deletar fornecedor?"
        footer={
          <>
            <Button variant="secondary" onClick={() => setDeletarTarget(null)}>
              Cancelar
            </Button>
            <Button
              variant="danger"
              loading={deletar.isPending}
              onClick={() => {
                if (!deletarTarget) return;
                deletar.mutate(deletarTarget.id, {
                  onSuccess: () => setDeletarTarget(null),
                });
              }}
            >
              Deletar
            </Button>
          </>
        }
      >
        <p className="text-sm text-slate-700">
          <strong>{deletarTarget?.nome}</strong> será removido permanentemente.
        </p>
        {deletar.error ? (
          <div className="mt-4">
            <ErrorState error={deletar.error} title="Falha ao deletar" />
          </div>
        ) : null}
      </Modal>
    </>
  );
}

interface NovoFornecedorModalProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (body: FornecedorRequest) => void;
  loading: boolean;
  error: Error | null;
}

function NovoFornecedorModal({ open, onClose, onSubmit, loading, error }: NovoFornecedorModalProps) {
  const [form, setForm] = useState<FornecedorRequest>(INITIAL_FORM);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const reset = () => {
    setForm(INITIAL_FORM);
    setErrors({});
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const next: Record<string, string> = {};
    if (!form.nome.trim()) next.nome = 'Obrigatório';
    if (!form.cidade.trim()) next.cidade = 'Obrigatório';
    if (!form.uf.trim() || form.uf.trim().length !== 2) next.uf = 'Use 2 letras';
    setErrors(next);
    if (Object.keys(next).length > 0) return;
    onSubmit({
      ...form,
      nome: form.nome.trim(),
      cidade: form.cidade.trim(),
      uf: form.uf.toUpperCase().trim(),
      identificador_externo: form.identificador_externo || null,
      municipio_id: form.municipio_id || null,
    });
  };

  return (
    <Modal
      open={open}
      onClose={handleClose}
      title="Novo fornecedor"
      footer={
        <>
          <Button variant="secondary" onClick={handleClose}>
            Cancelar
          </Button>
          <Button form="novo-fornecedor-form" type="submit" loading={loading}>
            Cadastrar
          </Button>
        </>
      }
    >
      <form id="novo-fornecedor-form" onSubmit={submit} className="flex flex-col gap-4">
        <Input
          label="Nome"
          value={form.nome}
          onChange={(e) => setForm((prev) => ({ ...prev, nome: e.target.value }))}
          error={errors.nome}
        />
        <div className="grid grid-cols-3 gap-3">
          <div className="col-span-2">
            <Input
              label="Cidade"
              value={form.cidade}
              onChange={(e) => setForm((prev) => ({ ...prev, cidade: e.target.value }))}
              error={errors.cidade}
            />
          </div>
          <Input
            label="UF"
            value={form.uf}
            onChange={(e) => setForm((prev) => ({ ...prev, uf: e.target.value.toUpperCase() }))}
            error={errors.uf}
            maxLength={2}
          />
        </div>
        <Input
          label="Identificador externo"
          value={form.identificador_externo ?? ''}
          onChange={(e) =>
            setForm((prev) => ({ ...prev, identificador_externo: e.target.value || null }))
          }
          hint="Opcional."
        />
        <Input
          label="Município ID (IBGE)"
          type="number"
          value={form.municipio_id ?? ''}
          onChange={(e) =>
            setForm((prev) => ({
              ...prev,
              municipio_id: e.target.value ? Number(e.target.value) : null,
            }))
          }
          hint="Opcional; se omitido o backend resolve via geocodificação."
        />
        {error ? <ErrorState error={error} title="Falha ao cadastrar" /> : null}
      </form>
    </Modal>
  );
}

interface ImportarModalProps {
  open: boolean;
  onClose: () => void;
}

function ImportarModal({ open, onClose }: ImportarModalProps) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [result, setResult] = useState<ResultadoImportacaoResponse | null>(null);
  const importar = useImportarFornecedores();

  const handleClose = () => {
    setResult(null);
    importar.reset();
    onClose();
  };

  const submit = () => {
    const file = fileRef.current?.files?.[0];
    if (!file) return;
    importar.mutate(file, {
      onSuccess: (response) => setResult(response),
    });
  };

  return (
    <Modal
      open={open}
      onClose={handleClose}
      title="Importar fornecedores"
      size="lg"
      footer={
        <>
          <Button variant="secondary" onClick={handleClose}>
            Fechar
          </Button>
          <Button loading={importar.isPending} onClick={submit}>
            Importar
          </Button>
        </>
      }
    >
      <div className="flex flex-col gap-4">
        <p className="text-sm text-slate-600">
          O arquivo deve conter as colunas <code>nome, cidade, uf</code> e, opcionalmente,{' '}
          <code>identificador_externo, municipio_id</code>.
        </p>
        <input
          ref={fileRef}
          type="file"
          accept=".csv,.xlsx,.xls"
          className="text-sm"
          aria-label="Arquivo de importação"
        />
        {importar.error ? <ErrorState error={importar.error} title="Falha ao importar" /> : null}
        {result ? (
          <div className="flex flex-col gap-3">
            <Alert tone="success" title="Importação concluída">
              {result.importados} importado{result.importados === 1 ? '' : 's'} ·{' '}
              {result.duplicados} duplicado{result.duplicados === 1 ? '' : 's'} ·{' '}
              {result.erros.length} erro{result.erros.length === 1 ? '' : 's'} em{' '}
              {result.total_linhas} linha{result.total_linhas === 1 ? '' : 's'}.
            </Alert>
            {result.erros.length > 0 ? (
              <div className="rounded-md border border-slate-200">
                <table className="min-w-full divide-y divide-slate-200 text-sm">
                  <thead className="bg-slate-50 text-left text-xs font-semibold uppercase text-slate-600">
                    <tr>
                      <th className="px-4 py-2">Linha</th>
                      <th className="px-4 py-2">Motivo</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {result.erros.map((erro) => (
                      <tr key={erro.linha}>
                        <td className="px-4 py-2 font-mono">{erro.linha}</td>
                        <td className="px-4 py-2">{erro.motivo}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </Modal>
  );
}
