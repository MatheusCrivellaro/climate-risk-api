import { useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import type { CriarExecucaoEstresseHidricoRequest } from '@/api/endpoints/estresseHidrico';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Input } from '@/components/Input';
import { Select } from '@/components/Select';
import { Header } from '@/features/layout/Header';
import { useCriarExecucaoEstresseHidrico } from '@/hooks/useEstresseHidrico';

type Cenario = 'rcp45' | 'rcp85';

const CENARIO_OPTIONS: { value: Cenario; label: string }[] = [
  { value: 'rcp45', label: 'rcp45' },
  { value: 'rcp85', label: 'rcp85' },
];

const DEFAULTS = {
  arquivo_pr: '',
  arquivo_tas: '',
  arquivo_evap: '',
  cenario: 'rcp45' as Cenario,
  limiar_pr_mm_dia: 1.0,
  limiar_tas_c: 30.0,
};

export default function NovaEstresseHidricoPage() {
  const navigate = useNavigate();
  const [form, setForm] = useState(DEFAULTS);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const criar = useCriarExecucaoEstresseHidrico();

  const validate = (): boolean => {
    const next: Record<string, string> = {};
    if (!form.arquivo_pr.trim()) next.arquivo_pr = 'Campo obrigatório';
    if (!form.arquivo_tas.trim()) next.arquivo_tas = 'Campo obrigatório';
    if (!form.arquivo_evap.trim()) next.arquivo_evap = 'Campo obrigatório';
    if (!CENARIO_OPTIONS.map((o) => o.value).includes(form.cenario)) {
      next.cenario = 'Cenário inválido';
    }
    setErrors(next);
    return Object.keys(next).length === 0;
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!validate()) return;
    const body: CriarExecucaoEstresseHidricoRequest = {
      arquivo_pr: form.arquivo_pr.trim(),
      arquivo_tas: form.arquivo_tas.trim(),
      arquivo_evap: form.arquivo_evap.trim(),
      cenario: form.cenario,
      parametros: {
        limiar_pr_mm_dia: form.limiar_pr_mm_dia,
        limiar_tas_c: form.limiar_tas_c,
      },
    };
    criar.mutate(body, {
      onSuccess: (response) => {
        navigate(`/execucoes/${response.execucao_id}`);
      },
    });
  };

  const update = <K extends keyof typeof form>(key: K, value: (typeof form)[K]) =>
    setForm((prev) => ({ ...prev, [key]: value }));

  return (
    <>
      <Header
        title="Nova execução — estresse hídrico"
        breadcrumbs={[
          { label: 'Execuções', to: '/execucoes' },
          { label: 'Estresse hídrico' },
        ]}
        description="Informe os três arquivos NetCDF (pr, tas, evap) e o cenário."
      />
      <form onSubmit={handleSubmit} className="flex flex-col gap-6 p-8">
        <Card title="Fontes de dados (NetCDF)">
          <div className="grid gap-4">
            <Input
              label="Arquivo de precipitação (pr)"
              placeholder="/dados/cordex/rcp45/pr_day_BR_2026-2035.nc"
              value={form.arquivo_pr}
              onChange={(e) => update('arquivo_pr', e.target.value)}
              error={errors.arquivo_pr}
            />
            <Input
              label="Arquivo de temperatura (tas)"
              placeholder="/dados/cordex/rcp45/tas_day_BR_2026-2035.nc"
              value={form.arquivo_tas}
              onChange={(e) => update('arquivo_tas', e.target.value)}
              error={errors.arquivo_tas}
            />
            <Input
              label="Arquivo de evaporação (evap/evspsbl)"
              placeholder="/dados/cordex/rcp45/evspsbl_day_BR_2026-2035.nc"
              value={form.arquivo_evap}
              onChange={(e) => update('arquivo_evap', e.target.value)}
              error={errors.arquivo_evap}
              hint="Os três arquivos devem cobrir o mesmo cenário."
            />
          </div>
        </Card>

        <Card title="Cenário">
          <Select
            label="Cenário CORDEX"
            value={form.cenario}
            onChange={(e) => update('cenario', e.target.value as Cenario)}
            options={CENARIO_OPTIONS}
            error={errors.cenario}
          />
        </Card>

        <details className="rounded-md border border-slate-200 bg-white p-4">
          <summary className="cursor-pointer text-sm font-medium text-slate-700">
            Parâmetros avançados
          </summary>
          <div className="mt-4 grid gap-4 md:grid-cols-2">
            <Input
              label="Limiar de precipitação (mm/dia)"
              type="number"
              step="0.5"
              min={0}
              value={form.limiar_pr_mm_dia}
              onChange={(e) => update('limiar_pr_mm_dia', Number(e.target.value))}
              hint="Dia 'seco' quando pr <= limiar."
            />
            <Input
              label="Limiar de temperatura (°C)"
              type="number"
              step="1"
              value={form.limiar_tas_c}
              onChange={(e) => update('limiar_tas_c', Number(e.target.value))}
              hint="Dia 'quente' quando tas >= limiar."
            />
          </div>
        </details>

        {criar.error ? <ErrorState error={criar.error} title="Falha ao criar execução" /> : null}

        <div className="flex justify-end gap-3">
          <Button type="button" variant="secondary" onClick={() => navigate('/execucoes')}>
            Cancelar
          </Button>
          <Button type="submit" loading={criar.isPending}>
            Criar execução
          </Button>
        </div>
      </form>
    </>
  );
}
