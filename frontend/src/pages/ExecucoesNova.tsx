import { useState, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import type { CriarExecucaoRequest } from '@/api/endpoints/execucoes';
import { Button } from '@/components/Button';
import { Card } from '@/components/Card';
import { ErrorState } from '@/components/ErrorState';
import { Input } from '@/components/Input';
import { Select } from '@/components/Select';
import { Header } from '@/features/layout/Header';
import { useCriarExecucao } from '@/hooks/useExecucoes';

const DEFAULTS = {
  arquivo_nc: '',
  cenario: 'rcp45',
  variavel: 'pr',
  bbox_lat_min: -33.75,
  bbox_lat_max: 5.5,
  bbox_lon_min: -74.0,
  bbox_lon_max: -34.8,
  freq_thr_mm: 20,
  heavy20: 20,
  heavy50: 50,
  p95_wet_thr: 1,
  baseline_inicio: 2026,
  baseline_fim: 2035,
  use_bbox: true,
  use_baseline: true,
};

export default function ExecucoesNovaPage() {
  const navigate = useNavigate();
  const [form, setForm] = useState(DEFAULTS);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const criar = useCriarExecucao();

  const validate = (): boolean => {
    const next: Record<string, string> = {};
    if (!form.arquivo_nc.trim()) next.arquivo_nc = 'Campo obrigatório';
    if (!form.cenario.trim()) next.cenario = 'Campo obrigatório';
    if (form.use_bbox) {
      if (form.bbox_lat_min >= form.bbox_lat_max)
        next.bbox_lat_min = 'lat_min deve ser menor que lat_max';
    }
    if (form.use_baseline) {
      if (form.baseline_inicio > form.baseline_fim)
        next.baseline_inicio = 'inicio deve ser ≤ fim';
    }
    setErrors(next);
    return Object.keys(next).length === 0;
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!validate()) return;

    const body: CriarExecucaoRequest = {
      arquivo_nc: form.arquivo_nc.trim(),
      cenario: form.cenario,
      variavel: form.variavel,
      bbox: form.use_bbox
        ? {
            lat_min: form.bbox_lat_min,
            lat_max: form.bbox_lat_max,
            lon_min: form.bbox_lon_min,
            lon_max: form.bbox_lon_max,
          }
        : null,
      parametros_indices: {
        freq_thr_mm: form.freq_thr_mm,
        heavy20: form.heavy20,
        heavy50: form.heavy50,
        p95_wet_thr: form.p95_wet_thr,
        p95_baseline: form.use_baseline
          ? { inicio: form.baseline_inicio, fim: form.baseline_fim }
          : null,
      },
      p95_baseline: form.use_baseline
        ? { inicio: form.baseline_inicio, fim: form.baseline_fim }
        : null,
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
        title="Nova execução"
        breadcrumbs={[{ label: 'Execuções', to: '/execucoes' }, { label: 'Nova' }]}
        description="Enfileira uma execução CORDEX que será processada de forma assíncrona."
      />
      <form onSubmit={handleSubmit} className="flex flex-col gap-6 p-8">
        <Card title="Fonte dos dados">
          <div className="grid gap-4 md:grid-cols-3">
            <div className="md:col-span-3">
              <Input
                label="Arquivo .nc"
                placeholder="/dados/cordex/rcp45/pr_day_BR_2026-2030.nc"
                value={form.arquivo_nc}
                onChange={(e) => update('arquivo_nc', e.target.value)}
                error={errors.arquivo_nc}
                hint="Caminho local acessível pelo backend."
              />
            </div>
            <Select
              label="Cenário"
              value={form.cenario}
              onChange={(e) => update('cenario', e.target.value)}
              options={[
                { value: 'rcp45', label: 'rcp45' },
                { value: 'rcp85', label: 'rcp85' },
              ]}
              error={errors.cenario}
            />
            <Select
              label="Variável"
              value={form.variavel}
              onChange={(e) => update('variavel', e.target.value)}
              options={[{ value: 'pr', label: 'pr' }]}
            />
          </div>
        </Card>

        <Card
          title="Recorte espacial (BBOX)"
          action={
            <label className="flex items-center gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={form.use_bbox}
                onChange={(e) => update('use_bbox', e.target.checked)}
                className="h-4 w-4"
              />
              Aplicar recorte
            </label>
          }
        >
          <div className="grid gap-4 md:grid-cols-4">
            <Input
              label="lat_min"
              type="number"
              step="0.01"
              value={form.bbox_lat_min}
              onChange={(e) => update('bbox_lat_min', Number(e.target.value))}
              disabled={!form.use_bbox}
              error={errors.bbox_lat_min}
            />
            <Input
              label="lat_max"
              type="number"
              step="0.01"
              value={form.bbox_lat_max}
              onChange={(e) => update('bbox_lat_max', Number(e.target.value))}
              disabled={!form.use_bbox}
            />
            <Input
              label="lon_min"
              type="number"
              step="0.01"
              value={form.bbox_lon_min}
              onChange={(e) => update('bbox_lon_min', Number(e.target.value))}
              disabled={!form.use_bbox}
            />
            <Input
              label="lon_max"
              type="number"
              step="0.01"
              value={form.bbox_lon_max}
              onChange={(e) => update('bbox_lon_max', Number(e.target.value))}
              disabled={!form.use_bbox}
            />
          </div>
        </Card>

        <Card title="Parâmetros dos índices">
          <div className="grid gap-4 md:grid-cols-4">
            <Input
              label="freq_thr_mm"
              type="number"
              step="0.1"
              value={form.freq_thr_mm}
              onChange={(e) => update('freq_thr_mm', Number(e.target.value))}
              hint="Limiar T (mm/dia)"
            />
            <Input
              label="heavy20"
              type="number"
              step="0.1"
              value={form.heavy20}
              onChange={(e) => update('heavy20', Number(e.target.value))}
            />
            <Input
              label="heavy50"
              type="number"
              step="0.1"
              value={form.heavy50}
              onChange={(e) => update('heavy50', Number(e.target.value))}
            />
            <Input
              label="p95_wet_thr"
              type="number"
              step="0.1"
              value={form.p95_wet_thr}
              onChange={(e) => update('p95_wet_thr', Number(e.target.value))}
            />
          </div>
        </Card>

        <Card
          title="Baseline do P95"
          action={
            <label className="flex items-center gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={form.use_baseline}
                onChange={(e) => update('use_baseline', e.target.checked)}
                className="h-4 w-4"
              />
              Aplicar baseline
            </label>
          }
        >
          <div className="grid gap-4 md:grid-cols-2">
            <Input
              label="Início"
              type="number"
              min={1850}
              max={2300}
              value={form.baseline_inicio}
              onChange={(e) => update('baseline_inicio', Number(e.target.value))}
              error={errors.baseline_inicio}
              disabled={!form.use_baseline}
            />
            <Input
              label="Fim"
              type="number"
              min={1850}
              max={2300}
              value={form.baseline_fim}
              onChange={(e) => update('baseline_fim', Number(e.target.value))}
              disabled={!form.use_baseline}
            />
          </div>
        </Card>

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
