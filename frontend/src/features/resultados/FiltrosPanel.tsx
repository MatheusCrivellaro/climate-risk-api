import type { FiltrosResultados } from '@/api/endpoints/resultados';
import { Card } from '@/components/Card';
import { Input } from '@/components/Input';
import { Select } from '@/components/Select';

export interface FiltrosPanelProps {
  value: FiltrosResultados;
  onChange: (next: FiltrosResultados) => void;
}

const CENARIO_OPTIONS = [
  { value: '', label: 'Todos' },
  { value: 'rcp45', label: 'rcp45' },
  { value: 'rcp85', label: 'rcp85' },
];

const VARIAVEL_OPTIONS = [
  { value: '', label: 'Todas' },
  { value: 'pr', label: 'pr' },
];

export function FiltrosPanel({ value, onChange }: FiltrosPanelProps) {
  const update = <K extends keyof FiltrosResultados>(
    key: K,
    newValue: FiltrosResultados[K] | string,
  ) => {
    onChange({
      ...value,
      [key]: newValue === '' ? undefined : newValue,
    });
  };

  const num = (v: string): number | undefined => (v === '' ? undefined : Number(v));

  return (
    <Card title="Filtros" bodyClassName="flex flex-col gap-4">
      <Input
        label="Execução ID"
        value={value.execucao_id ?? ''}
        onChange={(e) => update('execucao_id', e.target.value || undefined)}
        placeholder="exec_..."
      />
      <Select
        label="Cenário"
        value={value.cenario ?? ''}
        onChange={(e) => update('cenario', e.target.value || undefined)}
        options={CENARIO_OPTIONS}
      />
      <Select
        label="Variável"
        value={value.variavel ?? ''}
        onChange={(e) => update('variavel', e.target.value || undefined)}
        options={VARIAVEL_OPTIONS}
      />
      <Input
        label="Índice"
        value={value.nomes_indices ?? ''}
        onChange={(e) => update('nomes_indices', e.target.value || undefined)}
        placeholder="rx1day, wet_days, ..."
        hint="Separe múltiplos por vírgula."
      />
      <div className="grid grid-cols-2 gap-2">
        <Input
          label="Ano mín."
          type="number"
          value={value.ano_min ?? ''}
          onChange={(e) => update('ano_min', num(e.target.value))}
        />
        <Input
          label="Ano máx."
          type="number"
          value={value.ano_max ?? ''}
          onChange={(e) => update('ano_max', num(e.target.value))}
        />
      </div>
      <div className="grid grid-cols-2 gap-2">
        <Input
          label="UF"
          value={value.uf ?? ''}
          onChange={(e) => update('uf', e.target.value.toUpperCase() || undefined)}
          placeholder="SP"
          maxLength={2}
        />
        <Input
          label="Município ID"
          type="number"
          value={value.municipio_id ?? ''}
          onChange={(e) => update('municipio_id', num(e.target.value))}
        />
      </div>
      <details className="rounded-md border border-slate-200 bg-slate-50 p-3">
        <summary className="cursor-pointer text-sm font-medium text-slate-700">
          Filtro espacial
        </summary>
        <div className="mt-3 flex flex-col gap-3">
          <div className="grid grid-cols-2 gap-2">
            <Input
              label="lat_min"
              type="number"
              step="0.01"
              value={value.lat_min ?? ''}
              onChange={(e) => update('lat_min', num(e.target.value))}
            />
            <Input
              label="lat_max"
              type="number"
              step="0.01"
              value={value.lat_max ?? ''}
              onChange={(e) => update('lat_max', num(e.target.value))}
            />
            <Input
              label="lon_min"
              type="number"
              step="0.01"
              value={value.lon_min ?? ''}
              onChange={(e) => update('lon_min', num(e.target.value))}
            />
            <Input
              label="lon_max"
              type="number"
              step="0.01"
              value={value.lon_max ?? ''}
              onChange={(e) => update('lon_max', num(e.target.value))}
            />
          </div>
          <div className="grid grid-cols-3 gap-2">
            <Input
              label="centro_lat"
              type="number"
              step="0.01"
              value={value.centro_lat ?? ''}
              onChange={(e) => update('centro_lat', num(e.target.value))}
            />
            <Input
              label="centro_lon"
              type="number"
              step="0.01"
              value={value.centro_lon ?? ''}
              onChange={(e) => update('centro_lon', num(e.target.value))}
            />
            <Input
              label="raio_km"
              type="number"
              step="1"
              value={value.raio_km ?? ''}
              onChange={(e) => update('raio_km', num(e.target.value))}
            />
          </div>
        </div>
      </details>
    </Card>
  );
}
