import { Plus, Trash2 } from 'lucide-react';
import { Button } from '@/components/Button';

export interface PontoRow {
  lat: string;
  lon: string;
  identificador: string;
}

export interface PontosEditorProps {
  pontos: PontoRow[];
  onChange: (next: PontoRow[]) => void;
}

function emptyRow(): PontoRow {
  return { lat: '', lon: '', identificador: '' };
}

export function PontosEditor({ pontos, onChange }: PontosEditorProps) {
  const update = (index: number, key: keyof PontoRow, value: string) => {
    const next = pontos.slice();
    const row = next[index];
    if (!row) return;
    next[index] = { ...row, [key]: value };
    onChange(next);
  };

  const add = () => onChange([...pontos, emptyRow()]);
  const remove = (index: number) => {
    const next = pontos.filter((_, i) => i !== index);
    onChange(next.length > 0 ? next : [emptyRow()]);
  };

  return (
    <div className="flex flex-col gap-3">
      <div className="overflow-x-auto rounded-md border border-slate-200">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead className="bg-slate-50 text-left text-xs font-semibold uppercase tracking-wide text-slate-600">
            <tr>
              <th className="px-3 py-2">#</th>
              <th className="px-3 py-2">Latitude</th>
              <th className="px-3 py-2">Longitude</th>
              <th className="px-3 py-2">Identificador</th>
              <th className="px-3 py-2" />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 bg-white">
            {pontos.map((row, index) => (
              <tr key={index}>
                <td className="px-3 py-2 text-xs text-slate-500">{index + 1}</td>
                <td className="px-3 py-2">
                  <input
                    type="number"
                    step="0.0001"
                    value={row.lat}
                    onChange={(e) => update(index, 'lat', e.target.value)}
                    className="h-9 w-32 rounded border border-slate-300 px-2 text-sm"
                    placeholder="-23.55"
                  />
                </td>
                <td className="px-3 py-2">
                  <input
                    type="number"
                    step="0.0001"
                    value={row.lon}
                    onChange={(e) => update(index, 'lon', e.target.value)}
                    className="h-9 w-32 rounded border border-slate-300 px-2 text-sm"
                    placeholder="-46.63"
                  />
                </td>
                <td className="px-3 py-2">
                  <input
                    type="text"
                    value={row.identificador}
                    onChange={(e) => update(index, 'identificador', e.target.value)}
                    className="h-9 w-full rounded border border-slate-300 px-2 text-sm"
                    placeholder="forn-001"
                  />
                </td>
                <td className="px-3 py-2 text-right">
                  <button
                    type="button"
                    onClick={() => remove(index)}
                    className="text-slate-500 hover:text-danger"
                    aria-label="Remover"
                  >
                    <Trash2 className="h-4 w-4" />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div>
        <Button size="sm" variant="secondary" type="button" onClick={add}>
          <Plus className="h-4 w-4" aria-hidden />
          Adicionar ponto
        </Button>
      </div>
    </div>
  );
}

