import { cn } from '@/lib/cn';

export type BadgeTone = 'slate' | 'blue' | 'amber' | 'green' | 'red';

export interface BadgeProps {
  tone?: BadgeTone;
  children: React.ReactNode;
  className?: string;
}

const tones: Record<BadgeTone, string> = {
  slate: 'bg-slate-100 text-slate-700 border-slate-200',
  blue: 'bg-primary-50 text-primary-700 border-primary-200',
  amber: 'bg-amber-50 text-amber-800 border-amber-200',
  green: 'bg-emerald-50 text-emerald-800 border-emerald-200',
  red: 'bg-red-50 text-red-800 border-red-200',
};

export function Badge({ tone = 'slate', children, className }: BadgeProps) {
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium',
        tones[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

const STATUS_TONE: Record<string, BadgeTone> = {
  pending: 'amber',
  running: 'blue',
  completed: 'green',
  failed: 'red',
  canceled: 'slate',
};

export function StatusBadge({ status }: { status: string }) {
  const tone = STATUS_TONE[status] ?? 'slate';
  return <Badge tone={tone}>{status}</Badge>;
}

const TIPO_EXECUCAO_TONE: Record<string, BadgeTone> = {
  grade_bbox: 'blue',
  pontos: 'blue',
  estresse_hidrico: 'amber',
};

const TIPO_EXECUCAO_LABEL: Record<string, string> = {
  grade_bbox: 'precipitação extrema',
  pontos: 'pontos',
  estresse_hidrico: 'estresse hídrico',
};

export function TipoExecucaoBadge({ tipo }: { tipo: string }) {
  const tone = TIPO_EXECUCAO_TONE[tipo] ?? 'slate';
  const label = TIPO_EXECUCAO_LABEL[tipo] ?? tipo;
  return <Badge tone={tone}>{label}</Badge>;
}
