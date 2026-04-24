import type { ComponentType, SVGProps } from 'react';
import { cn } from '@/lib/cn';

export interface MetricCardProps {
  label: string;
  value: number | string;
  icon?: ComponentType<SVGProps<SVGSVGElement>>;
  accent?: 'primary' | 'amber' | 'green' | 'red';
  helper?: string;
}

const accents = {
  primary: 'text-primary-700 bg-primary-50',
  amber: 'text-amber-700 bg-amber-50',
  green: 'text-emerald-700 bg-emerald-50',
  red: 'text-red-700 bg-red-50',
};

export function MetricCard({ label, value, icon: Icon, accent = 'primary', helper }: MetricCardProps) {
  return (
    <div className="flex items-start justify-between gap-3 rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
      <div className="flex flex-col gap-1">
        <p className="text-sm font-medium text-slate-600">{label}</p>
        <p className="text-2xl font-semibold tracking-tight text-slate-900">{value}</p>
        {helper ? <p className="text-xs text-slate-500">{helper}</p> : null}
      </div>
      {Icon ? (
        <div className={cn('rounded-md p-2', accents[accent])}>
          <Icon className="h-5 w-5" aria-hidden />
        </div>
      ) : null}
    </div>
  );
}
