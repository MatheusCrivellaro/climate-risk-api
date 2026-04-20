import type { ReactNode } from 'react';
import { AlertCircle, AlertTriangle, CheckCircle2, Info } from 'lucide-react';
import { cn } from '@/lib/cn';

export type AlertTone = 'info' | 'success' | 'warning' | 'danger';

export interface AlertProps {
  tone?: AlertTone;
  title?: ReactNode;
  children?: ReactNode;
  className?: string;
}

const tones: Record<AlertTone, { box: string; icon: ReactNode }> = {
  info: {
    box: 'border-primary-200 bg-primary-50 text-primary-900',
    icon: <Info className="h-5 w-5 text-primary-600" aria-hidden />,
  },
  success: {
    box: 'border-emerald-200 bg-emerald-50 text-emerald-900',
    icon: <CheckCircle2 className="h-5 w-5 text-emerald-600" aria-hidden />,
  },
  warning: {
    box: 'border-amber-200 bg-amber-50 text-amber-900',
    icon: <AlertTriangle className="h-5 w-5 text-amber-600" aria-hidden />,
  },
  danger: {
    box: 'border-red-200 bg-red-50 text-red-900',
    icon: <AlertCircle className="h-5 w-5 text-red-600" aria-hidden />,
  },
};

export function Alert({ tone = 'info', title, children, className }: AlertProps) {
  const t = tones[tone];
  return (
    <div
      role="alert"
      className={cn('flex items-start gap-3 rounded-md border px-4 py-3 text-sm', t.box, className)}
    >
      <div className="mt-0.5 shrink-0">{t.icon}</div>
      <div className="flex flex-col gap-1">
        {title ? <p className="font-semibold">{title}</p> : null}
        {children ? <div className="text-sm">{children}</div> : null}
      </div>
    </div>
  );
}
