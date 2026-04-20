import { cn } from '@/lib/cn';

export interface SpinnerProps {
  size?: 'sm' | 'md' | 'lg';
  className?: string;
  label?: string;
}

const sizes = { sm: 'h-4 w-4', md: 'h-6 w-6', lg: 'h-10 w-10' };

export function Spinner({ size = 'md', className, label = 'Carregando…' }: SpinnerProps) {
  return (
    <div role="status" className={cn('inline-flex items-center gap-2 text-slate-500', className)}>
      <span
        aria-hidden
        className={cn(
          'animate-spin rounded-full border-2 border-slate-300 border-t-primary-600',
          sizes[size],
        )}
      />
      <span className="sr-only">{label}</span>
    </div>
  );
}
