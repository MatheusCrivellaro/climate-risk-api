import { cn } from '@/lib/cn';

export interface CodeBlockProps {
  value: unknown;
  className?: string;
}

export function CodeBlock({ value, className }: CodeBlockProps) {
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  return (
    <pre
      className={cn(
        'overflow-x-auto rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-xs text-slate-800',
        className,
      )}
    >
      {text}
    </pre>
  );
}
