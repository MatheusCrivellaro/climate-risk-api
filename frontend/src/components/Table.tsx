import type { ReactNode } from 'react';
import { cn } from '@/lib/cn';
import { Button } from './Button';
import { EmptyState } from './EmptyState';
import { Spinner } from './Spinner';

export interface TableColumn<T> {
  key: string;
  header: ReactNode;
  render: (row: T) => ReactNode;
  className?: string;
  headerClassName?: string;
}

export interface TablePagination {
  total: number;
  limit: number;
  offset: number;
  onChange: (offset: number) => void;
}

export interface TableProps<T> {
  columns: TableColumn<T>[];
  data: T[] | undefined;
  rowKey: (row: T) => string;
  loading?: boolean;
  emptyTitle?: string;
  emptyDescription?: ReactNode;
  emptyAction?: ReactNode;
  pagination?: TablePagination;
  onRowClick?: (row: T) => void;
}

export function Table<T>({
  columns,
  data,
  rowKey,
  loading,
  emptyTitle = 'Sem registros',
  emptyDescription,
  emptyAction,
  pagination,
  onRowClick,
}: TableProps<T>) {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Spinner />
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <EmptyState title={emptyTitle} description={emptyDescription} action={emptyAction} />
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="overflow-x-auto rounded-md border border-slate-200">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead className="bg-slate-50 text-left text-xs font-semibold uppercase tracking-wide text-slate-600">
            <tr>
              {columns.map((col) => (
                <th
                  key={col.key}
                  scope="col"
                  className={cn('px-4 py-2', col.headerClassName)}
                >
                  {col.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100 bg-white">
            {data.map((row) => (
              <tr
                key={rowKey(row)}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                className={cn(onRowClick && 'cursor-pointer hover:bg-slate-50')}
              >
                {columns.map((col) => (
                  <td key={col.key} className={cn('px-4 py-2 align-middle', col.className)}>
                    {col.render(row)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {pagination ? <TablePaginator {...pagination} /> : null}
    </div>
  );
}

function TablePaginator({ total, limit, offset, onChange }: TablePagination) {
  const start = total === 0 ? 0 : offset + 1;
  const end = Math.min(offset + limit, total);
  const canPrev = offset > 0;
  const canNext = offset + limit < total;
  return (
    <div className="flex items-center justify-between text-sm text-slate-600">
      <span>
        {start}–{end} de {total}
      </span>
      <div className="flex gap-2">
        <Button
          size="sm"
          variant="secondary"
          disabled={!canPrev}
          onClick={() => onChange(Math.max(0, offset - limit))}
        >
          Anterior
        </Button>
        <Button
          size="sm"
          variant="secondary"
          disabled={!canNext}
          onClick={() => onChange(offset + limit)}
        >
          Próxima
        </Button>
      </div>
    </div>
  );
}
