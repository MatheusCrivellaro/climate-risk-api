import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { ChevronRight } from 'lucide-react';

export interface Breadcrumb {
  label: string;
  to?: string;
}

export interface HeaderProps {
  title: string;
  description?: ReactNode;
  breadcrumbs?: Breadcrumb[];
  actions?: ReactNode;
}

export function Header({ title, description, breadcrumbs, actions }: HeaderProps) {
  return (
    <header className="flex flex-col gap-2 border-b border-slate-200 bg-white px-8 py-5">
      {breadcrumbs && breadcrumbs.length > 0 ? (
        <nav aria-label="Breadcrumb" className="flex items-center gap-1 text-xs text-slate-500">
          {breadcrumbs.map((crumb, index) => {
            const isLast = index === breadcrumbs.length - 1;
            return (
              <span key={`${crumb.label}-${index}`} className="flex items-center gap-1">
                {crumb.to && !isLast ? (
                  <Link to={crumb.to} className="hover:text-primary-700">
                    {crumb.label}
                  </Link>
                ) : (
                  <span className={isLast ? 'text-slate-700' : ''}>{crumb.label}</span>
                )}
                {!isLast ? <ChevronRight className="h-3 w-3" aria-hidden /> : null}
              </span>
            );
          })}
        </nav>
      ) : null}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-slate-900">{title}</h1>
          {description ? <p className="mt-1 text-sm text-slate-600">{description}</p> : null}
        </div>
        {actions ? <div className="flex items-center gap-2">{actions}</div> : null}
      </div>
    </header>
  );
}
