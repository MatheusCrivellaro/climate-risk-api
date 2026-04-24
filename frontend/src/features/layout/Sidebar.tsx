import { CloudRain } from 'lucide-react';
import { NavLink } from 'react-router-dom';
import { cn } from '@/lib/cn';
import { NAVIGATION, NAVIGATION_EXTERNA } from './navigation';

export function Sidebar() {
  return (
    <aside className="flex h-screen w-60 shrink-0 flex-col border-r border-slate-200 bg-white">
      <div className="flex items-center gap-2 border-b border-slate-200 px-5 py-4">
        <CloudRain className="h-6 w-6 text-primary-600" aria-hidden />
        <div className="leading-tight">
          <p className="text-sm font-semibold text-slate-900">Climate Risk</p>
          <p className="text-xs text-slate-500">Painel operacional</p>
        </div>
      </div>
      <nav className="flex-1 overflow-y-auto px-3 py-4">
        <ul className="flex flex-col gap-1">
          {NAVIGATION.map((item) => {
            const Icon = item.icon;
            return (
              <li key={item.to}>
                <NavLink
                  to={item.to}
                  end={item.end}
                  className={({ isActive }) =>
                    cn(
                      'flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                      isActive
                        ? 'bg-primary-50 text-primary-700'
                        : 'text-slate-700 hover:bg-slate-100',
                    )
                  }
                >
                  <Icon className="h-4 w-4" aria-hidden />
                  {item.label}
                </NavLink>
              </li>
            );
          })}
        </ul>
        {NAVIGATION_EXTERNA.length > 0 ? (
          <ul className="mt-4 flex flex-col gap-1 border-t border-slate-200 pt-4">
            {NAVIGATION_EXTERNA.map((item) => {
              const Icon = item.icon;
              return (
                <li key={item.to}>
                  <a
                    href={item.to}
                    target="_blank"
                    rel="noreferrer"
                    className={cn(
                      'flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                      'text-slate-500 hover:bg-slate-100 hover:text-slate-700',
                    )}
                  >
                    <Icon className="h-4 w-4" aria-hidden />
                    {item.label}
                  </a>
                </li>
              );
            })}
          </ul>
        ) : null}
      </nav>
      <footer className="border-t border-slate-200 px-5 py-3 text-xs text-slate-500">
        Climate Risk UI
      </footer>
    </aside>
  );
}
