import type { ReactNode } from 'react';
import { Sidebar } from './Sidebar';

export interface AppLayoutProps {
  children: ReactNode;
}

export function AppLayout({ children }: AppLayoutProps) {
  return (
    <div className="grid min-h-screen grid-cols-[auto_1fr] bg-slate-50">
      <Sidebar />
      <div className="flex min-h-screen flex-col">
        <main className="flex-1">{children}</main>
        <footer className="flex items-center justify-between border-t border-slate-200 bg-white px-8 py-3 text-xs text-slate-500">
          <span>Climate Risk UI · v0.1.0</span>
          <a
            href="/docs"
            target="_blank"
            rel="noreferrer"
            className="text-primary-700 hover:underline"
          >
            OpenAPI docs →
          </a>
        </footer>
      </div>
    </div>
  );
}
