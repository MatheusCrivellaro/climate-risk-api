import { Link } from 'react-router-dom';
import { Header } from '@/features/layout/Header';

export default function NotFound() {
  return (
    <>
      <Header title="Página não encontrada" breadcrumbs={[{ label: 'Home', to: '/' }]} />
      <div className="p-8">
        <div className="rounded-md border border-slate-200 bg-white p-10 text-center">
          <p className="text-sm text-slate-600">A rota que você tentou acessar não existe.</p>
          <Link to="/" className="mt-4 inline-block text-sm font-medium text-primary-700">
            Voltar para o dashboard
          </Link>
        </div>
      </div>
    </>
  );
}
