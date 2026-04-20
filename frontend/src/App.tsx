import { Route, Routes } from 'react-router-dom';
import { AppLayout } from '@/features/layout/AppLayout';
import CalculosPontosPage from '@/pages/CalculosPontos';
import DashboardPage from '@/pages/Dashboard';
import ExecucaoDetalhePage from '@/pages/ExecucaoDetalhe';
import ExecucoesListPage from '@/pages/ExecucoesList';
import ExecucoesNovaPage from '@/pages/ExecucoesNova';
import JobsListPage from '@/pages/JobsList';
import NotFound from '@/pages/NotFound';
import { Placeholder } from '@/pages/Placeholder';

export default function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/execucoes" element={<ExecucoesListPage />} />
        <Route path="/execucoes/nova" element={<ExecucoesNovaPage />} />
        <Route path="/execucoes/:id" element={<ExecucaoDetalhePage />} />
        <Route path="/jobs" element={<JobsListPage />} />
        <Route path="/calculos/pontos" element={<CalculosPontosPage />} />
        <Route
          path="/resultados"
          element={<Placeholder title="Resultados" breadcrumbs={[{ label: 'Resultados' }]} />}
        />
        <Route
          path="/fornecedores"
          element={<Placeholder title="Fornecedores" breadcrumbs={[{ label: 'Fornecedores' }]} />}
        />
        <Route
          path="/geocodificacao"
          element={<Placeholder title="Geocodificação" breadcrumbs={[{ label: 'Geocodificação' }]} />}
        />
        <Route
          path="/cobertura"
          element={<Placeholder title="Cobertura" breadcrumbs={[{ label: 'Cobertura' }]} />}
        />
        <Route path="/admin" element={<Placeholder title="Admin" breadcrumbs={[{ label: 'Admin' }]} />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </AppLayout>
  );
}
