import { Route, Routes } from 'react-router-dom';
import { AppLayout } from '@/features/layout/AppLayout';
import CalculosPontosPage from '@/pages/CalculosPontos';
import CoberturaPage from '@/pages/Cobertura';
import DashboardPage from '@/pages/Dashboard';
import ExecucaoDetalhePage from '@/pages/ExecucaoDetalhe';
import ExecucoesListPage from '@/pages/ExecucoesList';
import ExecucoesNovaPage from '@/pages/ExecucoesNova';
import FornecedoresPage from '@/pages/Fornecedores';
import GeocodificacaoPage from '@/pages/Geocodificacao';
import JobsListPage from '@/pages/JobsList';
import NotFound from '@/pages/NotFound';
import { Placeholder } from '@/pages/Placeholder';
import ResultadosPage from '@/pages/Resultados';

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
        <Route path="/resultados" element={<ResultadosPage />} />
        <Route path="/fornecedores" element={<FornecedoresPage />} />
        <Route path="/geocodificacao" element={<GeocodificacaoPage />} />
        <Route path="/cobertura" element={<CoberturaPage />} />
        <Route path="/admin" element={<Placeholder title="Admin" breadcrumbs={[{ label: 'Admin' }]} />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </AppLayout>
  );
}
