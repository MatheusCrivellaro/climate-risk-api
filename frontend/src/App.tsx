import { Route, Routes } from 'react-router-dom';
import { AppLayout } from '@/features/layout/AppLayout';
import AdminPage from '@/pages/Admin';
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
import ResultadosPage from '@/pages/Resultados';
import NovaEstresseHidricoPage from '@/pages/execucoes/NovaEstresseHidrico';
import ResultadosEstresseHidricoPage from '@/pages/resultados/ResultadosEstresseHidrico';

export default function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/execucoes" element={<ExecucoesListPage />} />
        <Route path="/execucoes/nova" element={<ExecucoesNovaPage />} />
        <Route path="/execucoes/estresse-hidrico/nova" element={<NovaEstresseHidricoPage />} />
        <Route path="/execucoes/:id" element={<ExecucaoDetalhePage />} />
        <Route path="/jobs" element={<JobsListPage />} />
        <Route path="/calculos/pontos" element={<CalculosPontosPage />} />
        <Route path="/resultados" element={<ResultadosPage />} />
        <Route path="/resultados/estresse-hidrico" element={<ResultadosEstresseHidricoPage />} />
        <Route path="/fornecedores" element={<FornecedoresPage />} />
        <Route path="/geocodificacao" element={<GeocodificacaoPage />} />
        <Route path="/cobertura" element={<CoberturaPage />} />
        <Route path="/admin" element={<AdminPage />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </AppLayout>
  );
}
