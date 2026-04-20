import { Route, Routes } from 'react-router-dom';
import { AppLayout } from '@/features/layout/AppLayout';
import NotFound from '@/pages/NotFound';
import { Placeholder } from '@/pages/Placeholder';

export default function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<Placeholder title="Dashboard" />} />
        <Route
          path="/execucoes"
          element={<Placeholder title="Execuções" breadcrumbs={[{ label: 'Execuções' }]} />}
        />
        <Route
          path="/execucoes/nova"
          element={
            <Placeholder
              title="Nova execução"
              breadcrumbs={[{ label: 'Execuções', to: '/execucoes' }, { label: 'Nova' }]}
            />
          }
        />
        <Route
          path="/execucoes/:id"
          element={
            <Placeholder
              title="Detalhe da execução"
              breadcrumbs={[{ label: 'Execuções', to: '/execucoes' }, { label: 'Detalhe' }]}
            />
          }
        />
        <Route path="/jobs" element={<Placeholder title="Jobs" breadcrumbs={[{ label: 'Jobs' }]} />} />
        <Route
          path="/calculos/pontos"
          element={<Placeholder title="Cálculo por pontos" breadcrumbs={[{ label: 'Cálculos' }]} />}
        />
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
