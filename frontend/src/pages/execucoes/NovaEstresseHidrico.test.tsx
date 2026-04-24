import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import NovaEstresseHidricoPage from './NovaEstresseHidrico';

vi.mock('@/api/endpoints/estresseHidrico', async () => {
  const actual =
    await vi.importActual<typeof import('@/api/endpoints/estresseHidrico')>(
      '@/api/endpoints/estresseHidrico',
    );
  return {
    ...actual,
    criarExecucaoEstresseHidrico: vi.fn().mockResolvedValue({
      execucao_id: 'exec_01',
      job_id: 'job_01',
      status: 'pending',
      criado_em: '2026-04-24T10:00:00+00:00',
      links: { self: '/api/execucoes/exec_01', job: '/api/jobs/job_01' },
    }),
  };
});

function renderPage() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={['/execucoes/estresse-hidrico/nova']}>
        <Routes>
          <Route
            path="/execucoes/estresse-hidrico/nova"
            element={<NovaEstresseHidricoPage />}
          />
          <Route path="/execucoes/:id" element={<div>detalhe</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('NovaEstresseHidricoPage', () => {
  it('renderiza formulário com três inputs de arquivo e cenário', () => {
    renderPage();
    expect(screen.getByLabelText(/precipitação \(pr\)/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/temperatura \(tas\)/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/evaporação/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Cenário CORDEX/i)).toBeInTheDocument();
  });

  it('valida campos vazios antes de submeter', async () => {
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: /Criar execução/i }));
    await waitFor(() => {
      expect(screen.getAllByText(/Campo obrigatório/).length).toBeGreaterThanOrEqual(3);
    });
  });

  it('submete com payload correto e redireciona', async () => {
    const { criarExecucaoEstresseHidrico } = await import('@/api/endpoints/estresseHidrico');
    renderPage();
    fireEvent.change(screen.getByLabelText(/precipitação \(pr\)/i), {
      target: { value: '/tmp/pr.nc' },
    });
    fireEvent.change(screen.getByLabelText(/temperatura \(tas\)/i), {
      target: { value: '/tmp/tas.nc' },
    });
    fireEvent.change(screen.getByLabelText(/evaporação/i), {
      target: { value: '/tmp/evap.nc' },
    });
    fireEvent.click(screen.getByRole('button', { name: /Criar execução/i }));

    await waitFor(() => {
      expect(criarExecucaoEstresseHidrico).toHaveBeenCalled();
    });
    const chamadaArgs = (criarExecucaoEstresseHidrico as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(chamadaArgs?.[0]).toEqual({
      arquivo_pr: '/tmp/pr.nc',
      arquivo_tas: '/tmp/tas.nc',
      arquivo_evap: '/tmp/evap.nc',
      cenario: 'rcp45',
      parametros: { limiar_pr_mm_dia: 1.0, limiar_tas_c: 30.0 },
    });
    await waitFor(() => {
      expect(screen.getByText('detalhe')).toBeInTheDocument();
    });
  });
});
