import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import ResultadosEstresseHidricoPage from './ResultadosEstresseHidrico';

vi.mock('@/api/endpoints/estresseHidrico', async () => {
  const actual =
    await vi.importActual<typeof import('@/api/endpoints/estresseHidrico')>(
      '@/api/endpoints/estresseHidrico',
    );
  return {
    ...actual,
    listarResultadosEstresseHidrico: vi.fn().mockResolvedValue({
      total: 1,
      limit: 25,
      offset: 0,
      items: [
        {
          id: 'reh_01',
          execucao_id: 'exec_01',
          municipio_id: 3550308,
          ano: 2026,
          cenario: 'rcp45',
          frequencia_dias_secos_quentes: 10,
          intensidade_mm: 15.5,
          nome_municipio: 'São Paulo',
          uf: 'SP',
        },
      ],
    }),
  };
});

function LocationSpy({ onChange }: { onChange: (search: string) => void }) {
  const location = useLocation();
  onChange(location.search);
  return null;
}

function renderPage(initialUrl = '/resultados/estresse-hidrico') {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const locationListener = vi.fn<(search: string) => void>();
  render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[initialUrl]}>
        <Routes>
          <Route
            path="/resultados/estresse-hidrico"
            element={
              <>
                <ResultadosEstresseHidricoPage />
                <LocationSpy onChange={locationListener} />
              </>
            }
          />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { locationListener };
}

describe('ResultadosEstresseHidricoPage', () => {
  it('renderiza tabela com os resultados retornados', async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText('São Paulo')).toBeInTheDocument();
    });
    // 'rcp45' aparece na linha da tabela e no select de filtro — buscamos por
    // ocorrência em uma célula <td>.
    const rcpCells = screen.getAllByText('rcp45').filter((el) => el.tagName === 'TD');
    expect(rcpCells.length).toBeGreaterThan(0);
    expect(screen.getByText('10')).toBeInTheDocument();
  });

  it('atualiza a URL quando filtros mudam', async () => {
    const { locationListener } = renderPage();
    fireEvent.change(screen.getByLabelText(/Execução ID/i), {
      target: { value: 'exec_xyz' },
    });
    await waitFor(() => {
      expect(locationListener).toHaveBeenCalledWith('?execucao_id=exec_xyz');
    });
  });

  it('lê filtros do query string', async () => {
    renderPage('/resultados/estresse-hidrico?cenario=rcp85&uf=RJ');
    const cenarioSelect = screen.getByLabelText(/Cenário$/i) as HTMLSelectElement;
    expect(cenarioSelect.value).toBe('rcp85');
    const ufInput = screen.getByLabelText(/^UF$/i) as HTMLInputElement;
    expect(ufInput.value).toBe('RJ');
  });
});
