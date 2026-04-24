import { useQuery } from '@tanstack/react-query';
import {
  listarAgregados,
  listarResultados,
  obterStatsResultados,
  type AgregacaoResponse,
  type EstatisticasResponse,
  type ListarAgregadosParams,
  type ListarResultadosParams,
  type PaginaResultadosResponse,
} from '@/api/endpoints/resultados';

export function useListarResultados(params: ListarResultadosParams, enabled = true) {
  return useQuery<PaginaResultadosResponse>({
    queryKey: ['resultados', 'lista', params],
    queryFn: ({ signal }) => listarResultados(params, signal),
    enabled,
  });
}

export function useListarAgregados(params: ListarAgregadosParams, enabled = true) {
  return useQuery<AgregacaoResponse>({
    queryKey: ['resultados', 'agregados', params],
    queryFn: ({ signal }) => listarAgregados(params, signal),
    enabled,
  });
}

export function useStatsResultados() {
  return useQuery<EstatisticasResponse>({
    queryKey: ['resultados', 'stats'],
    queryFn: ({ signal }) => obterStatsResultados(signal),
  });
}
