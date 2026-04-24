import { request } from '../client';
import type { components } from '../schema';

export type ResultadoResponse = components['schemas']['ResultadoResponse'];
export type PaginaResultadosResponse = components['schemas']['PaginaResultadosResponse'];
export type AgregacaoResponse = components['schemas']['AgregacaoResponse'];
export type EstatisticasResponse = components['schemas']['EstatisticasResponse'];

export interface FiltrosResultados {
  execucao_id?: string;
  cenario?: string;
  variavel?: string;
  ano?: number;
  ano_min?: number;
  ano_max?: number;
  nomes_indices?: string;
  lat_min?: number;
  lat_max?: number;
  lon_min?: number;
  lon_max?: number;
  centro_lat?: number;
  centro_lon?: number;
  raio_km?: number;
  uf?: string;
  municipio_id?: number;
}

export interface ListarResultadosParams extends FiltrosResultados {
  limit?: number;
  offset?: number;
}

export type AgregacaoTipo = 'media' | 'min' | 'max' | 'p50' | 'p95';

export interface ListarAgregadosParams extends FiltrosResultados {
  agregacao: AgregacaoTipo;
  agrupar_por?: string;
}

export function listarResultados(
  params: ListarResultadosParams,
  signal?: AbortSignal,
): Promise<PaginaResultadosResponse> {
  return request<PaginaResultadosResponse>('/resultados', { query: params, signal });
}

export function listarAgregados(
  params: ListarAgregadosParams,
  signal?: AbortSignal,
): Promise<AgregacaoResponse> {
  return request<AgregacaoResponse>('/resultados/agregados', { query: params, signal });
}

export function obterStatsResultados(signal?: AbortSignal): Promise<EstatisticasResponse> {
  return request<EstatisticasResponse>('/resultados/stats', { signal });
}
