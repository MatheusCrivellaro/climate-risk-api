import { request } from '../client';
import type { components } from '../schema';

export type CriarExecucaoEstresseHidricoRequest =
  components['schemas']['CriarExecucaoEstresseHidricoRequest'];
export type CriarExecucaoEstresseHidricoResponse =
  components['schemas']['CriarExecucaoEstresseHidricoResponse'];
export type ResultadoEstresseHidrico = components['schemas']['ResultadoEstresseHidricoSchema'];
export type ListarResultadosEstresseHidricoResponse =
  components['schemas']['ListarResultadosEstresseHidricoResponse'];

export interface FiltrosEstresseHidrico {
  execucao_id?: string;
  cenario?: string;
  ano?: number;
  ano_min?: number;
  ano_max?: number;
  municipio_id?: number;
  uf?: string;
}

export interface ListarResultadosEstresseHidricoParams extends FiltrosEstresseHidrico {
  limit?: number;
  offset?: number;
}

export function criarExecucaoEstresseHidrico(
  body: CriarExecucaoEstresseHidricoRequest,
): Promise<CriarExecucaoEstresseHidricoResponse> {
  return request<CriarExecucaoEstresseHidricoResponse>('/execucoes/estresse-hidrico', {
    method: 'POST',
    body,
  });
}

export function listarResultadosEstresseHidrico(
  params: ListarResultadosEstresseHidricoParams = {},
  signal?: AbortSignal,
): Promise<ListarResultadosEstresseHidricoResponse> {
  return request<ListarResultadosEstresseHidricoResponse>('/resultados/estresse-hidrico', {
    query: params,
    signal,
  });
}
