import { request } from '../client';
import type { components } from '../schema';

export type ExecucaoResumo = components['schemas']['ExecucaoResumo'];
export type ListaExecucoesResponse = components['schemas']['ListaExecucoesResponse'];
export type CriarExecucaoRequest = components['schemas']['CriarExecucaoRequest'];
export type CriarExecucaoResponse = components['schemas']['CriarExecucaoResponse'];

export interface ListarExecucoesParams {
  cenario?: string;
  variavel?: string;
  status?: string;
  limit?: number;
  offset?: number;
}

export function listarExecucoes(
  params: ListarExecucoesParams = {},
  signal?: AbortSignal,
): Promise<ListaExecucoesResponse> {
  return request<ListaExecucoesResponse>('/execucoes', { query: params, signal });
}

export function obterExecucao(execucaoId: string, signal?: AbortSignal): Promise<ExecucaoResumo> {
  return request<ExecucaoResumo>(`/execucoes/${encodeURIComponent(execucaoId)}`, { signal });
}

export function criarExecucao(body: CriarExecucaoRequest): Promise<CriarExecucaoResponse> {
  return request<CriarExecucaoResponse>('/execucoes', { method: 'POST', body });
}

export function cancelarExecucao(execucaoId: string): Promise<ExecucaoResumo> {
  return request<ExecucaoResumo>(`/execucoes/${encodeURIComponent(execucaoId)}/cancelar`, {
    method: 'POST',
  });
}
