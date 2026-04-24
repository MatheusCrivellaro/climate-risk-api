import { request } from '../client';
import type { components } from '../schema';

export type FornecedorRequest = components['schemas']['FornecedorRequest'];
export type FornecedorResponse = components['schemas']['FornecedorResponse'];
export type PaginaFornecedoresResponse = components['schemas']['PaginaFornecedoresResponse'];
export type ResultadoImportacaoResponse = components['schemas']['ResultadoImportacaoResponse'];

export interface ListarFornecedoresParams {
  uf?: string;
  cidade?: string;
  limit?: number;
  offset?: number;
}

export function listarFornecedores(
  params: ListarFornecedoresParams = {},
  signal?: AbortSignal,
): Promise<PaginaFornecedoresResponse> {
  return request<PaginaFornecedoresResponse>('/fornecedores', { query: params, signal });
}

export function obterFornecedor(id: string, signal?: AbortSignal): Promise<FornecedorResponse> {
  return request<FornecedorResponse>(`/fornecedores/${encodeURIComponent(id)}`, { signal });
}

export function criarFornecedor(body: FornecedorRequest): Promise<FornecedorResponse> {
  return request<FornecedorResponse>('/fornecedores', { method: 'POST', body });
}

export function deletarFornecedor(id: string): Promise<void> {
  return request<void>(`/fornecedores/${encodeURIComponent(id)}`, { method: 'DELETE' });
}

export function importarFornecedores(file: File): Promise<ResultadoImportacaoResponse> {
  const form = new FormData();
  form.append('arquivo', file);
  return request<ResultadoImportacaoResponse>('/fornecedores/importar', {
    method: 'POST',
    body: form,
  });
}
