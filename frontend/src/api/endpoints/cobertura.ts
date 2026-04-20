import { request } from '../client';
import type { components } from '../schema';

export type CoberturaRequest = components['schemas']['CoberturaRequest'];
export type CoberturaResponse = components['schemas']['CoberturaResponse'];
export type FornecedorEntradaRequest = components['schemas']['FornecedorEntradaRequest'];
export type FornecedorCoberturaResponse = components['schemas']['FornecedorCoberturaResponse'];

export function consultarCobertura(body: CoberturaRequest): Promise<CoberturaResponse> {
  return request<CoberturaResponse>('/cobertura/fornecedores', { method: 'POST', body });
}
