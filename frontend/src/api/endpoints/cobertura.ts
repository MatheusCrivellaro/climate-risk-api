import { request } from '../client';
import type { components } from '../schema';

export type CoberturaRequest = components['schemas']['CoberturaRequest'];
export type CoberturaResponse = components['schemas']['CoberturaResponse'];

export function consultarCobertura(body: CoberturaRequest): Promise<CoberturaResponse> {
  return request<CoberturaResponse>('/cobertura/fornecedores', { method: 'POST', body });
}
