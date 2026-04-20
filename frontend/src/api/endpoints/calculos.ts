import { request } from '../client';
import type { components } from '../schema';

export type CalculoPorPontosRequest = components['schemas']['CalculoPorPontosRequest'];
export type CalculoPorPontosResponse = components['schemas']['CalculoPorPontosResponse'];
export type CalculoPontosAsyncResponse = components['schemas']['CalculoPontosAsyncResponse'];

export type CalculoPontosResponse = CalculoPorPontosResponse | CalculoPontosAsyncResponse;

export function calcularPontos(body: CalculoPorPontosRequest): Promise<CalculoPontosResponse> {
  return request<CalculoPontosResponse>('/calculos/pontos', { method: 'POST', body });
}
