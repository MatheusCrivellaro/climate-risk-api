import { request } from '../client';
import type { components } from '../schema';

export type GeocodificarRequest = components['schemas']['GeocodificarRequest'];
export type GeocodificarResponse = components['schemas']['GeocodificarResponse'];
export type LocalizarPontosRequest = components['schemas']['LocalizarPontosRequest'];
export type LocalizarPontosResponse = components['schemas']['LocalizarPontosResponse'];
export type EntradaLocalizacaoSchema = components['schemas']['EntradaLocalizacaoSchema'];

export function geocodificar(body: GeocodificarRequest): Promise<GeocodificarResponse> {
  return request<GeocodificarResponse>('/localizacoes/geocodificar', { method: 'POST', body });
}

export function localizarPontos(body: LocalizarPontosRequest): Promise<LocalizarPontosResponse> {
  return request<LocalizarPontosResponse>('/localizacoes/localizar', { method: 'POST', body });
}
