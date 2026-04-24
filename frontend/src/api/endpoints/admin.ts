import { request } from '../client';
import type { components } from '../schema';

export type AdminStatsResponse = components['schemas']['AdminStatsResponse'];
export type RefreshIBGEResponse = components['schemas']['RefreshIBGEResponse'];

export function obterAdminStats(signal?: AbortSignal): Promise<AdminStatsResponse> {
  return request<AdminStatsResponse>('/admin/stats', { signal });
}

export function atualizarIbge(): Promise<RefreshIBGEResponse> {
  return request<RefreshIBGEResponse>('/admin/ibge/refresh', { method: 'POST' });
}
