import { request } from '../client';

export interface HealthResponse {
  status: string;
  [key: string]: unknown;
}

export function obterHealth(signal?: AbortSignal): Promise<HealthResponse> {
  return request<HealthResponse>('/health', { signal });
}

export function obterHealthReady(signal?: AbortSignal): Promise<HealthResponse> {
  return request<HealthResponse>('/health/ready', { signal });
}
