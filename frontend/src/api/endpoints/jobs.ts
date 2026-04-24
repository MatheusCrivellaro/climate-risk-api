import { request } from '../client';
import type { components } from '../schema';

export type JobResponse = components['schemas']['JobResponse'];
export type ListaJobsResponse = components['schemas']['ListaJobsResponse'];

export interface ListarJobsParams {
  status?: string;
  tipo?: string;
  limit?: number;
  offset?: number;
}

export function listarJobs(
  params: ListarJobsParams = {},
  signal?: AbortSignal,
): Promise<ListaJobsResponse> {
  return request<ListaJobsResponse>('/jobs', { query: params, signal });
}

export function obterJob(jobId: string, signal?: AbortSignal): Promise<JobResponse> {
  return request<JobResponse>(`/jobs/${encodeURIComponent(jobId)}`, { signal });
}

export function reprocessarJob(jobId: string): Promise<JobResponse> {
  return request<JobResponse>(`/jobs/${encodeURIComponent(jobId)}/retry`, { method: 'POST' });
}
