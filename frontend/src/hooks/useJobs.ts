import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  listarJobs,
  obterJob,
  reprocessarJob,
  type JobResponse,
  type ListaJobsResponse,
  type ListarJobsParams,
} from '@/api/endpoints/jobs';

const STATUSES_FINAIS = new Set(['completed', 'failed', 'canceled']);

export function useListarJobs(params: ListarJobsParams = {}) {
  return useQuery<ListaJobsResponse>({
    queryKey: ['jobs', 'lista', params],
    queryFn: ({ signal }) => listarJobs(params, signal),
  });
}

export function useObterJob(id: string | undefined) {
  return useQuery<JobResponse>({
    queryKey: ['jobs', 'detalhe', id],
    queryFn: ({ signal }) => obterJob(id as string, signal),
    enabled: Boolean(id),
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return false;
      return STATUSES_FINAIS.has(data.status) ? false : 2000;
    },
  });
}

export function useReprocessarJob() {
  const qc = useQueryClient();
  return useMutation<JobResponse, Error, string>({
    mutationFn: reprocessarJob,
    onSuccess: (job) => {
      void qc.invalidateQueries({ queryKey: ['jobs', 'lista'] });
      void qc.invalidateQueries({ queryKey: ['jobs', 'detalhe', job.id] });
    },
  });
}
