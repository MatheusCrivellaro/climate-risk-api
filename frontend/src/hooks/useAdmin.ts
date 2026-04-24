import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  atualizarIbge,
  obterAdminStats,
  type AdminStatsResponse,
  type RefreshIBGEResponse,
} from '@/api/endpoints/admin';

export function useAdminStats() {
  return useQuery<AdminStatsResponse>({
    queryKey: ['admin', 'stats'],
    queryFn: ({ signal }) => obterAdminStats(signal),
  });
}

export function useAtualizarIbge() {
  const qc = useQueryClient();
  return useMutation<RefreshIBGEResponse, Error, void>({
    mutationFn: atualizarIbge,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['admin', 'stats'] });
    },
  });
}
