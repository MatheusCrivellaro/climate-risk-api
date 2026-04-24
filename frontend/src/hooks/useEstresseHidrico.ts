import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  criarExecucaoEstresseHidrico,
  listarResultadosEstresseHidrico,
  type CriarExecucaoEstresseHidricoRequest,
  type CriarExecucaoEstresseHidricoResponse,
  type ListarResultadosEstresseHidricoParams,
  type ListarResultadosEstresseHidricoResponse,
} from '@/api/endpoints/estresseHidrico';

export function useCriarExecucaoEstresseHidrico() {
  const qc = useQueryClient();
  return useMutation<
    CriarExecucaoEstresseHidricoResponse,
    Error,
    CriarExecucaoEstresseHidricoRequest
  >({
    mutationFn: criarExecucaoEstresseHidrico,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['execucoes', 'lista'] });
    },
  });
}

export function useListarResultadosEstresseHidrico(
  params: ListarResultadosEstresseHidricoParams,
  enabled = true,
) {
  return useQuery<ListarResultadosEstresseHidricoResponse>({
    queryKey: ['estresse-hidrico', 'resultados', params],
    queryFn: ({ signal }) => listarResultadosEstresseHidrico(params, signal),
    enabled,
  });
}
