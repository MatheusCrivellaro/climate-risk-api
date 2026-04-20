import { useMutation, useQuery, useQueryClient, type UseQueryOptions } from '@tanstack/react-query';
import {
  cancelarExecucao,
  criarExecucao,
  listarExecucoes,
  obterExecucao,
  type CriarExecucaoRequest,
  type CriarExecucaoResponse,
  type ExecucaoResumo,
  type ListaExecucoesResponse,
  type ListarExecucoesParams,
} from '@/api/endpoints/execucoes';

const STATUSES_FINAIS = new Set(['completed', 'failed', 'canceled']);

export function useListarExecucoes(params: ListarExecucoesParams = {}) {
  return useQuery<ListaExecucoesResponse>({
    queryKey: ['execucoes', 'lista', params],
    queryFn: ({ signal }) => listarExecucoes(params, signal),
  });
}

type ObterExecucaoOptions = Omit<
  UseQueryOptions<ExecucaoResumo>,
  'queryKey' | 'queryFn' | 'enabled'
> & {
  enabled?: boolean;
};

export function useObterExecucao(id: string | undefined, options: ObterExecucaoOptions = {}) {
  return useQuery<ExecucaoResumo>({
    queryKey: ['execucoes', 'detalhe', id],
    queryFn: ({ signal }) => obterExecucao(id as string, signal),
    enabled: Boolean(id) && (options.enabled ?? true),
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return false;
      return STATUSES_FINAIS.has(data.status) ? false : 2000;
    },
    ...options,
  });
}

export function useCriarExecucao() {
  const qc = useQueryClient();
  return useMutation<CriarExecucaoResponse, Error, CriarExecucaoRequest>({
    mutationFn: criarExecucao,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['execucoes', 'lista'] });
    },
  });
}

export function useCancelarExecucao() {
  const qc = useQueryClient();
  return useMutation<ExecucaoResumo, Error, string>({
    mutationFn: cancelarExecucao,
    onSuccess: (execucao) => {
      void qc.invalidateQueries({ queryKey: ['execucoes', 'lista'] });
      void qc.invalidateQueries({ queryKey: ['execucoes', 'detalhe', execucao.id] });
    },
  });
}
