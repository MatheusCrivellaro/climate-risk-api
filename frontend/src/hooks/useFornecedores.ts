import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  criarFornecedor,
  deletarFornecedor,
  importarFornecedores,
  listarFornecedores,
  type FornecedorRequest,
  type FornecedorResponse,
  type ListarFornecedoresParams,
  type PaginaFornecedoresResponse,
  type ResultadoImportacaoResponse,
} from '@/api/endpoints/fornecedores';

export function useListarFornecedores(params: ListarFornecedoresParams = {}) {
  return useQuery<PaginaFornecedoresResponse>({
    queryKey: ['fornecedores', 'lista', params],
    queryFn: ({ signal }) => listarFornecedores(params, signal),
  });
}

export function useCriarFornecedor() {
  const qc = useQueryClient();
  return useMutation<FornecedorResponse, Error, FornecedorRequest>({
    mutationFn: criarFornecedor,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['fornecedores', 'lista'] });
    },
  });
}

export function useDeletarFornecedor() {
  const qc = useQueryClient();
  return useMutation<void, Error, string>({
    mutationFn: deletarFornecedor,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['fornecedores', 'lista'] });
    },
  });
}

export function useImportarFornecedores() {
  const qc = useQueryClient();
  return useMutation<ResultadoImportacaoResponse, Error, File>({
    mutationFn: importarFornecedores,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['fornecedores', 'lista'] });
    },
  });
}
