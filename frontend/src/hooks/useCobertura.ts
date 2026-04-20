import { useMutation } from '@tanstack/react-query';
import {
  consultarCobertura,
  type CoberturaRequest,
  type CoberturaResponse,
} from '@/api/endpoints/cobertura';

export function useConsultarCobertura() {
  return useMutation<CoberturaResponse, Error, CoberturaRequest>({
    mutationFn: consultarCobertura,
  });
}
