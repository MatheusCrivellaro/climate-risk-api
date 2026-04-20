import { useMutation } from '@tanstack/react-query';
import {
  calcularPontos,
  type CalculoPontosResponse,
  type CalculoPorPontosRequest,
} from '@/api/endpoints/calculos';

export function useCalcularPontos() {
  return useMutation<CalculoPontosResponse, Error, CalculoPorPontosRequest>({
    mutationFn: calcularPontos,
  });
}
