import { useMutation } from '@tanstack/react-query';
import {
  geocodificar,
  localizarPontos,
  type GeocodificarRequest,
  type GeocodificarResponse,
  type LocalizarPontosRequest,
  type LocalizarPontosResponse,
} from '@/api/endpoints/geocoding';

export function useGeocodificar() {
  return useMutation<GeocodificarResponse, Error, GeocodificarRequest>({
    mutationFn: geocodificar,
  });
}

export function useLocalizarPontos() {
  return useMutation<LocalizarPontosResponse, Error, LocalizarPontosRequest>({
    mutationFn: localizarPontos,
  });
}
