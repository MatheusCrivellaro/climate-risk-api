"""Adaptador HTTP de :class:`ClienteIBGE` usando ``httpx.AsyncClient``.

Traduz qualquer falha de rede ou resposta não-2xx persistente em
:class:`ErroClienteIBGE` — o middleware HTTP cuida de converter para 503.
Usa retry exponencial simples (sem backoff externo — ``httpx`` 0.27 não
tem retry nativo, e adicionar ``tenacity`` só para isso seria excesso).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from climate_risk.core.config import get_settings
from climate_risk.domain.excecoes import ErroClienteIBGE
from climate_risk.domain.portas.cliente_ibge import MunicipioIBGE

logger = logging.getLogger(__name__)

_LISTAR_MUNICIPIOS = "/api/v1/localidades/municipios"
_MALHA_TEMPLATE = "/api/v3/malhas/municipios/{id}?formato=application/vnd.geo+json"


class ClienteIBGEHttp:
    """Cliente HTTP para ``servicodados.ibge.gov.br``.

    Lê ``ibge_base_url``, ``ibge_timeout_segundos`` e ``ibge_max_retries`` das
    :class:`Settings`. O ``httpx.AsyncClient`` é recriado por instância —
    é leve o bastante e evita guardar um cliente global.
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout_segundos: float | None = None,
        max_retries: int | None = None,
    ) -> None:
        settings = get_settings()
        self._base_url = base_url if base_url is not None else settings.ibge_base_url
        self._timeout = (
            timeout_segundos if timeout_segundos is not None else settings.ibge_timeout_segundos
        )
        self._max_retries = max_retries if max_retries is not None else settings.ibge_max_retries

    async def listar_municipios(self) -> list[MunicipioIBGE]:
        dados = await self._get_json(_LISTAR_MUNICIPIOS)
        if not isinstance(dados, list):
            raise ErroClienteIBGE(
                "payload inesperado (esperava lista)",
                endpoint=_LISTAR_MUNICIPIOS,
            )
        return [self._para_municipio(item) for item in dados]

    async def obter_geometria_municipio(self, municipio_id: int) -> dict[str, Any]:
        endpoint = _MALHA_TEMPLATE.format(id=municipio_id)
        dados = await self._get_json(endpoint)
        if not isinstance(dados, dict):
            raise ErroClienteIBGE(
                "payload inesperado (esperava objeto GeoJSON)",
                endpoint=endpoint,
            )
        return dados

    async def _get_json(self, endpoint: str) -> Any:
        url = f"{self._base_url.rstrip('/')}{endpoint}"
        ultimo_erro: Exception | None = None
        for tentativa in range(1, self._max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as cliente:
                    resposta = await cliente.get(url)
                    resposta.raise_for_status()
                    return resposta.json()
            except (httpx.HTTPError, ValueError) as exc:
                ultimo_erro = exc
                if tentativa >= self._max_retries:
                    break
                espera = 2 ** (tentativa - 1)
                logger.warning(
                    "IBGE %s falhou (tentativa %d/%d): %s — retry em %ds",
                    endpoint,
                    tentativa,
                    self._max_retries,
                    exc,
                    espera,
                )
                await asyncio.sleep(espera)
        raise ErroClienteIBGE(str(ultimo_erro), endpoint=endpoint)

    @staticmethod
    def _para_municipio(item: dict[str, Any]) -> MunicipioIBGE:
        """Extrai ``(id, nome, uf)`` do JSON aninhado do IBGE."""
        try:
            municipio_id = int(item["id"])
            nome = str(item["nome"])
            uf = str(item["microrregiao"]["mesorregiao"]["UF"]["sigla"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ErroClienteIBGE(
                f"registro de município mal formado: {exc}",
                endpoint=_LISTAR_MUNICIPIOS,
            ) from exc
        return MunicipioIBGE(id=municipio_id, nome=nome, uf=uf)
