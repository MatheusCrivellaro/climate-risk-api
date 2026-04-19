"""Testes unitários de :class:`ClienteIBGEHttp` (sem rede real).

Usa ``httpx.MockTransport`` monkey-patchado no módulo — pequeno e
determinístico. O teste marcado ``network`` bate na API real do IBGE e
fica desligado por padrão (``-m network`` para rodar).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from climate_risk.domain.excecoes import ErroClienteIBGE
from climate_risk.infrastructure.geocodificacao import cliente_ibge_http as mod
from climate_risk.infrastructure.geocodificacao.cliente_ibge_http import ClienteIBGEHttp

_AsyncClientReal = httpx.AsyncClient


class _AsyncClientFake:
    """Substitui ``httpx.AsyncClient`` para devolver respostas pré-programadas."""

    def __init__(self, transport: httpx.MockTransport, timeout: float) -> None:
        self._cliente = _AsyncClientReal(transport=transport, timeout=timeout)

    async def __aenter__(self) -> httpx.AsyncClient:
        return self._cliente

    async def __aexit__(self, *args: Any) -> None:
        await self._cliente.aclose()


def _instalar_transport(monkeypatch: pytest.MonkeyPatch, handler: httpx.MockTransport) -> None:
    def _fabrica(*, timeout: float) -> _AsyncClientFake:
        return _AsyncClientFake(handler, timeout)

    monkeypatch.setattr(mod.httpx, "AsyncClient", _fabrica)


def _registro_ibge(id_: int, nome: str, uf: str) -> dict[str, Any]:
    return {
        "id": id_,
        "nome": nome,
        "microrregiao": {"mesorregiao": {"UF": {"sigla": uf}}},
    }


@pytest.mark.asyncio
async def test_listar_municipios_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = [
        _registro_ibge(3550308, "São Paulo", "SP"),
        _registro_ibge(3304557, "Rio de Janeiro", "RJ"),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/localidades/municipios"
        return httpx.Response(200, json=payload)

    _instalar_transport(monkeypatch, httpx.MockTransport(handler))
    cliente = ClienteIBGEHttp(base_url="https://ibge.test", max_retries=1)
    municipios = await cliente.listar_municipios()
    assert [(m.id, m.nome, m.uf) for m in municipios] == [
        (3550308, "São Paulo", "SP"),
        (3304557, "Rio de Janeiro", "RJ"),
    ]


@pytest.mark.asyncio
async def test_obter_geometria_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    geojson = {"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}}

    def handler(request: httpx.Request) -> httpx.Response:
        assert "/api/v3/malhas/municipios/3550308" in str(request.url)
        return httpx.Response(200, json=geojson)

    _instalar_transport(monkeypatch, httpx.MockTransport(handler))
    cliente = ClienteIBGEHttp(base_url="https://ibge.test", max_retries=1)
    res = await cliente.obter_geometria_municipio(3550308)
    assert res == geojson


@pytest.mark.asyncio
async def test_erro_http_persistente_levanta_erro_cliente(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="internal")

    _instalar_transport(monkeypatch, httpx.MockTransport(handler))

    async def _sem_espera(_: float) -> None:
        return None

    monkeypatch.setattr(mod.asyncio, "sleep", _sem_espera)

    cliente = ClienteIBGEHttp(base_url="https://ibge.test", max_retries=2)
    with pytest.raises(ErroClienteIBGE) as exc:
        await cliente.listar_municipios()
    assert "localidades/municipios" in exc.value.endpoint


@pytest.mark.asyncio
async def test_retry_recupera_depois_de_uma_falha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tentativas: Iterator[httpx.Response] = iter(
        [
            httpx.Response(503, text="tente novamente"),
            httpx.Response(200, json=[_registro_ibge(3550308, "São Paulo", "SP")]),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return next(tentativas)

    _instalar_transport(monkeypatch, httpx.MockTransport(handler))

    async def _sem_espera(_: float) -> None:
        return None

    monkeypatch.setattr(mod.asyncio, "sleep", _sem_espera)

    cliente = ClienteIBGEHttp(base_url="https://ibge.test", max_retries=3)
    municipios = await cliente.listar_municipios()
    assert len(municipios) == 1


@pytest.mark.asyncio
async def test_payload_lista_esperada_com_tipo_errado(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=json.dumps({"nao": "lista"}))

    _instalar_transport(monkeypatch, httpx.MockTransport(handler))
    cliente = ClienteIBGEHttp(base_url="https://ibge.test", max_retries=1)
    with pytest.raises(ErroClienteIBGE):
        await cliente.listar_municipios()


@pytest.mark.asyncio
async def test_registro_mal_formado_levanta_erro(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"id": 1, "nome": "X"}])  # sem microrregiao

    _instalar_transport(monkeypatch, httpx.MockTransport(handler))
    cliente = ClienteIBGEHttp(base_url="https://ibge.test", max_retries=1)
    with pytest.raises(ErroClienteIBGE):
        await cliente.listar_municipios()
