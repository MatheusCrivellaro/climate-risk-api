"""Configuração da aplicação via variáveis de ambiente.

Lê variáveis com prefixo ``CLIMATE_RISK_`` a partir do ambiente ou de um
arquivo ``.env`` na raiz do projeto. Exportar ``get_settings()`` garante
instância única por processo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configurações tipadas da aplicação."""

    model_config = SettingsConfigDict(
        env_prefix="CLIMATE_RISK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "sqlite+aiosqlite:///./climate_risk.db"
    log_level: str = "INFO"
    worker_poll_interval_seconds: int = 2
    worker_heartbeat_seconds: int = 30
    job_timeout_processar_cordex_seconds: int = 7200
    job_timeout_calcular_pontos_seconds: int = 1800
    ibge_base_url: str = "https://servicodados.ibge.gov.br"
    ibge_timeout_segundos: float = 30.0
    ibge_max_retries: int = 3
    shapefile_uf_path: str | None = None
    shapefile_mun_path: str | None = None
    cache_dir: str = "data/caches"
    sincrono_pontos_max: int = 100
    fs_raiz: str | None = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retorna instância cacheada de :class:`Settings`."""
    return Settings()


@dataclass(frozen=True)
class ConfigFS:
    """Configuração do browser de pastas (Slice 20.1).

    ``raiz`` é a única pasta a partir da qual o endpoint ``/api/fs/listar``
    pode navegar. Quando ``None``, o endpoint responde 503.
    """

    raiz: Path | None

    @classmethod
    def from_env(cls) -> ConfigFS:
        """Carrega ``CLIMATE_RISK_FS_RAIZ`` do ambiente.

        Resolve o caminho (segue symlinks) e exige que aponte para um
        diretório existente. Sem a variável, devolve ``raiz=None`` —
        endpoint dependente reagirá com 503.
        """
        valor = os.environ.get("CLIMATE_RISK_FS_RAIZ")
        if not valor:
            return cls(raiz=None)
        raiz = Path(valor).resolve()
        if not raiz.is_dir():
            raise ValueError(f"CLIMATE_RISK_FS_RAIZ aponta para diretório inexistente: {raiz}")
        return cls(raiz=raiz)
