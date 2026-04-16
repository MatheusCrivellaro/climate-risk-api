"""Configuração da aplicação via variáveis de ambiente.

Lê variáveis com prefixo ``CLIMATE_RISK_`` a partir do ambiente ou de um
arquivo ``.env`` na raiz do projeto. Exportar ``get_settings()`` garante
instância única por processo.
"""

from __future__ import annotations

from functools import lru_cache

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
    shapefile_uf_path: str | None = None
    shapefile_mun_path: str | None = None
    sincrono_pontos_max: int = 100


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retorna instância cacheada de :class:`Settings`."""
    return Settings()
