"""Rotas de health (``GET /health`` e ``GET /health/ready``).

- ``/health`` (liveness): apenas confirma que o processo responde.
- ``/health/ready`` (readiness): verifica que o banco responde a ``SELECT 1``
  e que a migração mais recente (``head``) foi aplicada. Falhas retornam
  ``503`` via :class:`HTTPException` — não dependem do middleware RFC 7807
  porque o endpoint precisa devolver o corpo com o diagnóstico detalhado.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config as AlembicConfig
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from fastapi import APIRouter, HTTPException
from sqlalchemy import Connection, text

from climate_risk.interfaces.dependencias import SessaoDep

router = APIRouter(tags=["admin"])


@router.get("/health", summary="Verificação de liveness.")
async def health() -> dict[str, str]:
    """Retorna ``{"status": "ok"}`` quando o processo responde."""
    return {"status": "ok"}


def _carregar_head_alembic() -> str | None:
    """Lê o ``head`` declarado em ``migrations/versions/*.py`` via Alembic.

    Retorna ``None`` se a configuração não for encontrada — improvável em
    produção, mas permite que o endpoint degrade graciosamente em setups
    atípicos (por exemplo, wheel instalado sem o diretório de migrações).
    """
    raiz = Path(__file__).resolve().parents[4]
    ini = raiz / "alembic.ini"
    if not ini.exists():
        return None
    config = AlembicConfig(str(ini))
    config.set_main_option("script_location", str(raiz / "migrations"))
    script = ScriptDirectory.from_config(config)
    return script.get_current_head()


@router.get(
    "/health/ready",
    summary="Verificação de readiness (banco + migrações).",
)
async def health_ready(sessao: SessaoDep) -> dict[str, object]:
    """Confirma conectividade com o banco e migrações em ``head``.

    Passos:

    1. Executa ``SELECT 1`` — falha aqui significa banco fora do ar.
    2. Lê a revisão atual do schema via :class:`MigrationContext`.
    3. Compara com o ``head`` declarado em ``migrations/versions/``.

    Retorna ``200`` com ``{"status": "ready", ...}`` apenas quando tudo bate;
    em qualquer outra situação, ``503`` com detalhes sobre a discrepância.
    """
    try:
        await sessao.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - caminho defensivo
        raise HTTPException(
            status_code=503,
            detail={"status": "unavailable", "motivo": "banco inacessível", "erro": str(exc)},
        ) from exc

    esperado = _carregar_head_alembic()

    def _ler_revisao_sync(conn: Connection) -> str | None:
        return MigrationContext.configure(conn).get_current_revision()

    try:
        conexao = await sessao.connection()
        atual = await conexao.run_sync(_ler_revisao_sync)
    except Exception as exc:  # pragma: no cover - caminho defensivo
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unavailable",
                "motivo": "falha ao ler revisão alembic",
                "erro": str(exc),
            },
        ) from exc

    if esperado is None:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unavailable",
                "motivo": "configuração Alembic não encontrada",
                "revisao_atual": atual,
            },
        )

    if atual != esperado:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unavailable",
                "motivo": "migrações pendentes",
                "revisao_atual": atual,
                "revisao_esperada": esperado,
            },
        )

    return {
        "status": "ready",
        "revisao_alembic": atual,
    }
