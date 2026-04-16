"""Entry-point CLI para rodar a API localmente."""

from __future__ import annotations

import uvicorn

from climate_risk.core.config import get_settings


def main() -> None:
    """Inicia o servidor uvicorn em ``localhost:8000``."""
    settings = get_settings()
    uvicorn.run(
        "climate_risk.interfaces.app:app",
        host="127.0.0.1",
        port=8000,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
