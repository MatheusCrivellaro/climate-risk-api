"""Configuração global de testes."""

from __future__ import annotations

import os

# Forçar banco em memória / temporário evita colidir com ambiente do desenvolvedor.
os.environ.setdefault("CLIMATE_RISK_DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("CLIMATE_RISK_LOG_LEVEL", "WARNING")
