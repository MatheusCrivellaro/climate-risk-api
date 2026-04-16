"""Entry-point CLI do worker (stub).

A implementação real do worker (polling, heartbeat, sweep de zumbis) entra
no Slice 5, conforme ``docs/plano-refatoracao.md``. Este arquivo existe
apenas para expor o script ``climate-risk-worker`` desde o Slice 0 e
permitir que scripts de deploy/CI exerçam o entry-point.
"""

from __future__ import annotations

import sys


def main() -> int:
    """Imprime aviso e encerra com sucesso."""
    print("Worker não implementado ainda - slice 5")
    return 0


if __name__ == "__main__":
    sys.exit(main())
