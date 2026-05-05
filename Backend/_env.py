"""
Populate os.environ from .env files so modules that read config at import
time (e.g. Backend.database) work regardless of launcher (uvicorn, scripts).
Values already present in the environment — Fly secrets, shell exports,
container env — are never overridden.

Precedence: Backend/.env > repo-root/.env > nothing.
"""
from __future__ import annotations

import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _apply(path: Path) -> None:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return

    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue

        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]

        os.environ[key] = value


def load() -> None:
    for candidate in (_REPO_ROOT / "Backend" / ".env", _REPO_ROOT / ".env"):
        if candidate.exists():
            _apply(candidate)
