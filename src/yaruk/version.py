"""Single source of truth for package version (mirrors pyproject.toml / hatch)."""
from __future__ import annotations

import importlib.metadata


def get_version() -> str:
    """Installed distribution version, or fallback when running from a source tree without metadata."""
    try:
        return importlib.metadata.version("yaruk-os")
    except importlib.metadata.PackageNotFoundError:
        return "0.1.0"
