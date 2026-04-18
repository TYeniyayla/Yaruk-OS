"""Resolve engine package versions for provider metadata (avoids hard-coded drift)."""
from __future__ import annotations

import importlib.metadata


def dist_version(distribution_name: str, *, fallback: str = "0.0.0") -> str:
    """Return installed version of *distribution_name* (PyPI name), or *fallback* if not installed."""
    try:
        return importlib.metadata.version(distribution_name)
    except importlib.metadata.PackageNotFoundError:
        return fallback
