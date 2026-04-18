from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore[import-untyped]

            return yaml.safe_load(text) or {}  # type: ignore[no-any-return]
        except ImportError:
            raise RuntimeError("PyYAML required for .yaml config files; pip install pyyaml") from None
    return json.loads(text)  # type: ignore[no-any-return]


def load_config_layers(
    global_path: Path | None = None,
    project_path: Path | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge global -> project -> CLI overrides (last wins)."""
    merged: dict[str, Any] = {}
    for p in (global_path, project_path):
        if p and p.exists():
            merged.update(_load_yaml_or_json(p))
    if cli_overrides:
        merged.update(cli_overrides)
    return merged
