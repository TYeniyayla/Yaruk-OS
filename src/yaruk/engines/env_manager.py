from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EngineEnvConfig:
    engine_name: str
    venvs_root: Path
    requirements: list[str]


def venv_path(cfg: EngineEnvConfig) -> Path:
    return cfg.venvs_root / cfg.engine_name


def ensure_venv(cfg: EngineEnvConfig) -> Path:
    """Create or reuse a venv for the given engine. Uses uv if available, else stdlib venv."""
    vp = venv_path(cfg)
    if (vp / "bin" / "python").exists():
        return vp
    try:
        subprocess.check_call(["uv", "venv", str(vp)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        subprocess.check_call(["python3", "-m", "venv", str(vp)])

    pip = str(vp / "bin" / "pip")
    if cfg.requirements:
        subprocess.check_call([pip, "install", *cfg.requirements], stdout=subprocess.DEVNULL)
    return vp


def python_for(cfg: EngineEnvConfig) -> str:
    return str(venv_path(cfg) / "bin" / "python")
