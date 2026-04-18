from __future__ import annotations

import os
import platform
import shutil
import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class HardwareProfile:
    os: str
    arch: str
    total_ram_mb: int | None
    has_nvidia: bool
    nvidia_vram_total_mb: int | None
    nvidia_vram_free_mb: int | None


def _read_meminfo_total_mb() -> int | None:
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    parts = line.split()
                    kb = int(parts[1])
                    return kb // 1024
    except Exception:
        return None
    return None


def _nvidia_smi() -> str | None:
    exe = shutil.which("nvidia-smi")
    if not exe:
        return None
    return exe


def read_nvidia_vram_mb() -> tuple[int | None, int | None]:
    """
    Returns (total_mb, free_mb) for GPU0 if available.
    Uses nvidia-smi to avoid extra deps.
    """
    exe = _nvidia_smi()
    if not exe:
        return (None, None)
    try:
        out = subprocess.check_output(
            [
                exe,
                "--query-gpu=memory.total,memory.free",
                "--format=csv,noheader,nounits",
            ],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.0,
        ).strip()
        if not out:
            return (None, None)
        first = out.splitlines()[0]
        total_s, free_s = [x.strip() for x in first.split(",")]
        return (int(total_s), int(free_s))
    except Exception:
        return (None, None)


def probe_hardware() -> HardwareProfile:
    total_ram = _read_meminfo_total_mb()
    nvidia_total, nvidia_free = read_nvidia_vram_mb()
    return HardwareProfile(
        os=platform.system().lower(),
        arch=platform.machine().lower(),
        total_ram_mb=total_ram,
        has_nvidia=nvidia_total is not None,
        nvidia_vram_total_mb=nvidia_total,
        nvidia_vram_free_mb=nvidia_free,
    )


def dynamic_memory_guard_vram_free_mb() -> int | None:
    """Runtime VRAM probe (free MB)."""
    _, free_mb = read_nvidia_vram_mb()
    return free_mb


def dynamic_memory_guard_ram_free_mb() -> int | None:
    """Runtime RAM free probe (best-effort)."""
    if os.path.exists("/proc/meminfo"):
        try:
            mem_free_kb: int | None = None
            mem_avail_kb: int | None = None
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemAvailable:"):
                        mem_avail_kb = int(line.split()[1])
                    if line.startswith("MemFree:"):
                        mem_free_kb = int(line.split()[1])
            kb = mem_avail_kb or mem_free_kb
            return kb // 1024 if kb is not None else None
        except Exception:
            return None
    return None

