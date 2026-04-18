from __future__ import annotations

from dataclasses import dataclass

from yaruk.core.config import GPUPolicy, YarukSettings
from yaruk.core.hardware import (
    dynamic_memory_guard_ram_free_mb,
    dynamic_memory_guard_vram_free_mb,
)


@dataclass(frozen=True)
class MemorySnapshot:
    vram_free_mb: int | None
    ram_free_mb: int | None


@dataclass(frozen=True)
class MemoryDecision:
    can_use_gpu: bool
    max_batch_size: int
    reason: str


class DynamicMemoryGuard:
    def __init__(self, settings: YarukSettings) -> None:
        self._settings = settings

    def probe(self) -> MemorySnapshot:
        return MemorySnapshot(
            vram_free_mb=dynamic_memory_guard_vram_free_mb(),
            ram_free_mb=dynamic_memory_guard_ram_free_mb(),
        )

    def decide(self, snapshot: MemorySnapshot | None = None) -> MemoryDecision:
        snap = snapshot or self.probe()
        policy = self._settings.gpu_policy

        if policy == GPUPolicy.CPU_ONLY:
            can_gpu = False
            reason = "gpu-policy=cpu_only"
        elif policy == GPUPolicy.GPU_PREFERRED:
            can_gpu = snap.vram_free_mb is not None
            reason = "gpu-available" if can_gpu else "no-gpu-detected"
        elif snap.vram_free_mb is not None and snap.vram_free_mb >= self._settings.vram_threshold_mb:
            can_gpu = True
            reason = "gpu-available"
        elif snap.vram_free_mb is None:
            can_gpu = False
            reason = "no-gpu-detected"
        else:
            can_gpu = False
            reason = f"vram-low ({snap.vram_free_mb}MB < {self._settings.vram_threshold_mb}MB)"

        if snap.ram_free_mb is not None and snap.ram_free_mb < self._settings.ram_threshold_mb:
            batch = 1
            reason += f"; ram-low ({snap.ram_free_mb}MB)"
        else:
            batch = 4

        return MemoryDecision(can_use_gpu=can_gpu, max_batch_size=batch, reason=reason)
