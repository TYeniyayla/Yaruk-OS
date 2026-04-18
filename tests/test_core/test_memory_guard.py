from __future__ import annotations

from yaruk.core.config import YarukSettings
from yaruk.core.memory_guard import DynamicMemoryGuard, MemorySnapshot


def test_decide_no_gpu() -> None:
    guard = DynamicMemoryGuard(YarukSettings())
    snap = MemorySnapshot(vram_free_mb=None, ram_free_mb=8000)
    decision = guard.decide(snap)
    assert decision.can_use_gpu is False
    assert "no-gpu" in decision.reason


def test_decide_low_vram() -> None:
    guard = DynamicMemoryGuard(YarukSettings(vram_threshold_mb=2048))
    snap = MemorySnapshot(vram_free_mb=500, ram_free_mb=8000)
    decision = guard.decide(snap)
    assert decision.can_use_gpu is False


def test_decide_gpu_ok() -> None:
    guard = DynamicMemoryGuard(YarukSettings(vram_threshold_mb=1024))
    snap = MemorySnapshot(vram_free_mb=4000, ram_free_mb=8000)
    decision = guard.decide(snap)
    assert decision.can_use_gpu is True
