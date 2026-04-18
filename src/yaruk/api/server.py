from __future__ import annotations

from fastapi import FastAPI

from yaruk.api.routes import router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Yaruk-OS API",
        description="Dokuman donusum ve analiz API'si",
        version="0.1.0",
    )
    app.include_router(router)

    @app.get("/health")
    def health() -> dict[str, object]:
        from yaruk.core.hardware import probe_hardware
        hw = probe_hardware()
        return {
            "ok": True,
            "gpu_available": hw.has_nvidia,
            "ram_mb": hw.total_ram_mb,
        }

    @app.get("/info")
    def info() -> dict[str, object]:
        from yaruk.core.config import YarukSettings
        from yaruk.core.hardware import probe_hardware
        from yaruk.core.memory_guard import DynamicMemoryGuard
        hw = probe_hardware()
        guard = DynamicMemoryGuard(YarukSettings())
        mem = guard.decide()
        return {
            "os": hw.os,
            "arch": hw.arch,
            "ram_mb": hw.total_ram_mb,
            "nvidia": hw.has_nvidia,
            "nvidia_vram_total_mb": hw.nvidia_vram_total_mb,
            "gpu_decision": mem.can_use_gpu,
            "max_batch_size": mem.max_batch_size,
        }

    return app

