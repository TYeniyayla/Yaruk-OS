"""VRAM-aware VLM model selector.

Selects the highest-quality VLM that fits in available GPU memory,
filtered by document language and model capabilities.
"""
from __future__ import annotations

import logging

from yaruk.vlm.manifest import VLMManifest, VLMModelSpec, load_manifest

log = logging.getLogger(__name__)


def _get_free_vram_mb() -> int | None:
    """Detect free GPU VRAM in MB. Returns None if no GPU."""
    try:
        import torch
        if not torch.cuda.is_available():
            return None
        free, total = torch.cuda.mem_get_info(0)
        free_mb = int(free / (1024 * 1024))
        log.info("GPU VRAM: %d MB free / %d MB total", free_mb, int(total / (1024 * 1024)))
        return free_mb
    except Exception:
        log.debug("torch VRAM probe failed", exc_info=True)

    try:
        import subprocess
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,nounits,noheader"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return int(result.stdout.strip().split("\n")[0])
    except Exception:
        log.debug("nvidia-smi VRAM probe failed", exc_info=True)

    return None


def select_vlm(
    pdf_language: str = "en",
    manifest: VLMManifest | None = None,
    force_model_id: str | None = None,
    free_vram_override: int | None = None,
) -> VLMModelSpec | None:
    """Select the best VLM for the current hardware and document.

    Returns None if no GPU available (CPU-only: VLM disabled).
    """
    manifest = manifest or load_manifest()
    if not manifest.models:
        log.warning("No VLM models in manifest")
        return None

    if force_model_id:
        spec = manifest.get_by_id(force_model_id)
        if spec:
            log.info("VLM forced: %s (tier %d)", spec.model_id, spec.tier)
            return spec
        log.warning("Forced VLM model '%s' not found in manifest", force_model_id)

    free_vram = free_vram_override or _get_free_vram_mb()
    if free_vram is None:
        log.info("No GPU detected, VLM captioning disabled")
        return None

    candidates = manifest.models_fitting_vram(free_vram)
    if not candidates:
        log.warning("No VLM fits in %d MB VRAM (smallest needs %d MB)",
                     free_vram, min(m.min_vram_mb for m in manifest.models))
        return None

    lang_preferred = [
        m for m in candidates
        if m.supports_language(pdf_language)
    ]
    chosen = lang_preferred[0] if lang_preferred else candidates[0]

    log.info(
        "VLM selected: %s (tier %d, %.1fB params, needs %dMB, free %dMB)",
        chosen.model_id, chosen.tier, chosen.params_b, chosen.min_vram_mb, free_vram,
    )
    return chosen
