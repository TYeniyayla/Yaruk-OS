"""VLM-based caption refinement for figure blocks.

Enhances heuristic captions with VLM-generated descriptions.
Falls back gracefully to heuristic captions when VLM is unavailable.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from yaruk.models.canonical import PageResult
from yaruk.models.enums import BlockType

log = logging.getLogger(__name__)


class VLMCaptionRefiner:
    """Refines figure captions using a local VLM."""

    def __init__(
        self,
        model: Any,
        processor: Any,
        spec: Any,
        job_dir: Path,
        language: str = "en",
    ) -> None:
        self._model = model
        self._processor = processor
        self._spec = spec
        self._job_dir = job_dir
        self._language = language
        self._refined_count = 0
        self._failed_count = 0

    def refine_pages(self, pages: list[PageResult]) -> int:
        """Refine figure captions across all pages. Returns count of refined figures."""
        from yaruk.vlm.inference import _validate_caption, generate_caption

        for page in pages:
            for block in page.blocks:
                if block.type != BlockType.FIGURE:
                    continue

                rp = block.raw_payload or {}
                asset_path = rp.get("asset_path", "")
                if not asset_path:
                    continue

                full_path = self._job_dir / asset_path
                if not full_path.exists():
                    continue

                existing = rp.get("caption", "")
                vlm_caption = generate_caption(
                    image_path=full_path,
                    model=self._model,
                    processor=self._processor,
                    spec=self._spec,
                    language=self._language,
                    existing_caption=existing,
                )

                if vlm_caption and _validate_caption(vlm_caption):
                    rp["vlm_caption"] = vlm_caption
                    rp["vlm_model_id"] = self._spec.model_id
                    rp["caption_source"] = "vlm"

                    figure_id = rp.get("figure_id", "")
                    summary_parts = []
                    if figure_id:
                        summary_parts.append(figure_id)
                    summary_parts.append(vlm_caption[:250])
                    if asset_path:
                        summary_parts.append(f"(see {asset_path})")
                    block.text = " -- ".join(summary_parts)

                    self._refined_count += 1
                    log.debug("VLM refined: page %d block %s", page.page_number, block.block_id)
                else:
                    rp["caption_source"] = "heuristic"
                    self._failed_count += 1

        log.info(
            "VLM caption refinement: refined=%d, fallback=%d, model=%s",
            self._refined_count, self._failed_count, self._spec.model_id,
        )
        return self._refined_count


def try_vlm_refine(
    pages: list[PageResult],
    job_dir: Path,
    language: str = "en",
    progress_cb: Any = None,
) -> int:
    """Attempt VLM caption refinement. Returns 0 if VLM unavailable."""
    from yaruk.vlm.model_manager import ModelManager
    from yaruk.vlm.selector import select_vlm

    spec = select_vlm(pdf_language=language)
    if spec is None:
        log.info("VLM not available (no GPU or insufficient VRAM), using heuristic captions")
        return 0

    mgr = ModelManager()

    def _dl_progress(msg: str, cur: int, total: int) -> None:
        if progress_cb:
            progress_cb(msg, cur, total)
        log.info(msg)

    if not mgr.is_downloaded(spec):
        log.info(
            "VLM model '%s' not yet downloaded. First download may take a while.",
            spec.model_id,
        )
        _dl_progress(
            f"VLM model indiriliyor: {spec.model_id} ({spec.params_b}B) - "
            f"ilk indirme uzun surebilir, internet hizina bagli",
            0, 100,
        )

    try:
        model, processor = mgr.load_model(spec, progress_cb=_dl_progress)
    except Exception as e:
        log.warning("VLM load failed: %s, falling back to heuristic captions", e)
        return 0

    refiner = VLMCaptionRefiner(
        model=model,
        processor=processor,
        spec=spec,
        job_dir=job_dir,
        language=language,
    )

    result = refiner.refine_pages(pages)
    return result
