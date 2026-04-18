"""Segment-based Orchestrator.

MasterPlan flow: Pre-analysis → Segmenter → per-segment Router → Workers → IR Merger → Export.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import tempfile
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import fitz  # type: ignore[import-untyped]

from yaruk.analyzer.classifier import profile_document, suggest_providers
from yaruk.analyzer.preanalyzer import analyze_pdf
from yaruk.core.cache import DiskCache, file_sha256
from yaruk.core.config import YarukSettings
from yaruk.core.merger import merge_blocks
from yaruk.core.progress import ProgressCallback, ProgressTracker
from yaruk.core.registry import ProviderRegistry
from yaruk.core.router import (
    GPU_ENGINES,
    DynamicRouter,
    PageRoutingPlan,
)
from yaruk.core.segmenter import PageSegment, Segmenter
from yaruk.engines.base_worker import WorkerPool
from yaruk.engines.docling.adapter import (
    worker_response_to_blocks as docling_response_to_blocks,
)
from yaruk.engines.marker.adapter import worker_response_to_blocks
from yaruk.engines.markitdown.adapter import (
    worker_response_to_blocks as markitdown_response_to_blocks,
)
from yaruk.engines.mineru.adapter import (
    worker_response_to_blocks as mineru_response_to_blocks,
)
from yaruk.engines.opendataloader.adapter import (
    worker_response_to_blocks as opendataloader_response_to_blocks,
)
from yaruk.models.canonical import (
    AnalysisSignal,
    BoundingBox,
    DocumentBlock,
    DocumentMetadata,
    DocumentResult,
    PageResult,
    ProcessingInfo,
)
from yaruk.models.enums import BlockType, JobStatus
from yaruk.models.output_contract import OutputLayout
from yaruk.observability.logging import get_logger
from yaruk.observability.metrics import MetricsCollector
from yaruk.observability.tracing import new_trace_id
from yaruk.output.asset_manager import AssetManager
from yaruk.output.renderer import export_result, sanitize_page_blocks
from yaruk.queue.manager import QueueConfig, QueueManager

log = logging.getLogger(__name__)

_RESPONSE_TO_BLOCKS: dict[str, Any] = {
    "marker": worker_response_to_blocks,
    "docling": docling_response_to_blocks,
    "mineru": mineru_response_to_blocks,
    "markitdown": markitdown_response_to_blocks,
    "opendataloader": opendataloader_response_to_blocks,
}

SUBSET_FULL_DOC_RATIO = 0.7


def _is_oom_error(err_lower: str) -> bool:
    """Detect CUDA VRAM OOM (not mmap/Linux address space errors)."""
    return "cuda" in err_lower and ("out of memory" in err_lower or "alloc" in err_lower)


def _normalize_pages_keys(pages: Any) -> dict[int, Any]:
    """Ensure page dict keys are int (workers may emit str keys)."""
    if not pages or not isinstance(pages, dict):
        return {}
    out: dict[int, Any] = {}
    for k, v in pages.items():
        try:
            ik = int(k)
        except (TypeError, ValueError):
            continue
        out[ik] = v
    return out


def _compress_sorted_pages_to_ranges(sorted_pages: list[int]) -> list[tuple[int, int]]:
    """[1,2,3,7,8,15] -> [(1,3),(7,8),(15,15)]. Input must be sorted unique."""
    if not sorted_pages:
        return []
    ranges: list[tuple[int, int]] = []
    start = prev = sorted_pages[0]
    for p in sorted_pages[1:]:
        if p == prev + 1:
            prev = p
        else:
            ranges.append((start, prev))
            start = prev = p
    ranges.append((start, prev))
    return ranges


def _remap_subset_pages_inplace(result: dict[str, Any], page_map: dict[int, int]) -> None:
    """Map subset PDF page indices to original 1-based page numbers. page_map: subset_1based -> original_1based."""
    pages = result.get("pages") or {}
    if not pages:
        return
    new_pages: dict[int, Any] = {}
    for subset_k, page_data in pages.items():
        try:
            sk = int(subset_k)
        except (TypeError, ValueError):
            continue
        orig = page_map.get(sk)
        if orig is None:
            continue
        if isinstance(page_data, dict):
            pd = dict(page_data)
            pd["page_number"] = orig
            new_pages[orig] = pd
        else:
            new_pages[orig] = page_data
    result["pages"] = new_pages


def _merge_pages_into_cache_entry(
    existing: dict[str, Any], new_pages: dict[int, Any],
) -> dict[str, Any]:
    """Merge page dicts; new entries do not overwrite non-empty blocks unless missing."""
    merged = dict(_normalize_pages_keys(existing.get("pages")))
    for pn, pdata in _normalize_pages_keys(new_pages).items():
        old = merged.get(pn)
        if old is None or not old.get("blocks") or pdata.get("blocks"):
            merged[pn] = pdata
    existing["pages"] = merged
    return existing


@dataclass(frozen=True)
class OrchestratorConfig:
    settings: YarukSettings
    output_dir: Path
    db_path: Path | None = None


@dataclass
class _EngineCache:
    """Per-PDF full conversion results keyed by provider name."""
    results: dict[str, dict[str, Any]] = field(default_factory=dict)


def _discover_available_engines(registry: ProviderRegistry | None = None) -> list[str]:
    """Discover engines via registry entrypoints, then probe imports as fallback."""
    available: list[str] = []

    if registry is not None:
        report = registry.discover_entrypoints()
        for name in report.loaded:
            try:
                prov = registry.get(name)
                health = prov.health_check()
                if health.ok:
                    available.append(name)
                else:
                    log.info("provider %s unhealthy: %s", name, health.detail)
            except Exception as e:
                log.debug("provider %s health check failed: %s", name, e)

    if available:
        return available

    _PROBE_MAP: dict[str, str] = {
        "marker": "yaruk.engines.marker.worker",
        "docling": "yaruk.engines.docling.worker",
        "mineru": "yaruk.engines.mineru.worker",
        "markitdown": "yaruk.engines.markitdown.worker",
        "opendataloader": "yaruk.engines.opendataloader.worker",
    }
    _AVAIL_ATTR: dict[str, str] = {
        "marker": "MARKER_AVAILABLE",
        "docling": "DOCLING_AVAILABLE",
        "mineru": "MINERU_AVAILABLE",
        "markitdown": "MARKITDOWN_AVAILABLE",
        "opendataloader": "OPENDATALOADER_AVAILABLE",
    }

    import importlib
    for engine_name, module_path in _PROBE_MAP.items():
        try:
            mod = importlib.import_module(module_path)
            attr = _AVAIL_ATTR[engine_name]
            if getattr(mod, attr, False):
                available.append(engine_name)
        except (ImportError, AttributeError):
            pass

    if not available:
        available.append("marker")

    return available


def _instantiate_providers(registry: ProviderRegistry, names: list[str]) -> dict[str, Any]:
    """Get provider instances from registry for supports() scoring."""
    import contextlib
    result: dict[str, Any] = {}
    for name in names:
        with contextlib.suppress(Exception):
            result[name] = registry.get(name)
    return result


class Orchestrator:
    def __init__(
        self,
        cfg: OrchestratorConfig,
        registry: ProviderRegistry | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self._cfg = cfg
        self._registry = registry or ProviderRegistry()
        self._available_engines = _discover_available_engines(self._registry)
        self._provider_instances = _instantiate_providers(self._registry, self._available_engines)
        self._router = DynamicRouter(
            cfg.settings,
            available_providers=self._available_engines,
            provider_instances=self._provider_instances,
        )
        self._segmenter = Segmenter(cfg.settings)
        self._metrics = MetricsCollector()
        self._queue: QueueManager | None = None
        self._engine_cache = _EngineCache()
        self._disk_cache: DiskCache | None = None
        if cfg.settings.cache_enabled and cfg.settings.cache_dir:
            self._disk_cache = DiskCache(cfg.settings.cache_dir)
        self._use_subprocess = cfg.settings.use_subprocess
        self._worker_pool: WorkerPool | None = None
        self._subprocess_oom_engines: set[str] = set()
        self._progress = ProgressTracker(progress_callback)

        if cfg.db_path:
            self._queue = QueueManager(QueueConfig(db_path=cfg.db_path))

        self._gpu_available = self._check_gpu_availability()
        log.info("orchestrator init: available engines = %s, subprocess=%s, gpu=%s",
                 self._available_engines, self._use_subprocess, self._gpu_available)

    @staticmethod
    def _check_gpu_availability() -> bool:
        try:
            import torch
            if torch.cuda.is_available():
                dev = torch.cuda.get_device_name(0)
                vram = torch.cuda.get_device_properties(0).total_memory // (1024**2)
                log.info("GPU detected: %s (%d MB VRAM) - engines will use GPU", dev, vram)
                return True
            else:
                log.warning(
                    "NO GPU detected! All engines will run on CPU. "
                    "Processing will be significantly slower (10-50x). "
                    "Install CUDA-compatible PyTorch for GPU acceleration."
                )
                return False
        except ImportError:
            log.warning(
                "PyTorch not found - GPU unavailable. "
                "Processing will run on CPU only (very slow for large documents)."
            )
            return False

    def build_processing_info(self) -> ProcessingInfo:
        return ProcessingInfo(
            trace_id=new_trace_id(),
            config_snapshot=self._cfg.settings.model_dump(),
        )

    def can_use_gpu_now(self) -> bool:
        return self._router.can_use_gpu_now()

    def empty_result(self, source_path: Path) -> DocumentResult:
        return DocumentResult(
            source_path=source_path,
            total_pages=0,
            pages=[],
            processing_info=self.build_processing_info(),
        )

    def process_sync(self, source_path: Path, max_pages: int | None = None) -> DocumentResult:
        return asyncio.run(self.process(source_path, max_pages=max_pages))

    async def process(self, source_path: Path, max_pages: int | None = None) -> DocumentResult:
        slog = get_logger(source=str(source_path))
        trace_id = new_trace_id()
        job_id = f"job-{uuid.uuid4().hex[:8]}"

        if self._queue:
            self._queue.create_job(job_id, source_path)
            self._queue.update_job_status(job_id, JobStatus.RUNNING)

        page_results: list[PageResult] = []
        layout: OutputLayout | None = None
        engines_to_run: list[str] = []

        try:
            # ----------------------------------------------------------
            # 1. Pre-analysis
            # ----------------------------------------------------------
            slog.info("pre-analysis starting", trace_id=trace_id)
            from yaruk.analyzer.preanalyzer import PreAnalysisConfig
            pre_cfg = PreAnalysisConfig(max_pages=max_pages) if max_pages else None
            signals = analyze_pdf(source_path, cfg=pre_cfg)
            total_pages = len(signals)
            slog.info("pre-analysis done", pages=total_pages, trace_id=trace_id)

            doc_profile = profile_document(signals)
            suggested = suggest_providers(doc_profile)

            slog.info(
                "document profile",
                profile=doc_profile.dominant_type,
                has_math=doc_profile.has_math,
                has_tables=doc_profile.has_tables,
                suggested=suggested,
                trace_id=trace_id,
            )

            layout = OutputLayout.for_job(self._cfg.output_dir, job_id)
            layout.ensure_dirs()
            asset_mgr = AssetManager(layout.assets_dir)

            doc = fitz.open(str(source_path))
            try:
                # ----------------------------------------------------------
                # 2. Segmentation (MasterPlan 3.2)
                # ----------------------------------------------------------
                slog.info("segmentation starting", trace_id=trace_id)
                self._progress.begin_stage("Segmentation", total_pages)
                page_layouts = self._segmenter.segment_document(source_path, max_pages=max_pages)
                total_segments = sum(len(pl.segments) for pl in page_layouts)
                self._progress.finish_stage()
                slog.info("segmentation done", pages=len(page_layouts), segments=total_segments, trace_id=trace_id)

                # ----------------------------------------------------------
                # 3. Per-segment routing (MasterPlan 3.1)
                # ----------------------------------------------------------
                routing_plans: list[PageRoutingPlan] = []
                all_needed_providers: set[str] = set()
                for pl in page_layouts:
                    plan = self._router.route_segments(pl.segments, pl.page_number)
                    routing_plans.append(plan)
                    all_needed_providers.update(plan.providers_needed)

                engines_to_run = [
                    e for e in self._available_engines
                    if e in all_needed_providers and e != "opendataloader"
                ]
                if not engines_to_run:
                    engines_to_run = [
                        e for e in self._available_engines[:2] if e != "opendataloader"
                    ]
                engines_to_run = list(dict.fromkeys(engines_to_run))
                if "marker" in engines_to_run and engines_to_run[0] != "marker":
                    engines_to_run.remove("marker")
                    engines_to_run.insert(0, "marker")

                slog.info(
                    "routing plan summary",
                    engines_needed=list(all_needed_providers),
                    engines_to_run=engines_to_run,
                    trace_id=trace_id,
                )

                # ----------------------------------------------------------
                # 4. Engine runs: subset-aware cache population (MasterPlan 3.x)
                # ----------------------------------------------------------
                self._progress.begin_stage("Engines", len(engines_to_run))
                self._run_engines_for_document(
                    source_path=source_path,
                    engines_to_run=engines_to_run,
                    routing_plans=routing_plans,
                    signals=signals,
                    total_pages=total_pages,
                    max_pages=max_pages,
                    doc=doc,
                    slog=slog,
                    trace_id=trace_id,
                )
                self._run_pass2_low_confidence_fallbacks(
                    source_path=source_path,
                    routing_plans=routing_plans,
                    total_pages=total_pages,
                    max_pages=max_pages,
                    doc=doc,
                    slog=slog,
                    trace_id=trace_id,
                )
                self._progress.finish_stage()

                # ----------------------------------------------------------
                # 5. Per-page, per-segment block extraction + merge
                # ----------------------------------------------------------
                routing_decisions: list[dict[str, Any]] = []
                page_results.clear()
                self._progress.begin_stage("Pages", total_pages)

                resume_from = 0
                if self._queue:
                    resume_from = self._queue.last_completed_page(job_id)
                    if resume_from > 0:
                        slog.info("resuming from checkpoint", page=resume_from, trace_id=trace_id)

                for pl, plan in zip(page_layouts, routing_plans, strict=False):
                    if pl.page_number <= resume_from:
                        continue

                    segment_routing_info = []
                    all_page_blocks: list[list[DocumentBlock]] = []

                    for seg_idx, (seg, decision) in enumerate(
                        zip(pl.segments, plan.segment_decisions, strict=False)
                    ):
                        seg_blocks = self._extract_segment_blocks(
                            source_path, pl.page_number, seg, decision.provider_chain,
                            decision.best_provider,
                        )
                        if seg_blocks:
                            all_page_blocks.append(seg_blocks)

                        segment_routing_info.append({
                            "seg_idx": seg_idx,
                            "type": seg.block_type,
                            "best_provider": decision.best_provider,
                            "score": decision.score,
                            "chain": decision.provider_chain,
                            "blocks_found": len(seg_blocks),
                        })

                    merged = merge_blocks(all_page_blocks) if all_page_blocks else []

                    if not merged:
                        legacy_routing = self._router.route_page(signals[pl.page_number - 1])
                        merged = self._get_page_blocks_multi(
                            source_path, pl.page_number, legacy_routing.provider_chain,
                        )
                    if not merged:
                        merged = self._pymupdf_fallback(doc, pl.page_number)

                    page_providers = list({b.source_provider for b in merged})
                    routing_decisions.append({
                        "page": pl.page_number,
                        "segment_count": len(pl.segments),
                        "segments": segment_routing_info,
                        "providers_used": page_providers,
                    })

                    page = doc.load_page(pl.page_number - 1)
                    page_results.append(PageResult(
                        page_number=pl.page_number,
                        width=float(page.rect.width),
                        height=float(page.rect.height),
                        blocks=merged,
                    ))

                    page_img_assets = self._store_pymupdf_page_images(
                        doc, pl.page_number, asset_mgr,
                    )
                    has_figure = any(b.type == BlockType.FIGURE for b in merged)
                    if page_img_assets and not has_figure:
                        for img_idx, (asset_rel, _img_bbox) in enumerate(page_img_assets):
                            merged.append(DocumentBlock(
                                page=pl.page_number,
                                block_id=f"p{pl.page_number}-figure-{img_idx}",
                                type=BlockType.FIGURE,
                                text=f"Figure (page {pl.page_number})",
                                bbox=BoundingBox(x0=0.0, y0=0.0, x1=612.0, y1=100.0),
                                confidence=0.6,
                                source_provider="pymupdf",
                                source_version="image-extract",
                                reading_order=len(merged) + img_idx,
                                raw_payload={"asset_path": asset_rel},
                            ))

                    sanitize_page_blocks(page_results[-1])
                    page_path = layout.pages_dir / f"page_{pl.page_number:03d}.json"
                    page_path.write_text(
                        page_results[-1].model_dump_json(indent=2), encoding="utf-8",
                    )

                    if self._queue:
                        self._queue.set_page_done(job_id, pl.page_number)

                    self._progress.update(
                        pl.page_number,
                        message=f"p{pl.page_number} {len(merged)} blocks",
                        providers=page_providers,
                    )

                self._progress.finish_stage()

            finally:
                doc.close()

            # ----- Store Marker's rendered images as assets -----
            marker_rewrite_map = self._store_marker_images(source_path, asset_mgr)
            if marker_rewrite_map:
                self._rewrite_figure_block_assets(page_results, marker_rewrite_map)
            slog.info(
                "marker images stored",
                count=len(marker_rewrite_map), trace_id=trace_id,
            )

            # ----- LLM-friendly figure captions (heuristic) -----
            from yaruk.postprocess.figure_captioner import caption_all_figures
            captioned = caption_all_figures(page_results)
            slog.info("heuristic figure captioning done", enriched=captioned, trace_id=trace_id)

            # ----- VLM caption refinement (GPU only, with stall guard) -----
            self._run_vlm_with_timeout(
                page_results, layout, doc_profile, slog, trace_id,
            )

            metadata = self._extract_metadata(source_path)
            proc_info = ProcessingInfo(
                trace_id=trace_id,
                config_snapshot=self._cfg.settings.model_dump(),
                routing_decisions=routing_decisions,
                provider_versions={e: "full-doc" for e in engines_to_run},
            )

            page_results.sort(key=lambda p: p.page_number)

            result = DocumentResult(
                source_path=source_path,
                total_pages=total_pages,
                pages=page_results,
                assets=asset_mgr.index,
                metadata=metadata,
                processing_info=proc_info,
            )

            best_md: str | None = None
            if self._cfg.settings.use_engine_full_markdown:
                best_md = self._pick_best_full_markdown(source_path, engines_to_run)
                if best_md and marker_rewrite_map:
                    best_md = self._rewrite_image_links(best_md, marker_rewrite_map)
            export_result(result, layout, full_markdown=best_md)

            for rec in self._metrics.records:
                log.info("metric: %s.%s = %.0f ms", rec.provider, rec.method, rec.elapsed_ms)
            if self._img_extract_fail_count:
                log.info(
                    "image extraction: %d images skipped (unsupported colorspace/format)",
                    self._img_extract_fail_count,
                )
            slog.info("export done", job_id=job_id, pages=total_pages, trace_id=trace_id)

            if self._queue:
                self._queue.update_job_status(job_id, JobStatus.DONE)

            self._engine_cache.results.clear()
            self._cleanup_workers()
            return result

        except Exception as exc:
            slog.error("processing failed", error=str(exc), trace_id=trace_id)
            self._try_partial_export(layout, page_results, source_path, engines_to_run, slog, trace_id)
            if self._queue:
                self._queue.update_job_status(job_id, JobStatus.FAILED, error_msg=str(exc))
            self._engine_cache.results.clear()
            self._cleanup_workers()
            raise

    def _try_partial_export(
        self,
        layout: OutputLayout | None,
        page_results: list[PageResult],
        source_path: Path,
        engines_to_run: list[str],
        slog: Any,
        trace_id: str,
    ) -> None:
        """Best-effort partial export so the user always gets *something*."""
        if not layout or not page_results:
            return
        try:
            from yaruk.output.renderer import render_page_markdown
            from yaruk.postprocess.pipeline import beautify_markdown

            page_results.sort(key=lambda p: p.page_number)
            md_parts = [render_page_markdown(p) for p in page_results]
            merged_md = "\n---\n\n".join(md_parts) if md_parts else ""
            merged_md = beautify_markdown(merged_md)
            layout.merged_md.write_text(merged_md, encoding="utf-8")

            partial_result = DocumentResult(
                source_path=source_path,
                total_pages=len(page_results),
                pages=page_results,
                metadata=DocumentMetadata(),
                processing_info=ProcessingInfo(trace_id=trace_id),
            )
            layout.merged_json.write_text(
                partial_result.model_dump_json(indent=2), encoding="utf-8",
            )
            slog.warning(
                "PARTIAL EXPORT saved",
                pages=len(page_results),
                output=str(layout.job_dir),
                trace_id=trace_id,
            )
            log.warning(
                "Partial export: %d pages saved to %s (processing failed but output preserved)",
                len(page_results), layout.job_dir,
            )
        except Exception as pe:
            log.debug("partial export also failed: %s", pe, exc_info=True)

    def _run_vlm_with_timeout(
        self,
        page_results: list[PageResult],
        layout: OutputLayout,
        doc_profile: Any,
        slog: Any,
        trace_id: str,
        timeout_s: float = 1800.0,
    ) -> int:
        """Run VLM caption refinement in a daemon thread with stall protection."""
        import threading
        result_box: list[int] = []
        error_box: list[str] = []

        lang = getattr(doc_profile, "language", "en") if doc_profile else "en"

        def _vlm_work() -> None:
            try:
                from yaruk.postprocess.vlm_caption_refiner import try_vlm_refine
                r = try_vlm_refine(pages=page_results, job_dir=layout.job_dir, language=lang)
                result_box.append(r)
            except Exception as e:
                error_box.append(str(e)[:300])

        t = threading.Thread(target=_vlm_work, daemon=True)
        t.start()
        t.join(timeout=timeout_s)

        if t.is_alive():
            slog.warning(
                "VLM caption refinement stalled (>%.0fs), skipping. "
                "Heuristic captions preserved. Output quality unaffected.",
                timeout_s,
                trace_id=trace_id,
            )
            log.warning("VLM stalled after %.0fs — daemon thread abandoned", timeout_s)
            return 0

        if error_box:
            slog.warning("VLM caption refinement skipped", error=error_box[0], trace_id=trace_id)
            return 0

        refined = result_box[0] if result_box else 0
        if refined:
            slog.info("VLM caption refinement done", refined=refined, trace_id=trace_id)
        return refined

    def _cleanup_workers(self) -> None:
        if self._worker_pool:
            self._worker_pool.close_all()
            self._worker_pool = None
        try:
            import gc
            gc.collect()
            import torch
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            log.debug("VRAM cleanup error during worker shutdown", exc_info=True)

    # ------------------------------------------------------------------
    # Engine execution (subset-aware cache population)
    # ------------------------------------------------------------------

    @staticmethod
    def _create_subset_pdf(
        source_path: Path, page_numbers: list[int],
    ) -> tuple[Path, dict[int, int]]:
        """Build a temp PDF with selected pages. Returns (path, subset_1based -> original_1based)."""
        uniq = sorted(set(page_numbers))
        doc = fitz.open(str(source_path))
        subset = fitz.open()
        try:
            for start, end in _compress_sorted_pages_to_ranges(uniq):
                subset.insert_pdf(doc, from_page=start - 1, to_page=end - 1)
            page_map: dict[int, int] = {}
            for new_idx, orig_page in enumerate(uniq):
                page_map[new_idx + 1] = orig_page
            fd, tmp = tempfile.mkstemp(suffix=".pdf", prefix="yaruk_subset_")
            os.close(fd)
            subset.save(tmp)
            return Path(tmp), page_map
        finally:
            subset.close()
            doc.close()

    def _after_engine_gpu_cleanup(self, engine_name: str) -> None:
        if engine_name not in GPU_ENGINES:
            return
        if self._worker_pool:
            self._worker_pool.close(engine_name)
            log.info("closed subprocess worker for %s (VRAM release)", engine_name)
        try:
            import gc
            gc.collect()
            import torch
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                free, total = torch.cuda.mem_get_info()
                log.info(
                    "VRAM after %s cleanup: %d MB free / %d MB total",
                    engine_name,
                    free // (1024**2),
                    total // (1024**2),
                )
        except Exception:
            log.debug("VRAM cleanup error for %s", engine_name, exc_info=True)

    def _build_engine_page_sets(
        self,
        routing_plans: list[PageRoutingPlan],
        signals: list[AnalysisSignal],
    ) -> dict[str, set[int]]:
        """Pass-1: best_provider pages only, plus pre-analysis table/equation hints."""
        engine_pages: dict[str, set[int]] = defaultdict(set)
        for plan in routing_plans:
            for decision in plan.segment_decisions:
                engine_pages[decision.best_provider].add(plan.page_number)
        for sig in signals:
            if sig.has_table_signals:
                engine_pages["docling"].add(sig.page_number)
            if sig.has_equation_signals:
                engine_pages["mineru"].add(sig.page_number)
        return dict(engine_pages)

    def _run_engines_for_document(
        self,
        source_path: Path,
        engines_to_run: list[str],
        routing_plans: list[PageRoutingPlan],
        signals: list[AnalysisSignal],
        total_pages: int,
        max_pages: int | None,
        doc: Any | None,
        slog: Any,
        trace_id: str,
    ) -> None:
        if not engines_to_run:
            return
        engine_pages = self._build_engine_page_sets(routing_plans, signals)

        if slog:
            slog.info(
                "multi-engine conversion starting (subset-aware)",
                engines=engines_to_run,
                engine_page_counts={k: len(v) for k, v in engine_pages.items()},
                trace_id=trace_id,
            )

        for idx, eng in enumerate(engines_to_run):
            self._progress.update(idx, message=f"Engine: {eng}", engine=eng)
            try:
                if eng == "marker":
                    self._ensure_full_conversion(
                        source_path,
                        eng,
                        max_pages=max_pages,
                        doc=doc,
                        total_page_estimate=total_pages,
                    )
                else:
                    pages = engine_pages.get(eng) or set()
                    if not pages:
                        log.info("skipping engine %s: no pages in routing plan", eng)
                        self._progress.update(idx + 1, message=f"{eng} skipped", engine=eng)
                        continue
                    ratio = len(pages) / max(total_pages, 1)
                    if ratio >= SUBSET_FULL_DOC_RATIO:
                        log.info(
                            "engine %s: full document (subset ratio %.2f >= %.2f)",
                            eng, ratio, SUBSET_FULL_DOC_RATIO,
                        )
                        self._ensure_full_conversion(
                            source_path,
                            eng,
                            max_pages=max_pages,
                            doc=doc,
                            total_page_estimate=total_pages,
                        )
                    else:
                        log.info(
                            "engine %s: subset %d pages (ratio %.2f)",
                            eng, len(pages), ratio,
                        )
                        self._ensure_engine_via_subset_pdf(
                            source_path, eng, pages, total_pages, max_pages, doc,
                        )
                self._progress.update(idx + 1, message=f"{eng} done", engine=eng)
                if slog:
                    slog.info("engine done", engine=eng, trace_id=trace_id)
            except Exception as e:
                err_str = str(e)[:200]
                is_stall = "stall-watchdog" in err_str
                if is_stall:
                    log.warning(
                        "ENGINE STALLED: %s — switching to fallback engines. Output preserved.",
                        eng,
                    )
                    self._progress.update(
                        idx + 1,
                        message=f"{eng} stalled → fallback",
                        engine=eng,
                    )
                else:
                    log.warning("engine %s failed: %s", eng, err_str)
                    self._progress.update(idx + 1, message=f"{eng} failed", engine=eng)
            finally:
                self._after_engine_gpu_cleanup(eng)

        if slog:
            cached_engines = [
                e for e in engines_to_run
                if len(
                    _normalize_pages_keys(
                        self._engine_cache.results.get(f"{e}:{source_path}", {}).get("pages"),
                    ),
                )
                > 0
            ]
            slog.info(
                "multi-engine conversion done",
                cached_engines=cached_engines, trace_id=trace_id,
            )

    def _ensure_engine_via_subset_pdf(
        self,
        source_path: Path,
        provider_name: str,
        page_numbers: set[int],
        total_pages: int,
        max_pages: int | None,
        doc: Any | None,
    ) -> None:
        cache_key = f"{provider_name}:{source_path}"
        subset_path: Path | None = None
        try:
            subset_path, page_map = self._create_subset_pdf(source_path, list(page_numbers))
            params: dict[str, Any] = {"pdf_path": str(subset_path)}
            if max_pages:
                params["max_pages"] = min(max_pages, len(page_map))

            result: dict[str, Any] | None = None
            saved_cuda_env: str | None = None
            is_oom = False
            if self._use_subprocess:
                result = self._run_via_subprocess(provider_name, params)
            if result is None:
                is_oom = provider_name in self._subprocess_oom_engines
                if self._worker_pool:
                    self._worker_pool.close(provider_name)
                try:
                    import gc
                    gc.collect()
                    import torch
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                    if is_oom:
                        saved_cuda_env = os.environ.get("CUDA_VISIBLE_DEVICES")
                        os.environ["CUDA_VISIBLE_DEVICES"] = ""
                except Exception:
                    log.debug("VRAM cleanup error for subset %s", provider_name, exc_info=True)
                result = self._run_in_process(provider_name, params)
                if saved_cuda_env is not None:
                    os.environ["CUDA_VISIBLE_DEVICES"] = saved_cuda_env
                elif "CUDA_VISIBLE_DEVICES" in os.environ and is_oom:
                    del os.environ["CUDA_VISIBLE_DEVICES"]

            if not result:
                result = {"pages": {}, "markdown": ""}

            self._postprocess_result(result, provider_name, source_path, doc, max_pages)
            _remap_subset_pages_inplace(result, page_map)
            result["pages"] = _normalize_pages_keys(result.get("pages"))
            prior = self._engine_cache.results.get(cache_key)
            if prior and _normalize_pages_keys(prior.get("pages")):
                merged_entry = dict(prior)
                _merge_pages_into_cache_entry(merged_entry, result.get("pages") or {})
                self._engine_cache.results[cache_key] = merged_entry
            else:
                self._engine_cache.results[cache_key] = result

            n = len(_normalize_pages_keys(self._engine_cache.results[cache_key].get("pages")))
            log.info(
                "cached %s: %d pages after subset merge (key=%s)",
                provider_name, n, cache_key,
            )
        finally:
            if subset_path is not None:
                with contextlib.suppress(OSError):
                    subset_path.unlink(missing_ok=True)

    def _run_pass2_low_confidence_fallbacks(
        self,
        source_path: Path,
        routing_plans: list[PageRoutingPlan],
        total_pages: int,
        max_pages: int | None,
        doc: Any | None,
        slog: Any,
        trace_id: str,
    ) -> None:
        """Second pass: run fallback engines for low-confidence segments (subset)."""
        thr = self._cfg.settings.confidence_threshold
        extra: dict[str, set[int]] = defaultdict(set)
        for plan in routing_plans:
            for dec in plan.segment_decisions:
                if dec.score >= thr:
                    continue
                for fb in dec.provider_chain[1:4]:
                    if fb in ("opendataloader", "marker"):
                        continue
                    extra[fb].add(plan.page_number)

        for eng, pages in extra.items():
            if not pages or eng not in self._available_engines:
                continue
            cache_key = f"{eng}:{source_path}"
            existing = _normalize_pages_keys(
                self._engine_cache.results.get(cache_key, {}).get("pages"),
            )
            needed = {
                p for p in pages
                if p not in existing or not (existing.get(p) or {}).get("blocks")
            }
            if not needed:
                continue
            ratio = len(needed) / max(total_pages, 1)
            if slog:
                slog.info(
                    "pass2 fallback engine",
                    engine=eng,
                    pages=len(needed),
                    ratio=round(ratio, 3),
                    trace_id=trace_id,
                )
            try:
                if ratio >= SUBSET_FULL_DOC_RATIO:
                    self._ensure_full_conversion(
                        source_path,
                        eng,
                        max_pages=max_pages,
                        doc=doc,
                        total_page_estimate=total_pages,
                    )
                else:
                    self._ensure_engine_via_subset_pdf(
                        source_path, eng, needed, total_pages, max_pages, doc,
                    )
            except Exception as e:
                log.warning("pass2 fallback %s failed: %s", eng, str(e)[:200])
            finally:
                self._after_engine_gpu_cleanup(eng)

    def _ensure_full_conversion(
        self,
        source_path: Path,
        provider_name: str,
        max_pages: int | None = None,
        doc: Any | None = None,
        pdf_path: Path | None = None,
        total_page_estimate: int | None = None,
    ) -> None:
        cache_key = f"{provider_name}:{source_path}"
        estimate = total_page_estimate
        if estimate is None and doc is not None:
            try:
                estimate = doc.page_count
            except Exception:
                estimate = None
        if max_pages is not None and estimate is not None:
            estimate = min(max_pages, estimate)
        elif max_pages is not None:
            estimate = max_pages

        if cache_key in self._engine_cache.results:
            existing = _normalize_pages_keys(
                self._engine_cache.results[cache_key].get("pages"),
            )
            if existing:
                if estimate is not None and len(existing) >= estimate:
                    log.info(
                        "cache hit (complete) %s: %d/%d pages",
                        cache_key, len(existing), estimate,
                    )
                    return
                if estimate is None:
                    log.info("cache hit %s: %d pages", cache_key, len(existing))
                    return

        if self._disk_cache and not (pdf_path and pdf_path != source_path):
            fhash = file_sha256(source_path)
            disk_result = self._disk_cache.get(fhash, provider_name)
            if disk_result is not None:
                disk_result["pages"] = _normalize_pages_keys(disk_result.get("pages"))
                self._engine_cache.results[cache_key] = disk_result
                n = len(disk_result.get("pages") or {})
                log.info("disk cache hit %s: %d pages", cache_key, n)
                return
        else:
            fhash = None

        path_for_worker = pdf_path or source_path
        params: dict[str, Any] = {"pdf_path": str(path_for_worker)}
        if max_pages:
            params["max_pages"] = max_pages

        saved_cuda_env: str | None = None
        is_oom = False

        with self._metrics.measure(provider_name, "ensure_full_conversion"):
            if self._use_subprocess:
                result = self._run_via_subprocess(provider_name, params)
                if result is not None:
                    self._postprocess_result(result, provider_name, source_path, doc, max_pages)
                    result["pages"] = _normalize_pages_keys(result.get("pages"))
                    self._engine_cache.results[cache_key] = result
                    n = len(result.get("pages") or {})
                    log.info("cached %s: %d pages (key=%s)", provider_name, n, cache_key)
                    if self._disk_cache and fhash:
                        self._disk_cache.put(fhash, provider_name, result, source_path)
                    return
                is_oom = provider_name in self._subprocess_oom_engines
                log.info(
                    "subprocess failed for %s, falling back to in-process%s",
                    provider_name,
                    " (OOM detected, forcing CPU)" if is_oom else "",
                )
                if self._worker_pool:
                    self._worker_pool.close(provider_name)
                try:
                    import gc
                    gc.collect()
                    import torch
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                    if is_oom:
                        saved_cuda_env = os.environ.get("CUDA_VISIBLE_DEVICES")
                        os.environ["CUDA_VISIBLE_DEVICES"] = ""
                except Exception:
                    log.debug("VRAM cleanup error during fallback for %s", provider_name, exc_info=True)

            result = self._run_in_process(provider_name, params)
            if saved_cuda_env is not None:
                os.environ["CUDA_VISIBLE_DEVICES"] = saved_cuda_env
            elif "CUDA_VISIBLE_DEVICES" in os.environ and is_oom:
                del os.environ["CUDA_VISIBLE_DEVICES"]
            self._postprocess_result(result, provider_name, source_path, doc, max_pages)
            result["pages"] = _normalize_pages_keys(result.get("pages"))
            self._engine_cache.results[cache_key] = result
            n = len(result.get("pages") or {})
            log.info("cached %s: %d pages (key=%s)", provider_name, n, cache_key)
            if self._disk_cache and fhash:
                self._disk_cache.put(fhash, provider_name, result, source_path)

    def _run_via_subprocess(
        self, provider_name: str, params: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            if self._worker_pool is None:
                self._worker_pool = WorkerPool()

            resp = self._worker_pool.request(
                provider_name, "convert_full", params,
                timeout_s=None,
                use_watchdog=True,
                stall_timeout_s=1800.0,
                grace_s=120.0,
            )

            all_errors = ""
            if resp.ok and resp.result:
                r = resp.result
                err = r.get("error")
                if err:
                    all_errors = str(err).lower()
                    partial = _normalize_pages_keys(r.get("pages"))
                    if partial:
                        log.warning(
                            "subprocess %s returned error but partial pages (%d): %s",
                            provider_name,
                            len(partial),
                            str(err)[:200],
                        )
                        return r
                    log.warning(
                        "subprocess %s returned engine error: %s",
                        provider_name,
                        str(err)[:200],
                    )
                    if _is_oom_error(all_errors):
                        self._subprocess_oom_engines.add(provider_name)
                    return None
                return r

            all_errors = (resp.error or "unknown").lower()
            log.warning(
                "subprocess %s returned error: %s",
                provider_name, (resp.error or "unknown")[:200],
            )
            if _is_oom_error(all_errors):
                self._subprocess_oom_engines.add(provider_name)
            return None

        except Exception as e:
            err_lower = str(e).lower()
            log.warning("subprocess %s failed: %s", provider_name, str(e)[:200])
            if _is_oom_error(err_lower):
                self._subprocess_oom_engines.add(provider_name)
            return None

    def _run_in_process(
        self, provider_name: str, params: dict[str, Any],
    ) -> dict[str, Any]:
        _HANDLER_MAP: dict[str, type] = {}

        if provider_name == "marker":
            from yaruk.engines.marker.worker import MarkerWorkerHandler
            _HANDLER_MAP["marker"] = MarkerWorkerHandler
        elif provider_name == "docling":
            from yaruk.engines.docling.worker import DoclingWorkerHandler
            _HANDLER_MAP["docling"] = DoclingWorkerHandler
        elif provider_name == "mineru":
            from yaruk.engines.mineru.worker import MinerUWorkerHandler
            _HANDLER_MAP["mineru"] = MinerUWorkerHandler
        elif provider_name == "markitdown":
            from yaruk.engines.markitdown.worker import MarkItDownWorkerHandler
            _HANDLER_MAP["markitdown"] = MarkItDownWorkerHandler
        elif provider_name == "opendataloader":
            from yaruk.engines.opendataloader.worker import OpenDataLoaderWorkerHandler
            _HANDLER_MAP["opendataloader"] = OpenDataLoaderWorkerHandler
        else:
            return {"pages": {}, "markdown": ""}

        handler_cls = _HANDLER_MAP.get(provider_name)
        if not handler_cls:
            return {"pages": {}, "markdown": ""}

        handler = handler_cls()
        result = handler.handle("convert_full", params)

        if result.get("error"):
            partial = _normalize_pages_keys(result.get("pages"))
            if partial:
                log.warning(
                    "%s conversion reported error but keeping %d partial pages: %s",
                    provider_name, len(partial), str(result["error"])[:200],
                )
                return result
            log.warning("%s conversion failed: %s", provider_name, result["error"])
            return {"pages": {}, "markdown": ""}

        return result

    def _postprocess_result(
        self,
        result: dict[str, Any],
        provider_name: str,
        source_path: Path,
        doc: Any | None,
        max_pages: int | None,
    ) -> None:
        if provider_name == "marker" and doc:
            full_md = result.get("markdown", "")
            page_count = max_pages or (doc.page_count if doc else 0)
            if full_md and page_count > 0:
                resplit = self._split_marker_md_by_anchors(doc, full_md, page_count)
                if resplit:
                    result["pages"] = resplit

    # ------------------------------------------------------------------
    # Segment-level block extraction (MasterPlan core)
    # ------------------------------------------------------------------

    def _extract_segment_blocks(
        self,
        source_path: Path,
        page_number: int,
        segment: PageSegment,
        provider_chain: list[str],
        best_provider: str,
    ) -> list[DocumentBlock]:
        """Extract blocks for a specific segment using the best provider first,
        then fallback chain. Filters blocks by segment bbox overlap."""
        ordered_providers = [best_provider] + [p for p in provider_chain if p != best_provider]

        for provider_name in ordered_providers:
            cache_key = f"{provider_name}:{source_path}"
            cached = self._engine_cache.results.get(cache_key, {})
            pages = cached.get("pages", {})
            page_data = pages.get(page_number, {})
            if not page_data or not page_data.get("blocks"):
                continue

            converter = _RESPONSE_TO_BLOCKS.get(provider_name, worker_response_to_blocks)
            all_blocks = converter(page_data)

            seg_blocks = self._filter_blocks_by_bbox(all_blocks, segment.bbox)
            if seg_blocks:
                return seg_blocks

        return []

    @staticmethod
    def _filter_blocks_by_bbox(
        blocks: list[DocumentBlock],
        seg_bbox: tuple[float, float, float, float],
        overlap_threshold: float = 0.3,
    ) -> list[DocumentBlock]:
        """Return blocks whose bbox overlaps substantially with the segment bbox."""
        sx0, sy0, sx1, sy1 = seg_bbox
        seg_area = max(0.0, sx1 - sx0) * max(0.0, sy1 - sy0)
        if seg_area <= 0:
            return blocks

        matched: list[DocumentBlock] = []
        for block in blocks:
            bx0, by0, bx1, by1 = block.bbox.x0, block.bbox.y0, block.bbox.x1, block.bbox.y1
            ix0 = max(sx0, bx0)
            iy0 = max(sy0, by0)
            ix1 = min(sx1, bx1)
            iy1 = min(sy1, by1)
            inter = max(0.0, ix1 - ix0) * max(0.0, iy1 - iy0)
            block_area = max(0.0, bx1 - bx0) * max(0.0, by1 - by0)
            if block_area <= 0:
                continue
            overlap = inter / block_area
            if overlap >= overlap_threshold:
                matched.append(block)
        return matched

    # ------------------------------------------------------------------
    # Legacy page-level block retrieval (fallback)
    # ------------------------------------------------------------------

    def _get_page_blocks_multi(
        self, source_path: Path, page_number: int, provider_chain: list[str],
    ) -> list[DocumentBlock]:
        all_blocks: list[list[DocumentBlock]] = []

        for provider_name in provider_chain:
            cache_key = f"{provider_name}:{source_path}"
            cached = self._engine_cache.results.get(cache_key, {})
            pages = cached.get("pages", {})
            page_data = pages.get(page_number, {})
            if not page_data or not page_data.get("blocks"):
                continue

            converter = _RESPONSE_TO_BLOCKS.get(provider_name, worker_response_to_blocks)
            blocks = converter(page_data)

            if blocks:
                all_blocks.append(blocks)

        if all_blocks:
            return merge_blocks(all_blocks)
        return []

    def _pick_best_full_markdown(
        self, source_path: Path, engines: list[str],
    ) -> str | None:
        priority = ["marker"] + [e for e in engines if e != "marker"]
        for eng in priority:
            cache_key = f"{eng}:{source_path}"
            md = self._engine_cache.results.get(cache_key, {}).get("markdown", "")
            if md and len(md.strip()) > 100:
                return md
        return None

    _VALID_IMAGE_HEADERS: dict[bytes, tuple[str, str]] = {  # noqa: RUF012
        b"\xff\xd8": ("image/jpeg", ".jpeg"),
        b"\x89P": ("image/png", ".png"),
    }

    def _store_marker_images(
        self, source_path: Path, asset_mgr: AssetManager,
    ) -> dict[str, str]:
        """Extract Marker's rendered images from cache, store as assets,
        return a mapping {original_key -> 'assets/<hash>.ext'}.
        Supports both base64 (small docs) and disk-based (chunked large docs)."""
        import base64
        cache_key = f"marker:{source_path}"
        cached = self._engine_cache.results.get(cache_key, {})
        rewrite_map: dict[str, str] = {}

        images_b64: dict[str, str] = cached.get("images_b64", {})
        for img_key, b64_data in images_b64.items():
            try:
                img_bytes = base64.b64decode(b64_data)
                if len(img_bytes) < 200:
                    continue
                header2 = img_bytes[:2]
                detected = self._VALID_IMAGE_HEADERS.get(header2)
                if not detected:
                    log.warning("marker image %s has invalid header %s, skipping", img_key, header2.hex())
                    continue
                mime, ext = detected
                ref = asset_mgr.store(img_bytes, f"marker-{img_key}", mime_type=mime, ext=ext)
                rewrite_map[img_key] = ref.rel_path
            except Exception:
                log.debug("failed to store marker b64 image %s", img_key, exc_info=True)
                continue

        images_on_disk: dict[str, str] = cached.get("images_on_disk", {})
        for img_key, img_path_str in images_on_disk.items():
            try:
                img_path = Path(img_path_str)
                if not img_path.exists():
                    continue
                img_bytes = img_path.read_bytes()
                if len(img_bytes) < 200:
                    continue
                header2 = img_bytes[:2]
                detected = self._VALID_IMAGE_HEADERS.get(header2)
                if not detected:
                    continue
                mime, ext = detected
                ref = asset_mgr.store(img_bytes, f"marker-{img_key}", mime_type=mime, ext=ext)
                rewrite_map[img_key] = ref.rel_path
                img_path.unlink(missing_ok=True)
            except Exception:
                log.debug("failed to store marker disk image %s", img_key, exc_info=True)
                continue

        return rewrite_map

    _img_extract_fail_count: int = 0

    def _store_pymupdf_page_images(
        self, doc: Any, page_number: int, asset_mgr: AssetManager,
    ) -> list[tuple[str, BoundingBox]]:
        """Extract all images from a page via PyMuPDF, store as assets,
        return list of (asset_rel_path, bbox)."""
        stored: list[tuple[str, BoundingBox]] = []
        try:
            page = doc.load_page(page_number - 1)
            for img_idx, img_info in enumerate(page.get_images(full=True)):
                xref = img_info[0]
                try:
                    pix = fitz.Pixmap(doc, xref)

                    color_channels = pix.n - pix.alpha
                    if color_channels not in (1, 3):
                        try:
                            pix = fitz.Pixmap(fitz.csRGB, pix)
                        except Exception:
                            self._img_extract_fail_count += 1
                            continue

                    if pix.alpha:
                        pix = fitz.Pixmap(pix, 0)
                    if pix.width < 20 or pix.height < 20:
                        continue

                    img_bytes: bytes | None = None
                    mime, ext = "image/png", ".png"
                    try:
                        img_bytes = pix.tobytes("png")
                    except Exception:
                        try:
                            rgb = fitz.Pixmap(fitz.csRGB, pix) if pix.n != 3 else pix
                            img_bytes = rgb.tobytes("jpeg")
                            mime, ext = "image/jpeg", ".jpeg"
                        except Exception:
                            self._img_extract_fail_count += 1
                            continue

                    if not img_bytes or len(img_bytes) < 200:
                        continue
                    ref = asset_mgr.store(
                        img_bytes, f"p{page_number}-img{img_idx}",
                        mime_type=mime, ext=ext,
                    )
                    stored.append((ref.rel_path, BoundingBox(
                        x0=0.0, y0=0.0, x1=float(pix.width), y1=float(pix.height),
                    )))
                except Exception:
                    self._img_extract_fail_count += 1
                    continue
        except Exception:
            log.debug("image extraction failed for page %d", page_number)
        return stored

    @staticmethod
    def _rewrite_figure_block_assets(
        page_results: list[PageResult], rewrite_map: dict[str, str],
    ) -> None:
        """Update raw_payload.asset_path in FIGURE blocks using the rewrite map.

        Also catches figure blocks where raw_payload was not set but the text
        contains an image markdown reference (e.g. ``![](_page_6_Picture_0.jpeg)``).
        """
        import re as _re
        img_pat = _re.compile(r"!\[[^\]]*\]\(([^)]+)\)")

        for page in page_results:
            for block in page.blocks:
                if block.type != BlockType.FIGURE:
                    continue

                old_path: str | None = None
                if block.raw_payload and block.raw_payload.get("asset_path"):
                    old_path = block.raw_payload["asset_path"]
                else:
                    m = img_pat.search(block.text)
                    if m:
                        old_path = m.group(1)
                        if block.raw_payload is None:
                            block.raw_payload = {}
                        block.raw_payload["asset_path"] = old_path

                if not old_path:
                    continue

                if old_path in rewrite_map:
                    block.raw_payload["asset_path"] = rewrite_map[old_path]  # type: ignore[index]
                    block.text = block.text.replace(old_path, rewrite_map[old_path])
                    continue
                for key, asset_path in rewrite_map.items():
                    if key in old_path or old_path in key:
                        block.raw_payload["asset_path"] = asset_path  # type: ignore[index]
                        block.text = block.text.replace(old_path, asset_path)
                        break

    @staticmethod
    def _rewrite_image_links(markdown: str, rewrite_map: dict[str, str]) -> str:
        """Replace dangling image references with actual asset paths."""
        import re as _re
        def _replacer(m: _re.Match[str]) -> str:
            alt = m.group(1)
            original_path = m.group(2)
            if original_path in rewrite_map:
                return f"![{alt}]({rewrite_map[original_path]})"
            for key, asset_path in rewrite_map.items():
                if key in original_path or original_path in key:
                    return f"![{alt}]({asset_path})"
            return m.group(0)
        return _re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", _replacer, markdown)

    @staticmethod
    def _text_to_words(text: str) -> set[str]:
        return {w for w in re.findall(r"[a-z0-9]{3,}", text.lower()) if len(w) >= 3}

    def _split_marker_md_by_anchors(
        self,
        doc: Any,
        full_md: str,
        page_count: int,
    ) -> dict[int, dict[str, Any]]:
        from yaruk.engines.marker.worker import _parse_page_blocks

        page_words: list[set[str]] = []
        for pg_idx in range(page_count):
            page = doc.load_page(pg_idx)
            raw = page.get_text("text")
            page_words.append(self._text_to_words(raw))

        page_img_re = re.compile(r"_page_(\d+)_")

        paragraphs = re.split(r"\n{2,}", full_md)

        para_pages: list[int] = []
        last_page = 0
        for para in paragraphs:
            para_s = para.strip()
            if not para_s:
                para_pages.append(last_page)
                continue

            img_m = page_img_re.search(para_s)
            if img_m:
                detected = int(img_m.group(1))
                if 0 <= detected < page_count:
                    last_page = detected
                    para_pages.append(last_page)
                    continue

            words = self._text_to_words(para_s)
            if not words:
                para_pages.append(last_page)
                continue

            best_page = last_page
            best_score = 0
            search_start = max(0, last_page - 1)
            search_end = min(page_count, last_page + 4)
            for pg_idx in range(search_start, search_end):
                overlap = len(words & page_words[pg_idx])
                if overlap > best_score:
                    best_score = overlap
                    best_page = pg_idx

            if best_page >= last_page:
                last_page = best_page
            para_pages.append(last_page)

        page_paras: dict[int, list[str]] = {i: [] for i in range(page_count)}
        for para, pg_idx in zip(paragraphs, para_pages, strict=False):
            page_paras[pg_idx].append(para)

        pages: dict[int, dict[str, Any]] = {}
        for pg_idx in range(page_count):
            page_md = "\n\n".join(page_paras[pg_idx]).strip()
            page_num = pg_idx + 1
            blocks = _parse_page_blocks(page_md, page_num) if page_md else []
            pages[page_num] = {
                "page_number": page_num,
                "markdown": page_md,
                "blocks": blocks,
            }

        return pages

    def _pymupdf_fallback(self, doc: Any, page_number: int) -> list[DocumentBlock]:
        page = doc.load_page(page_number - 1)
        blocks_raw = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE).get("blocks", [])
        result: list[DocumentBlock] = []
        for idx, b in enumerate(blocks_raw):
            if b.get("type") == 0:
                lines_text = ""
                for line in b.get("lines", []):
                    for span in line.get("spans", []):
                        lines_text += span.get("text", "")
                    lines_text += "\n"
                lines_text = lines_text.strip()
                if not lines_text:
                    continue
                bbox = b.get("bbox", [0, 0, 0, 0])
                result.append(DocumentBlock(
                    page=page_number,
                    block_id=f"p{page_number}-pymupdf-b{idx}",
                    type=BlockType.PARAGRAPH,
                    text=lines_text,
                    bbox=BoundingBox(x0=bbox[0], y0=bbox[1], x1=bbox[2], y1=bbox[3]),
                    confidence=0.4,
                    source_provider="pymupdf",
                    source_version="fallback",
                    reading_order=idx,
                ))
        return result

    def _extract_metadata(self, source_path: Path) -> DocumentMetadata:
        doc = fitz.open(str(source_path))
        meta = doc.metadata or {}
        doc.close()
        return DocumentMetadata(
            title=meta.get("title") or None,
            author=meta.get("author") or None,
            subject=meta.get("subject") or None,
            keywords=[k.strip() for k in (meta.get("keywords") or "").split(",") if k.strip()],
        )
