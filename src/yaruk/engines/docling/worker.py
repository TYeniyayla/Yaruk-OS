from __future__ import annotations

import logging
import os
import tempfile
import warnings
from pathlib import Path
from typing import Any

import fitz  # type: ignore[import-untyped]

from yaruk.engines.base_worker import run_worker_server

log = logging.getLogger(__name__)

logging.getLogger("RapidOCR").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", message=".*text detection result is empty.*")

DOCLING_AVAILABLE = False
_DOCLING_VERSION = "0.0.0"

try:
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    DOCLING_AVAILABLE = True
    try:
        import importlib.metadata as _meta
        _DOCLING_VERSION = _meta.version("docling")
    except Exception:
        _DOCLING_VERSION = "2.88.0"
except ImportError:
    pass


_LABEL_MAP: dict[str, str] = {
    "text": "paragraph",
    "paragraph": "paragraph",
    "section_header": "heading",
    "title": "heading",
    "table": "table",
    "figure": "figure",
    "picture": "figure",
    "caption": "caption",
    "list_item": "list",
    "formula": "equation",
    "page_header": "header",
    "page_footer": "footer",
    "code": "code",
}


def _label_to_block_type(label: Any) -> str:
    label_str = str(label).lower().replace("-", "_")
    for key, val in _LABEL_MAP.items():
        if key in label_str:
            return val
    return "paragraph"


def _extract_page_no(item: Any) -> int:
    """Map Docling provenance to 1-based page index (matches Yaruk page_number)."""
    if hasattr(item, "prov") and item.prov:
        for p in item.prov:
            if hasattr(p, "page_no"):
                pn = int(p.page_no)
                # Some builds used 0-based keys; doc.pages is usually 1..N today
                if pn == 0:
                    return 1
                return max(1, pn)
    return 1


def _extract_bbox(item: Any) -> dict[str, float]:
    if hasattr(item, "prov") and item.prov:
        for p in item.prov:
            bbox = getattr(p, "bbox", None)
            if bbox is not None:
                l_val = float(getattr(bbox, "l", None) or getattr(bbox, "x0", 0.0))
                t_val = float(getattr(bbox, "t", None) or getattr(bbox, "y0", 0.0))
                r_val = float(getattr(bbox, "r", None) or getattr(bbox, "x1", 612.0))
                b_val = float(getattr(bbox, "b", None) or getattr(bbox, "y1", 792.0))
                # Docling BoundingBox uses l,t,r,b (top/bottom per coord origin)
                return {"x0": l_val, "y0": t_val, "x1": r_val, "y1": b_val}
    return {"x0": 0.0, "y0": 0.0, "x1": 612.0, "y1": 792.0}



def _has_text_layer(pdf_path: Path, sample_pages: int = 3) -> bool:
    """Check if PDF already has an extractable text layer (no OCR needed)."""
    try:
        doc = fitz.open(str(pdf_path))
        check_count = min(doc.page_count, sample_pages)
        text_pages = 0
        for i in range(check_count):
            text = doc.load_page(i).get_text("text").strip()
            if len(text) > 50:
                text_pages += 1
        doc.close()
        return text_pages >= (check_count * 0.6)
    except Exception:
        return False


def _detect_cuda() -> bool:
    if os.environ.get("CUDA_VISIBLE_DEVICES") == "":
        return False
    try:
        import torch
        return torch.cuda.is_available()
    except ImportError:
        pass
    try:
        import onnxruntime
        return "CUDAExecutionProvider" in onnxruntime.get_available_providers()
    except ImportError:
        return False


def _build_converter(pdf_path: Path | None = None) -> Any:
    """Build a Docling DocumentConverter.

    If the PDF already has a text layer, OCR is skipped for speed.
    RapidOCR uses CUDA if available via onnxruntime-gpu.
    """
    os.environ.setdefault("HF_TOKEN", os.environ.get("HF_TOKEN", ""))

    skip_ocr = False
    if pdf_path is not None:
        skip_ocr = _has_text_layer(pdf_path)
        if skip_ocr:
            log.info("docling: text layer detected, skipping OCR for speed")

    has_cuda = _detect_cuda()

    from docling.datamodel.pipeline_options import AcceleratorOptions
    accel = AcceleratorOptions(device="cuda" if has_cuda else "cpu")

    import contextlib
    ocr_opts = None
    with contextlib.suppress(Exception):
        from docling.datamodel.pipeline_options import RapidOcrOptions
        if has_cuda:
            ocr_opts = RapidOcrOptions(
                use_cuda=True,
                use_gpu=True,
            )
            log.info("docling: RapidOCR CUDA enabled")

    pipeline_kwargs: dict[str, Any] = {
        "do_ocr": not skip_ocr,
        "generate_page_images": True,
        "images_scale": 2.0,
        "generate_picture_images": True,
        "accelerator_options": accel,
    }
    if ocr_opts is not None:
        pipeline_kwargs["ocr_options"] = ocr_opts

    # document_timeout must stay None (no limit). Setting 0 makes every conversion
    # abort immediately (0s timeout) — see docling PdfPipelineOptions.document_timeout.
    pipeline_options = PdfPipelineOptions(**pipeline_kwargs)

    return DocumentConverter(
        allowed_formats=[InputFormat.PDF, InputFormat.IMAGE],
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
        },
    )


class DoclingWorkerHandler:
    def __init__(self, *, is_subprocess: bool = False) -> None:
        self._converter: Any = None
        self._converter_pdf: str | None = None
        self._is_subprocess = is_subprocess

    def _ensure_loaded(self, pdf_path: Path | None = None) -> None:
        if not DOCLING_AVAILABLE:
            return
        pdf_key = str(pdf_path) if pdf_path else None
        if self._converter is not None and self._converter_pdf == pdf_key:
            return
        self._converter = _build_converter(pdf_path)
        self._converter_pdf = pdf_key

    def handle(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "health":
            return {"name": "docling", "ok": DOCLING_AVAILABLE, "version": _DOCLING_VERSION}

        if method == "convert_full":
            pdf_path = params.get("pdf_path", "")
            max_pages = params.get("max_pages")
            return self._convert_full(
                Path(pdf_path),
                max_pages=int(max_pages) if max_pages else None,
            )

        if method == "get_page":
            pdf_path = params.get("pdf_path", "")
            page_number = int(params.get("page_number", 1))
            full = self._convert_full(Path(pdf_path))
            page_data = full.get("pages", {}).get(page_number, {})
            return page_data or {"page_number": page_number, "blocks": [], "markdown": ""}

        raise ValueError(f"unknown method: {method}")

    def _convert_full(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        if not DOCLING_AVAILABLE:
            return {"error": "docling not installed", "pages": {}, "markdown": ""}

        if not pdf_path.exists():
            return {"error": f"file not found: {pdf_path}", "pages": {}, "markdown": ""}

        self._ensure_loaded(pdf_path)

        result = self._try_pdf_conversion(pdf_path, max_pages)
        if result.get("error"):
            log.info(
                "docling PDF conversion failed, trying page-by-page image fallback: %s",
                result["error"][:120],
            )
            result = self._try_image_fallback(pdf_path, max_pages)

        return result

    def _try_pdf_conversion(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        try:
            convert_kwargs: dict[str, Any] = {}
            if max_pages:
                convert_kwargs["max_num_pages"] = max_pages

            conv_result = self._converter.convert(pdf_path, **convert_kwargs)
            return self._extract_from_docling_result(conv_result)

        except Exception as e:
            err_str = str(e).lower()

            is_oom = "cuda" in err_str and ("out of memory" in err_str or "alloc" in err_str)
            if is_oom:
                import sys
                print(
                    f"docling PDF conversion failed: {str(e)[:200]}",
                    file=sys.stderr, flush=True,
                )
                if self._is_subprocess:
                    return {"error": f"cuda_oom: {str(e)[:300]}", "pages": {}, "markdown": ""}
                return self._retry_on_cpu(pdf_path, max_pages)

            if "not valid" in err_str or "invalid" in err_str:
                log.info(
                    "docling reports document not valid — attempting repair via PyMuPDF save/reload",
                )
                repaired = self._repair_pdf(pdf_path)
                if repaired:
                    try:
                        kw: dict[str, Any] = {}
                        if max_pages:
                            kw["max_num_pages"] = max_pages
                        conv_result = self._converter.convert(repaired, **kw)
                        return self._extract_from_docling_result(conv_result)
                    except Exception as e2:
                        log.warning("docling repaired-PDF conversion also failed: %s", str(e2)[:200])
                        return {"error": str(e2)[:500], "pages": {}, "markdown": ""}
            log.warning("docling PDF conversion failed: %s", str(e)[:200])
            return {"error": str(e)[:500], "pages": {}, "markdown": ""}

    def _retry_on_cpu(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        """Rebuild Docling converter on CPU and retry after CUDA OOM."""
        import sys
        print("[docling] CUDA OOM detected — rebuilding converter on CPU and retrying", file=sys.stderr, flush=True)
        try:
            import gc
            gc.collect()
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except ImportError:
                pass
            os.environ["CUDA_VISIBLE_DEVICES"] = ""
            self._converter = _build_converter(pdf_path)
            self._converter_pdf = str(pdf_path)

            convert_kwargs: dict[str, Any] = {}
            if max_pages:
                convert_kwargs["max_num_pages"] = max_pages
            conv_result = self._converter.convert(pdf_path, **convert_kwargs)
            return self._extract_from_docling_result(conv_result)
        except Exception as e2:
            log.warning("docling CPU retry also failed: %s", str(e2)[:200])
            return {"error": str(e2)[:500], "pages": {}, "markdown": ""}

    @staticmethod
    def _repair_pdf(pdf_path: Path) -> Path | None:
        """Attempt to 'repair' a PDF by re-saving it through PyMuPDF."""
        try:
            repaired = pdf_path.with_suffix(".repaired.pdf")
            doc = fitz.open(str(pdf_path))
            doc.save(
                str(repaired),
                garbage=4,
                clean=True,
                deflate=True,
            )
            doc.close()
            log.info("docling: repaired PDF saved to %s (%d KB)", repaired.name, repaired.stat().st_size // 1024)
            return repaired
        except Exception as exc:
            log.warning("docling: PDF repair failed: %s", str(exc)[:120])
            return None

    def _try_image_fallback(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        """Render each PDF page as an image and convert via Docling's image pipeline."""
        try:
            doc = fitz.open(str(pdf_path))
            page_count = min(doc.page_count, max_pages or doc.page_count)

            all_pages: dict[int, dict[str, Any]] = {}
            all_md_parts: list[str] = []
            success_count = 0

            tmp_dir = tempfile.mkdtemp(prefix="docling_img_")

            for pg_idx in range(page_count):
                page_num = pg_idx + 1
                if page_num % 10 == 1 or page_num == page_count:
                    import sys
                    print(
                        f"[docling] image fallback: page {page_num}/{page_count}",
                        file=sys.stderr, flush=True,
                    )
                try:
                    page = doc.load_page(pg_idx)
                    mat = fitz.Matrix(2.0, 2.0)
                    pix = page.get_pixmap(matrix=mat)
                    img_path = Path(tmp_dir) / f"page_{page_num:04d}.png"
                    pix.save(str(img_path))

                    conv_result = self._converter.convert(img_path)
                    page_result = self._extract_single_page(conv_result, page_num)

                    if page_result["blocks"]:
                        all_pages[page_num] = page_result
                        all_md_parts.append(page_result.get("markdown", ""))
                        success_count += 1
                    else:
                        all_pages[page_num] = {
                            "page_number": page_num,
                            "markdown": "",
                            "blocks": [],
                        }
                except Exception as e:
                    log.debug("docling image fallback page %d failed: %s", page_num, str(e)[:100])
                    all_pages[page_num] = {
                        "page_number": page_num,
                        "markdown": "",
                        "blocks": [],
                    }

            doc.close()

            if success_count == 0:
                return {"error": "image fallback produced no results", "pages": {}, "markdown": ""}

            return {
                "pages": all_pages,
                "page_count": len(all_pages),
                "markdown": "\n\n---\n\n".join(all_md_parts),
            }

        except Exception as e:
            log.warning("docling image fallback failed: %s", str(e)[:200])
            return {"error": str(e)[:500], "pages": {}, "markdown": ""}

    def _extract_from_docling_result(self, conv_result: Any) -> dict[str, Any]:
        doc = conv_result.document

        full_md = (
            doc.export_to_markdown()
            if hasattr(doc, "export_to_markdown")
            else str(doc)
        )

        page_blocks: dict[int, list[dict[str, Any]]] = {}
        global_idx = 0

        for item, _level in doc.iterate_items():
            text = getattr(item, "text", "") or ""
            if len(text.strip()) < 2:
                continue

            label = getattr(item, "label", "paragraph")
            btype = _label_to_block_type(label)
            page_no = _extract_page_no(item)
            bbox = _extract_bbox(item)

            block = {
                "page": page_no,
                "block_id": f"p{page_no}-docling-b{global_idx}",
                "type": btype,
                "text": text[:2000],
                "bbox": bbox,
                "confidence": 0.92 if btype in ("table", "equation") else 0.88,
                "source_provider": "docling",
                "source_version": _DOCLING_VERSION,
                "schema_version": "v1",
                "reading_order": global_idx,
            }
            page_blocks.setdefault(page_no, []).append(block)
            global_idx += 1

        page_count = len(doc.pages) if hasattr(doc, "pages") else max(page_blocks.keys(), default=0)

        pages: dict[int, dict[str, Any]] = {}
        md_sections = full_md.split("\n\n---\n\n") if full_md else []

        for pg_num in sorted(page_blocks.keys()):
            pg_blocks = page_blocks[pg_num]
            for local_idx, b in enumerate(pg_blocks):
                b["reading_order"] = local_idx

            page_md = md_sections[pg_num - 1] if pg_num <= len(md_sections) else ""
            if not page_md:
                page_md = "\n\n".join(b["text"] for b in pg_blocks)

            pages[pg_num] = {
                "page_number": pg_num,
                "markdown": page_md,
                "blocks": pg_blocks,
            }

        if not pages:
            log.warning(
                "docling _extract_from_docling_result: no blocks (iterate_items empty or "
                "all text stripped); page_count_hint=%s md_len=%d",
                page_count,
                len(full_md or ""),
            )

        return {
            "pages": pages,
            "page_count": page_count,
            "markdown": full_md,
        }

    def _extract_single_page(self, conv_result: Any, page_num: int) -> dict[str, Any]:
        """Extract blocks from a single-page Docling conversion (image input)."""
        doc = conv_result.document
        blocks: list[dict[str, Any]] = []
        idx = 0

        for item, _level in doc.iterate_items():
            text = getattr(item, "text", "") or ""
            if len(text.strip()) < 2:
                continue

            label = getattr(item, "label", "paragraph")
            btype = _label_to_block_type(label)
            bbox = _extract_bbox(item)

            blocks.append({
                "page": page_num,
                "block_id": f"p{page_num}-docling-b{idx}",
                "type": btype,
                "text": text[:2000],
                "bbox": bbox,
                "confidence": 0.90 if btype in ("table", "equation") else 0.85,
                "source_provider": "docling",
                "source_version": _DOCLING_VERSION,
                "schema_version": "v1",
                "reading_order": idx,
            })
            idx += 1

        md = (
            doc.export_to_markdown()
            if hasattr(doc, "export_to_markdown")
            else "\n\n".join(b["text"] for b in blocks)
        )

        return {
            "page_number": page_num,
            "markdown": md,
            "blocks": blocks,
        }


def main() -> None:
    run_worker_server(DoclingWorkerHandler(is_subprocess=True))


if __name__ == "__main__":
    main()
