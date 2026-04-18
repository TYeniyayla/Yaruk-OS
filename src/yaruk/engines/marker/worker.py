from __future__ import annotations

import gc
import logging
import re
import tempfile
from pathlib import Path
from typing import Any

from yaruk.engines.base_worker import run_worker_server

log = logging.getLogger(__name__)

try:
    from marker.converters.pdf import PdfConverter  # type: ignore[import-untyped]
    from marker.models import create_model_dict  # type: ignore[import-untyped]
    MARKER_AVAILABLE = True
except ImportError:
    MARKER_AVAILABLE = False

PAGE_SEPARATOR_PATTERN = re.compile(r"\n{0,2}-{48}\n{0,2}")

CHUNK_SIZE = 200


def _classify_block(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("#"):
        return "heading"
    if stripped.startswith("$$") or stripped.startswith("\\["):
        return "equation"
    if stripped.startswith("```"):
        return "code"
    if stripped.startswith("|") and "|" in stripped[1:]:
        return "table"
    if stripped.startswith("!["):
        return "figure"
    if stripped.startswith("- ") or stripped.startswith("* ") or re.match(r"^\d+\.", stripped):
        return "list"
    return "paragraph"


def _heading_level(text: str) -> str:
    m = re.match(r"^(#{1,6})\s", text.strip())
    if m:
        return f"h{len(m.group(1))}"
    return "h2"


def _split_markdown_to_pages(markdown: str) -> list[str]:
    pages = PAGE_SEPARATOR_PATTERN.split(markdown)
    return [p.strip() for p in pages if p.strip()]


def _parse_page_blocks(
    page_md: str, page_number: int, images: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    paragraphs = re.split(r"\n{2,}", page_md)

    for idx, para in enumerate(paragraphs):
        para = para.strip()
        if not para:
            continue

        btype = _classify_block(para)
        text = para
        style: dict[str, Any] | None = None
        raw_payload: dict[str, Any] | None = None

        if btype == "heading":
            style = {"level": _heading_level(para)}
            text = re.sub(r"^#{1,6}\s*", "", para)

        if btype == "figure" and images:
            img_match = re.match(r"!\[([^\]]*)\]\(([^)]+)\)", para)
            if img_match:
                img_key = img_match.group(2)
                raw_payload = {"asset_path": img_key}
                text = img_match.group(1) or "figure"

        blocks.append({
            "page": page_number,
            "block_id": f"p{page_number}-marker-b{idx}",
            "type": btype,
            "text": text,
            "bbox": {"x0": 0.0, "y0": idx * 50.0, "x1": 612.0, "y1": (idx + 1) * 50.0},
            "confidence": 0.85,
            "source_provider": "marker",
            "source_version": "real",
            "schema_version": "v1",
            "reading_order": idx,
            **({"style": style} if style else {}),
            **({"raw_payload": raw_payload} if raw_payload else {}),
        })

    return blocks


class MarkerWorkerHandler:
    def __init__(self) -> None:
        self._converter: Any = None
        self._model_dict: Any = None
        self._cache: dict[str, dict[str, Any]] = {}

    def _ensure_loaded(self, page_range: range | None = None) -> None:
        if not MARKER_AVAILABLE:
            return
        if self._converter is not None:
            return

        import os
        import sys

        force_cpu = os.environ.get("MARKER_FORCE_CPU", "") == "1"

        try:
            import torch
            if not force_cpu and torch.cuda.is_available():
                dev = torch.cuda.get_device_name(0)
                vram = torch.cuda.get_device_properties(0).total_memory // (1024**2)
                print(f"[marker] GPU detected: {dev} ({vram} MB)", file=sys.stderr)
            else:
                reason = "MARKER_FORCE_CPU=1" if force_cpu else "no GPU"
                print(f"[marker] running on CPU ({reason})", file=sys.stderr)
                if force_cpu and torch.cuda.is_available():
                    os.environ["CUDA_VISIBLE_DEVICES"] = ""
        except ImportError:
            print("[marker] WARNING: torch not found, running on CPU", file=sys.stderr)

        self._model_dict = create_model_dict()
        config_overrides: dict[str, Any] = {}
        if page_range is not None:
            config_overrides["page_range"] = list(page_range)
        self._converter = PdfConverter(
            artifact_dict=self._model_dict,
            config=config_overrides if config_overrides else None,
        )

    def handle(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "health":
            return {"name": "marker", "ok": True, "real": MARKER_AVAILABLE}

        if method == "convert_full":
            pdf_path = params.get("pdf_path", "")
            mp = params.get("max_pages")
            return self._convert_full_cached(Path(pdf_path), max_pages=int(mp) if mp else None)

        if method == "get_page":
            pdf_path = params.get("pdf_path", "")
            page_number = int(params.get("page_number", 1))
            return self._get_page_from_cache(Path(pdf_path), page_number)

        if method == "analyze_page":
            pdf_path = params.get("pdf_path")
            if pdf_path and MARKER_AVAILABLE and Path(pdf_path).exists():
                return self._get_page_from_cache(
                    Path(pdf_path), int(params.get("page_number", 1)),
                )
            return self._analyze_page_fallback(params)

        raise ValueError(f"unknown method: {method}")

    def _convert_full_cached(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        key = str(pdf_path)
        if key in self._cache:
            return self._cache[key]
        result = self._convert_full(pdf_path, max_pages=max_pages)
        self._cache[key] = result
        return result

    def _convert_full(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        if not MARKER_AVAILABLE:
            return {"error": "marker not installed", "pages": {}, "markdown": ""}

        import fitz
        pdf_doc = fitz.open(str(pdf_path))
        total_pages = min(pdf_doc.page_count, max_pages or pdf_doc.page_count)
        pdf_doc.close()

        if total_pages <= CHUNK_SIZE:
            return self._convert_chunk(pdf_path, 0, total_pages)

        return self._convert_chunked(pdf_path, total_pages)

    def _convert_chunked(self, pdf_path: Path, total_pages: int) -> dict[str, Any]:
        """Process large PDFs in chunks to bound memory and pipe-transfer size."""
        import sys

        all_pages: dict[int, dict[str, Any]] = {}
        all_md_parts: list[tuple[int, str]] = []
        images_on_disk: dict[str, str] = {}

        img_dir = Path(tempfile.mkdtemp(prefix="yaruk_marker_imgs_"))
        n_chunks = (total_pages + CHUNK_SIZE - 1) // CHUNK_SIZE

        for chunk_idx, chunk_start in enumerate(range(0, total_pages, CHUNK_SIZE)):
            chunk_end = min(chunk_start + CHUNK_SIZE, total_pages)
            print(
                f"[marker] chunk {chunk_idx+1}/{n_chunks}: pages {chunk_start+1}-{chunk_end}",
                file=sys.stderr,
            )

            self._converter = None
            self._model_dict = None
            gc.collect()

            chunk_result = self._convert_chunk(pdf_path, chunk_start, chunk_end)

            for img_key, b64_data in chunk_result.get("images_b64", {}).items():
                import base64
                img_bytes = base64.b64decode(b64_data)
                safe_name = img_key.replace("/", "_").replace("\\", "_")
                img_path = img_dir / safe_name
                img_path.write_bytes(img_bytes)
                images_on_disk[img_key] = str(img_path)

            for page_num_key, page_data in chunk_result.get("pages", {}).items():
                page_num = int(page_num_key)
                if page_num in all_pages:
                    print(
                        f"[marker] WARNING: duplicate page {page_num} from chunk {chunk_idx+1}, "
                        f"keeping first occurrence",
                        file=sys.stderr,
                    )
                    continue
                all_pages[page_num] = page_data

            chunk_md = chunk_result.get("markdown", "")
            if chunk_md:
                all_md_parts.append((chunk_start, chunk_md))

            del chunk_result
            gc.collect()

        self._converter = None
        self._model_dict = None
        gc.collect()

        all_md_parts.sort(key=lambda x: x[0])
        merged_md = "\n\n---\n\n".join(md for _, md in all_md_parts)

        return {
            "pages": all_pages,
            "page_count": len(all_pages),
            "markdown": merged_md,
            "images_b64": {},
            "images_on_disk": images_on_disk,
        }

    def _convert_chunk(self, pdf_path: Path, start: int, end: int) -> dict[str, Any]:
        """Convert a page range [start, end) and return results.

        If a CUDA OOM occurs, retry once on CPU after clearing VRAM.
        """
        page_range = range(start, end)
        try:
            return self._convert_chunk_inner(pdf_path, start, end, page_range)
        except (RuntimeError, MemoryError) as exc:
            err_str = str(exc).lower()
            if "cuda" not in err_str and "memory" not in err_str and "allocat" not in err_str:
                raise
            import sys
            print(
                f"[marker] CUDA OOM on pages {start+1}-{end}, retrying on CPU",
                file=sys.stderr,
            )
            self._converter = None
            self._model_dict = None
            gc.collect()
            try:
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
            except ImportError:
                pass
            import os
            os.environ["MARKER_FORCE_CPU"] = "1"
            return self._convert_chunk_inner(pdf_path, start, end, page_range)

    def _convert_chunk_inner(
        self, pdf_path: Path, start: int, end: int, page_range: range,
    ) -> dict[str, Any]:
        self._ensure_loaded(page_range=page_range)
        rendered = self._converter(str(pdf_path))
        full_md = rendered.markdown if hasattr(rendered, "markdown") else str(rendered)
        images = rendered.images if hasattr(rendered, "images") else {}

        import base64
        import io
        images_b64: dict[str, str] = {}
        for img_key, img_obj in images.items():
            try:
                if isinstance(img_obj, bytes):
                    if len(img_obj) > 2 and img_obj[:2] in (b"\xff\xd8", b"\x89P"):
                        images_b64[img_key] = base64.b64encode(img_obj).decode("ascii")
                    continue

                is_jpeg_key = img_key.lower().endswith((".jpeg", ".jpg"))
                buf = io.BytesIO()
                if hasattr(img_obj, "save"):
                    pil_img = img_obj
                    if is_jpeg_key and pil_img.mode in ("RGBA", "P", "LA"):
                        pil_img = pil_img.convert("RGB")
                    fmt = "JPEG" if is_jpeg_key else "PNG"
                    pil_img.save(buf, format=fmt, quality=92)
                    encoded = buf.getvalue()
                    if len(encoded) > 100:
                        images_b64[img_key] = base64.b64encode(encoded).decode("ascii")
            except Exception:
                log.debug("failed to encode marker image %s", img_key, exc_info=True)

        page_markdowns = _split_markdown_to_pages(full_md)
        pages: dict[int, dict[str, Any]] = {}
        for page_idx, page_md in enumerate(page_markdowns):
            page_num = start + page_idx + 1
            blocks = _parse_page_blocks(page_md, page_num, images)
            pages[page_num] = {
                "page_number": page_num,
                "markdown": page_md,
                "blocks": blocks,
            }

        return {
            "pages": pages,
            "page_count": len(pages),
            "markdown": full_md,
            "images_b64": images_b64,
        }

    def _get_page_from_cache(self, pdf_path: Path, page_number: int) -> dict[str, Any]:
        full = self._convert_full_cached(pdf_path)
        pages = full.get("pages", {})
        if page_number in pages:
            return pages[page_number]
        return {
            "page_number": page_number,
            "blocks": [],
            "markdown": "",
        }

    def _analyze_page_fallback(self, params: dict[str, Any]) -> dict[str, Any]:
        page_number = int(params.get("page_number", 1))
        text = params.get("text", "")
        blocks: list[dict[str, Any]] = []
        if text:
            paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
            for idx, para in enumerate(paragraphs):
                btype = _classify_block(para)
                blocks.append({
                    "page": page_number,
                    "block_id": f"p{page_number}-b{idx}",
                    "type": btype,
                    "text": para,
                    "bbox": {"x0": 0.0, "y0": idx * 50.0, "x1": 612.0, "y1": (idx + 1) * 50.0},
                    "confidence": 0.5,
                    "source_provider": "marker",
                    "source_version": "fallback",
                    "schema_version": "v1",
                    "reading_order": idx,
                })
        if not blocks:
            blocks.append({
                "page": page_number,
                "block_id": f"p{page_number}-b0",
                "type": "paragraph",
                "text": text or "",
                "bbox": {"x0": 0.0, "y0": 0.0, "x1": 612.0, "y1": 792.0},
                "confidence": 0.3,
                "source_provider": "marker",
                "source_version": "fallback",
                "schema_version": "v1",
                "reading_order": 0,
            })
        return {"page_number": page_number, "blocks": blocks}


def main() -> None:
    run_worker_server(MarkerWorkerHandler())


if __name__ == "__main__":
    main()
