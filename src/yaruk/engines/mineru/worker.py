from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

from yaruk.engines.base_worker import run_worker_server

log = logging.getLogger(__name__)

MINERU_AVAILABLE = False
_MINERU_VERSION = "0.0.0"

try:
    from magic_pdf.data.data_reader_writer import FileBasedDataWriter
    from magic_pdf.data.dataset import PymuDocDataset

    MINERU_AVAILABLE = True
    try:
        import importlib.metadata as _meta
        _MINERU_VERSION = _meta.version("magic-pdf")
    except Exception:
        _MINERU_VERSION = "1.3.12"
except ImportError:
    pass


def _classify_block(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("$$") or stripped.startswith("\\["):
        return "equation"
    if stripped.startswith("#"):
        return "heading"
    if stripped.startswith("|") and "|" in stripped[1:]:
        return "table"
    if stripped.startswith("!["):
        return "figure"
    if stripped.startswith("- ") or stripped.startswith("* "):
        return "list"
    return "paragraph"


def _safe_doc_analyze() -> Any:
    """Import doc_analyze, returning None if models are unavailable."""
    try:
        from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
        return doc_analyze
    except Exception as e:
        log.debug("doc_analyze import failed (models may be missing): %s", str(e)[:100])
        return None


class MinerUWorkerHandler:
    def handle(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "health":
            return {
                "name": "mineru",
                "ok": MINERU_AVAILABLE,
                "version": _MINERU_VERSION,
            }

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
            return full.get("pages", {}).get(
                page_number, {"page_number": page_number, "blocks": [], "markdown": ""},
            )

        raise ValueError(f"unknown method: {method}")

    def _convert_full(
        self, pdf_path: Path, max_pages: int | None = None,
    ) -> dict[str, Any]:
        if not MINERU_AVAILABLE:
            return {"error": "magic-pdf not installed", "pages": {}, "markdown": ""}

        if not pdf_path.exists():
            return {"error": f"file not found: {pdf_path}", "pages": {}, "markdown": ""}

        result = self._try_with_models(pdf_path, max_pages)
        if result is not None:
            return result

        result = self._try_txt_mode_only(pdf_path, max_pages)
        if result is not None:
            return result

        return {"error": "all MinerU conversion strategies failed", "pages": {}, "markdown": ""}

    def _try_with_models(self, pdf_path: Path, max_pages: int | None) -> dict[str, Any] | None:
        """Full pipeline with layout detection + OCR models."""
        doc_analyze = _safe_doc_analyze()
        if doc_analyze is None:
            return None

        try:
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()

            ds = PymuDocDataset(pdf_bytes)
            method = ds.classify()

            end_page = max_pages if max_pages else None
            infer_result = ds.apply(
                doc_analyze,
                ocr=method.value == "ocr",
                start_page_id=0,
                end_page_id=end_page,
            )

            out_dir = tempfile.mkdtemp(prefix="mineru_")
            writer = FileBasedDataWriter(out_dir)

            if method.value == "txt":
                pipe_result = infer_result.pipe_txt_mode(writer)
            else:
                pipe_result = infer_result.pipe_ocr_mode(writer)

            full_md = pipe_result.get_markdown()
            content_list = pipe_result.get_content_list()

            pages = self._split_to_pages(full_md, content_list, max_pages)

            return {
                "pages": pages,
                "page_count": len(pages),
                "markdown": full_md,
            }

        except Exception as e:
            log.info("mineru full model pipeline failed: %s", str(e)[:200])
            return None

    def _try_txt_mode_only(self, pdf_path: Path, max_pages: int | None) -> dict[str, Any] | None:
        """Lightweight text-only extraction without layout models (bypasses OCR model issues)."""
        try:
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()

            ds = PymuDocDataset(pdf_bytes)

            out_dir = tempfile.mkdtemp(prefix="mineru_txt_")
            writer = FileBasedDataWriter(out_dir)

            doc_analyze = _safe_doc_analyze()
            if doc_analyze is not None:
                try:
                    infer_result = ds.apply(
                        doc_analyze,
                        ocr=False,
                        start_page_id=0,
                        end_page_id=max_pages,
                    )
                    pipe_result = infer_result.pipe_txt_mode(writer)
                    full_md = pipe_result.get_markdown()
                    content_list = pipe_result.get_content_list()
                    pages = self._split_to_pages(full_md, content_list, max_pages)

                    if pages:
                        return {
                            "pages": pages,
                            "page_count": len(pages),
                            "markdown": full_md,
                        }
                except Exception as e:
                    log.debug("mineru txt mode with doc_analyze failed: %s", str(e)[:150])

            try:
                import fitz  # type: ignore[import-untyped]
                doc = fitz.open(str(pdf_path))
                page_count = min(doc.page_count, max_pages or doc.page_count)

                pages: dict[int, dict[str, Any]] = {}
                md_parts: list[str] = []

                for pg_idx in range(page_count):
                    page_num = pg_idx + 1
                    page = doc.load_page(pg_idx)
                    blocks_raw = page.get_text("dict", flags=0).get("blocks", [])

                    blocks: list[dict[str, Any]] = []
                    for idx, b in enumerate(blocks_raw):
                        if b.get("type") != 0:
                            continue
                        lines_text = ""
                        for line in b.get("lines", []):
                            for span in line.get("spans", []):
                                lines_text += span.get("text", "")
                            lines_text += "\n"
                        lines_text = lines_text.strip()
                        if not lines_text:
                            continue

                        btype = _classify_block(lines_text)
                        bbox = b.get("bbox", [0, 0, 612, 792])
                        blocks.append({
                            "page": page_num,
                            "block_id": f"p{page_num}-mineru-b{idx}",
                            "type": btype,
                            "text": lines_text[:2000],
                            "bbox": {
                                "x0": bbox[0], "y0": bbox[1],
                                "x1": bbox[2], "y1": bbox[3],
                            },
                            "confidence": 0.75,
                            "source_provider": "mineru",
                            "source_version": _MINERU_VERSION,
                            "schema_version": "v1",
                            "reading_order": idx,
                        })
                        md_parts.append(lines_text)

                    page_md = "\n\n".join(b["text"] for b in blocks)
                    pages[page_num] = {
                        "page_number": page_num,
                        "markdown": page_md,
                        "blocks": blocks,
                    }

                doc.close()

                if pages:
                    return {
                        "pages": pages,
                        "page_count": len(pages),
                        "markdown": "\n\n---\n\n".join(
                            p["markdown"] for p in pages.values() if p["markdown"]
                        ),
                    }
            except Exception as e:
                log.debug("mineru pymupdf fallback failed: %s", str(e)[:150])

            return None

        except Exception as e:
            log.warning("mineru txt-mode conversion failed: %s", str(e)[:200])
            return None

    def _split_to_pages(
        self,
        full_md: str,
        content_list: list[Any],
        max_pages: int | None = None,
    ) -> dict[int, dict[str, Any]]:
        page_contents: dict[int, list[dict[str, Any]]] = {}

        for item in content_list:
            if not isinstance(item, dict):
                continue
            page_idx = item.get("page_idx", 0)
            page_num = page_idx + 1
            page_contents.setdefault(page_num, []).append(item)

        if not page_contents and full_md:
            page_contents[1] = [{"type": "text", "text": full_md}]

        pages: dict[int, dict[str, Any]] = {}
        for page_num in sorted(page_contents.keys()):
            items = page_contents[page_num]
            blocks: list[dict[str, Any]] = []
            md_parts: list[str] = []

            for idx, item in enumerate(items):
                text = item.get("text", "") or ""
                if not text.strip():
                    continue

                btype = item.get("type", "text")
                if btype == "text":
                    btype = _classify_block(text)
                elif btype == "image":
                    btype = "figure"
                elif btype == "interline_equation":
                    btype = "equation"

                bbox = item.get("bbox", {})
                blocks.append({
                    "page": page_num,
                    "block_id": f"p{page_num}-mineru-b{idx}",
                    "type": btype,
                    "text": text[:2000],
                    "bbox": {
                        "x0": bbox.get("x0", 0.0) if isinstance(bbox, dict) else 0.0,
                        "y0": bbox.get("y0", 0.0) if isinstance(bbox, dict) else 0.0,
                        "x1": bbox.get("x1", 612.0) if isinstance(bbox, dict) else 612.0,
                        "y1": bbox.get("y1", 0.0) if isinstance(bbox, dict) else 0.0,
                    },
                    "confidence": 0.90 if btype == "equation" else 0.85,
                    "source_provider": "mineru",
                    "source_version": _MINERU_VERSION,
                    "schema_version": "v1",
                    "reading_order": idx,
                })
                md_parts.append(text)

            pages[page_num] = {
                "page_number": page_num,
                "markdown": "\n\n".join(md_parts),
                "blocks": blocks,
            }

        return pages


def main() -> None:
    run_worker_server(MinerUWorkerHandler())


if __name__ == "__main__":
    main()
