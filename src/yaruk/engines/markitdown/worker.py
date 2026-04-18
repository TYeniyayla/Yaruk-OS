from __future__ import annotations

import logging
import re
import warnings
from pathlib import Path
from typing import Any

from yaruk.engines.base_worker import run_worker_server

log = logging.getLogger(__name__)

logging.getLogger("pdfminer").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", message=".*non-stroke color.*")
warnings.filterwarnings("ignore", message=".*invalid float value.*")

MARKITDOWN_AVAILABLE = False
_MARKITDOWN_VERSION = "0.0.0"

try:
    from markitdown import MarkItDown

    MARKITDOWN_AVAILABLE = True
    try:
        import importlib.metadata as _meta
        _MARKITDOWN_VERSION = _meta.version("markitdown")
    except Exception:
        _MARKITDOWN_VERSION = "0.1.5"
except ImportError:
    pass


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


def _split_md_to_pages(full_md: str, total_pages: int) -> dict[int, str]:
    """Split MarkItDown output into pages using form-feed or --- separators."""
    if "\f" in full_md:
        sections = full_md.split("\f")
    elif "\n---\n" in full_md:
        sections = full_md.split("\n---\n")
    else:
        sections = [full_md]

    pages: dict[int, str] = {}
    if len(sections) >= total_pages:
        for i in range(total_pages):
            pages[i + 1] = sections[i].strip()
    elif total_pages > 0:
        chunk_size = max(1, len(full_md) // total_pages)
        lines = full_md.split("\n")
        current_page = 1
        current_lines: list[str] = []
        char_count = 0
        for line in lines:
            current_lines.append(line)
            char_count += len(line) + 1
            if char_count >= chunk_size and current_page < total_pages:
                pages[current_page] = "\n".join(current_lines).strip()
                current_lines = []
                char_count = 0
                current_page += 1
        if current_lines:
            pages[current_page] = "\n".join(current_lines).strip()
    else:
        pages[1] = full_md.strip()

    return pages


class MarkItDownWorkerHandler:
    def __init__(self) -> None:
        self._converter: Any = None

    def _ensure_loaded(self) -> None:
        if not MARKITDOWN_AVAILABLE:
            return
        if self._converter is not None:
            return
        self._converter = MarkItDown()

    def handle(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "health":
            return {
                "name": "markitdown",
                "ok": MARKITDOWN_AVAILABLE,
                "version": _MARKITDOWN_VERSION,
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
                page_number,
                {"page_number": page_number, "blocks": [], "markdown": ""},
            )

        raise ValueError(f"unknown method: {method}")

    def _convert_full(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        if not MARKITDOWN_AVAILABLE:
            return {"error": "markitdown not installed", "pages": {}, "markdown": ""}

        if not pdf_path.exists():
            return {"error": f"file not found: {pdf_path}", "pages": {}, "markdown": ""}

        self._ensure_loaded()

        actual_path = pdf_path
        tmp_path: Path | None = None

        try:
            if max_pages:
                tmp_path = self._extract_subset(pdf_path, max_pages)
                if tmp_path:
                    actual_path = tmp_path

            result = self._converter.convert_local(str(actual_path))
            full_md = result.text_content if hasattr(result, "text_content") else str(result)

            total_pages = self._count_pages(pdf_path, max_pages)
            page_mds = _split_md_to_pages(full_md, total_pages)

            pages: dict[int, dict[str, Any]] = {}
            for page_num, page_md in page_mds.items():
                if max_pages and page_num > max_pages:
                    break
                blocks = self._parse_blocks(page_md, page_num)
                pages[page_num] = {
                    "page_number": page_num,
                    "markdown": page_md,
                    "blocks": blocks,
                }

            return {
                "pages": pages,
                "page_count": len(pages),
                "markdown": full_md,
            }

        except Exception as e:
            log.warning("markitdown conversion failed: %s", str(e)[:200])
            return {"error": str(e)[:500], "pages": {}, "markdown": ""}
        finally:
            if tmp_path and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    @staticmethod
    def _extract_subset(pdf_path: Path, max_pages: int) -> Path | None:
        """Extract first N pages into a temp PDF so MarkItDown doesn't parse the entire file."""
        try:
            import fitz  # type: ignore[import-untyped]
            doc = fitz.open(str(pdf_path))
            if doc.page_count <= max_pages:
                doc.close()
                return None
            import tempfile
            fd, tmp_name = tempfile.mkstemp(suffix=".pdf")
            import os as _os
            _os.close(fd)
            subset = fitz.open()
            subset.insert_pdf(doc, to_page=max_pages - 1)
            subset.save(tmp_name)
            subset.close()
            doc.close()
            log.info("markitdown: extracted %d/%d pages into temp PDF", max_pages, doc.page_count)
            return Path(tmp_name)
        except Exception as e:
            log.debug("markitdown subset extraction failed: %s", str(e)[:100])
            return None

    @staticmethod
    def _count_pages(pdf_path: Path, max_pages: int | None) -> int:
        try:
            import fitz  # type: ignore[import-untyped]
            doc = fitz.open(str(pdf_path))
            count = doc.page_count
            doc.close()
            return min(count, max_pages) if max_pages else count
        except Exception:
            return max_pages or 1

    @staticmethod
    def _parse_blocks(page_md: str, page_number: int) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        paragraphs = re.split(r"\n{2,}", page_md)

        for idx, para in enumerate(paragraphs):
            para = para.strip()
            if not para:
                continue

            btype = _classify_block(para)
            blocks.append({
                "page": page_number,
                "block_id": f"p{page_number}-markitdown-b{idx}",
                "type": btype,
                "text": para[:2000],
                "bbox": {"x0": 0.0, "y0": idx * 50.0, "x1": 612.0, "y1": (idx + 1) * 50.0},
                "confidence": 0.70,
                "source_provider": "markitdown",
                "source_version": _MARKITDOWN_VERSION,
                "schema_version": "v1",
                "reading_order": idx,
            })

        return blocks


def main() -> None:
    run_worker_server(MarkItDownWorkerHandler())


if __name__ == "__main__":
    main()
