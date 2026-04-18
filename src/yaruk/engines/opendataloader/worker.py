"""Extraction worker using Hancom OpenDataLoader-PDF SDK.

Primary backbone engine: uses ``opendataloader-pdf`` Python package
(``opendataloader_pdf.convert``) which wraps the Java CLI JAR.  Produces
structured JSON with bounding boxes and semantic types for every element.

Falls back to pdfplumber-based extraction when ODL SDK or Java is unavailable.
"""
from __future__ import annotations

import json
import logging
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from yaruk.engines.base_worker import run_worker_server

log = logging.getLogger(__name__)

OPENDATALOADER_AVAILABLE = False
_ODL_VERSION = "0.0.0"
_USE_SDK = False

try:
    import opendataloader_pdf  # noqa: F401
    _USE_SDK = bool(shutil.which("java"))
    if _USE_SDK:
        try:
            import importlib.metadata as _meta
            _ODL_VERSION = _meta.version("opendataloader-pdf")
        except Exception:
            _ODL_VERSION = "2.2.0"
        OPENDATALOADER_AVAILABLE = True
except ImportError:
    pass

if not OPENDATALOADER_AVAILABLE:
    try:
        import pdfplumber  # noqa: F401
        OPENDATALOADER_AVAILABLE = True
        try:
            import importlib.metadata as _meta2
            _ODL_VERSION = "pdfplumber-" + _meta2.version("pdfplumber")
        except Exception:
            _ODL_VERSION = "pdfplumber-0.11.0"
    except ImportError:
        pass


def _classify_block(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("#"):
        return "heading"
    if stripped.startswith("$$") or stripped.startswith("\\["):
        return "equation"
    if stripped.startswith("|") and "|" in stripped[1:]:
        return "table"
    if stripped.startswith("!["):
        return "figure"
    if stripped.startswith("- ") or stripped.startswith("* ") or re.match(r"^\d+\.", stripped):
        return "list"
    return "paragraph"


def _table_to_markdown(table: list[list[str | None]]) -> str:
    if not table:
        return ""
    rows: list[str] = []
    for i, row in enumerate(table):
        cells = [str(c or "") for c in row]
        rows.append("| " + " | ".join(cells) + " |")
        if i == 0:
            rows.append("| " + " | ".join(["---"] * len(cells)) + " |")
    return "\n".join(rows)


def _flatten_odl_kids(data: Any) -> list[dict[str, Any]]:
    """Recursively flatten ODL's hierarchical ``kids`` JSON into a flat element list."""
    if isinstance(data, list):
        out: list[dict[str, Any]] = []
        for item in data:
            out.extend(_flatten_odl_kids(item))
        return out
    if isinstance(data, dict):
        kids = data.get("kids")
        if isinstance(kids, list) and kids:
            flat: list[dict[str, Any]] = []
            for kid in kids:
                flat.extend(_flatten_odl_kids(kid))
            return flat
        if data.get("type") and data.get("content") is not None:
            return [data]
        elements = data.get("elements")
        if isinstance(elements, list):
            return elements
    return []


_ODL_TYPE_TO_BLOCK: dict[str, str] = {
    "heading": "heading",
    "paragraph": "paragraph",
    "table": "table",
    "list": "list",
    "formula": "equation",
    "image": "figure",
    "picture": "figure",
    "figure": "figure",
    "caption": "caption",
    "footnote": "footer",
    "header": "header",
    "footer": "footer",
    "page-header": "header",
    "page-footer": "footer",
    "code": "code",
}


class OpenDataLoaderWorkerHandler:
    def handle(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "health":
            return {
                "name": "opendataloader",
                "ok": OPENDATALOADER_AVAILABLE,
                "version": _ODL_VERSION,
                "sdk": _USE_SDK,
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
        if not OPENDATALOADER_AVAILABLE:
            return {"error": "opendataloader-pdf SDK / pdfplumber not installed", "pages": {}, "markdown": ""}

        if not pdf_path.exists():
            return {"error": f"file not found: {pdf_path}", "pages": {}, "markdown": ""}

        if _USE_SDK:
            return self._convert_via_sdk(pdf_path, max_pages)
        return self._convert_via_pdfplumber(pdf_path, max_pages)

    def _convert_via_sdk(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        """Convert using real Hancom OpenDataLoader-PDF SDK."""
        tmpdir = tempfile.mkdtemp(prefix="yaruk_odl_engine_")
        try:
            import opendataloader_pdf

            pages_arg = None
            if max_pages:
                pages_arg = f"1-{max_pages}"

            import contextlib
            import io as _io

            print(f"[odl-worker] converting {pdf_path.name} via SDK", file=sys.stderr, flush=True)
            with contextlib.redirect_stderr(_io.StringIO()):
                opendataloader_pdf.convert(
                    input_path=[str(pdf_path)],
                    output_dir=tmpdir,
                    format="json,markdown",
                    quiet=True,
                    pages=pages_arg,
                )

            json_files = list(Path(tmpdir).rglob("*.json"))
            md_files = list(Path(tmpdir).rglob("*.md"))

            full_md = ""
            if md_files:
                full_md = md_files[0].read_text(encoding="utf-8")

            pages: dict[int, dict[str, Any]] = {}
            all_elements: list[dict[str, Any]] = []

            for jf in json_files:
                try:
                    data = json.loads(jf.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    continue
                all_elements.extend(_flatten_odl_kids(data))

            page_groups: dict[int, list[dict[str, Any]]] = {}
            for elem in all_elements:
                pn = int(elem.get("page number", elem.get("page_number", 1)))
                page_groups.setdefault(pn, []).append(elem)

            for pn, elems in sorted(page_groups.items()):
                blocks: list[dict[str, Any]] = []
                md_parts: list[str] = []
                for idx, elem in enumerate(elems):
                    raw_type = str(elem.get("type", "paragraph")).lower()
                    mapped_type = _ODL_TYPE_TO_BLOCK.get(raw_type, "paragraph")
                    content = str(elem.get("content", ""))

                    raw_bb = elem.get("bounding box") or elem.get("bbox") or [0, 0, 0, 0]
                    if len(raw_bb) >= 4:
                        left, bottom, right, top = (float(v) for v in raw_bb[:4])
                    else:
                        left = bottom = right = top = 0.0

                    blocks.append({
                        "page": pn,
                        "block_id": f"p{pn}-odl-b{idx}",
                        "type": mapped_type,
                        "text": content[:2000],
                        "bbox": {
                            "x0": min(left, right),
                            "y0": min(bottom, top),
                            "x1": max(left, right),
                            "y1": max(bottom, top),
                        },
                        "confidence": 0.92,
                        "source_provider": "opendataloader",
                        "source_version": _ODL_VERSION,
                        "schema_version": "v1",
                        "reading_order": idx,
                    })
                    md_parts.append(content)

                pages[pn] = {
                    "page_number": pn,
                    "markdown": "\n\n".join(md_parts),
                    "blocks": blocks,
                }

            print(
                f"[odl-worker] SDK done: {len(pages)} pages, {len(all_elements)} elements",
                file=sys.stderr, flush=True,
            )
            return {
                "pages": pages,
                "page_count": len(pages),
                "markdown": full_md,
            }

        except FileNotFoundError:
            log.warning("ODL SDK: Java not found, falling back to pdfplumber")
            return self._convert_via_pdfplumber(pdf_path, max_pages)
        except Exception as e:
            log.warning("ODL SDK conversion failed: %s. Falling back to pdfplumber.", str(e)[:200])
            return self._convert_via_pdfplumber(pdf_path, max_pages)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def _convert_via_pdfplumber(self, pdf_path: Path, max_pages: int | None = None) -> dict[str, Any]:
        """Fallback conversion using pdfplumber (legacy path)."""
        try:
            import pdfplumber as _plumber
        except ImportError:
            return {"error": "pdfplumber not installed", "pages": {}, "markdown": ""}

        try:
            pdf = _plumber.open(str(pdf_path))
            total = min(len(pdf.pages), max_pages or len(pdf.pages))

            pages: dict[int, dict[str, Any]] = {}
            all_md: list[str] = []

            for pg_idx in range(total):
                page_num = pg_idx + 1
                page = pdf.pages[pg_idx]
                page_width = float(page.width)
                page_height = float(page.height)

                blocks: list[dict[str, Any]] = []
                md_parts: list[str] = []
                block_idx = 0

                tables = page.find_tables() or []
                table_bboxes: list[tuple[float, float, float, float]] = []
                for tbl in tables:
                    extracted = tbl.extract()
                    if not extracted:
                        continue
                    tbl_md = _table_to_markdown(extracted)
                    bbox = tbl.bbox
                    table_bboxes.append(bbox)
                    blocks.append({
                        "page": page_num,
                        "block_id": f"p{page_num}-odl-b{block_idx}",
                        "type": "table",
                        "text": tbl_md[:2000],
                        "bbox": {
                            "x0": float(bbox[0]), "y0": float(bbox[1]),
                            "x1": float(bbox[2]), "y1": float(bbox[3]),
                        },
                        "confidence": 0.88,
                        "source_provider": "opendataloader",
                        "source_version": _ODL_VERSION,
                        "schema_version": "v1",
                        "reading_order": block_idx,
                    })
                    md_parts.append(tbl_md)
                    block_idx += 1

                images = page.images or []
                for img in images:
                    x0 = float(img.get("x0", 0))
                    y0 = float(img.get("top", 0))
                    x1 = float(img.get("x1", page_width))
                    y1 = float(img.get("bottom", page_height))
                    blocks.append({
                        "page": page_num,
                        "block_id": f"p{page_num}-odl-b{block_idx}",
                        "type": "figure",
                        "text": "[image]",
                        "bbox": {"x0": x0, "y0": y0, "x1": x1, "y1": y1},
                        "confidence": 0.80,
                        "source_provider": "opendataloader",
                        "source_version": _ODL_VERSION,
                        "schema_version": "v1",
                        "reading_order": block_idx,
                    })
                    md_parts.append("![image]()")
                    block_idx += 1

                words = page.extract_words(keep_blank_chars=True) or []
                text_clusters = self._cluster_words(words, table_bboxes)
                for cluster_text, bbox in text_clusters:
                    if not cluster_text.strip():
                        continue
                    btype = _classify_block(cluster_text)
                    blocks.append({
                        "page": page_num,
                        "block_id": f"p{page_num}-odl-b{block_idx}",
                        "type": btype,
                        "text": cluster_text[:2000],
                        "bbox": {
                            "x0": bbox[0], "y0": bbox[1],
                            "x1": bbox[2], "y1": bbox[3],
                        },
                        "confidence": 0.82,
                        "source_provider": "opendataloader",
                        "source_version": _ODL_VERSION,
                        "schema_version": "v1",
                        "reading_order": block_idx,
                    })
                    md_parts.append(cluster_text)
                    block_idx += 1

                blocks.sort(key=lambda b: (b["bbox"]["y0"], b["bbox"]["x0"]))
                for i, b in enumerate(blocks):
                    b["reading_order"] = i

                page_md = "\n\n".join(md_parts)
                pages[page_num] = {
                    "page_number": page_num,
                    "markdown": page_md,
                    "blocks": blocks,
                }
                all_md.append(page_md)

            pdf.close()

            return {
                "pages": pages,
                "page_count": len(pages),
                "markdown": "\n\n---\n\n".join(all_md),
            }

        except Exception as e:
            log.warning("opendataloader pdfplumber conversion failed: %s", str(e)[:200])
            return {"error": str(e)[:500], "pages": {}, "markdown": ""}

    @staticmethod
    def _is_in_table(word: dict[str, Any], table_bboxes: list[tuple[float, float, float, float]]) -> bool:
        wx0 = float(word.get("x0", 0))
        wy0 = float(word.get("top", 0))
        wx1 = float(word.get("x1", 0))
        wy1 = float(word.get("bottom", 0))
        for tx0, ty0, tx1, ty1 in table_bboxes:
            if wx0 >= tx0 - 2 and wy0 >= ty0 - 2 and wx1 <= tx1 + 2 and wy1 <= ty1 + 2:
                return True
        return False

    def _cluster_words(
        self,
        words: list[dict[str, Any]],
        table_bboxes: list[tuple[float, float, float, float]],
        line_tolerance: float = 5.0,
        para_gap: float = 15.0,
    ) -> list[tuple[str, tuple[float, float, float, float]]]:
        filtered = [w for w in words if not self._is_in_table(w, table_bboxes)]
        if not filtered:
            return []

        filtered.sort(key=lambda w: (float(w.get("top", 0)), float(w.get("x0", 0))))

        lines: list[tuple[str, float, float, float, float]] = []
        current_line_words: list[str] = []
        current_top = float(filtered[0].get("top", 0))
        x0_min = float(filtered[0].get("x0", 0))
        x1_max = float(filtered[0].get("x1", 0))
        bottom_max = float(filtered[0].get("bottom", 0))

        for w in filtered:
            w_top = float(w.get("top", 0))
            if abs(w_top - current_top) > line_tolerance:
                if current_line_words:
                    lines.append((" ".join(current_line_words), x0_min, current_top, x1_max, bottom_max))
                current_line_words = [w.get("text", "")]
                current_top = w_top
                x0_min = float(w.get("x0", 0))
                x1_max = float(w.get("x1", 0))
                bottom_max = float(w.get("bottom", 0))
            else:
                current_line_words.append(w.get("text", ""))
                x0_min = min(x0_min, float(w.get("x0", 0)))
                x1_max = max(x1_max, float(w.get("x1", 0)))
                bottom_max = max(bottom_max, float(w.get("bottom", 0)))

        if current_line_words:
            lines.append((" ".join(current_line_words), x0_min, current_top, x1_max, bottom_max))

        if not lines:
            return []

        clusters: list[tuple[str, tuple[float, float, float, float]]] = []
        para_lines: list[str] = []
        px0, py0, px1, py1 = lines[0][1], lines[0][2], lines[0][3], lines[0][4]

        for text, lx0, ly0, lx1, ly1 in lines:
            if ly0 - py1 > para_gap and para_lines:
                clusters.append(("\n".join(para_lines), (px0, py0, px1, py1)))
                para_lines = [text]
                px0, py0, px1, py1 = lx0, ly0, lx1, ly1
            else:
                para_lines.append(text)
                px0 = min(px0, lx0)
                px1 = max(px1, lx1)
                py1 = max(py1, ly1)

        if para_lines:
            clusters.append(("\n".join(para_lines), (px0, py0, px1, py1)))

        return clusters


def main() -> None:
    run_worker_server(OpenDataLoaderWorkerHandler())


if __name__ == "__main__":
    main()
