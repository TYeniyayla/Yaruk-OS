"""Segmenter: page-level bbox segments (reference grid) for routing.

MasterPlan 3.2 — layering:
  PRIMARY BACKBONE: Hancom OpenDataLoader-PDF Python SDK (``opendataloader-pdf``).
    - ``convert(input_path, output_dir, format="json")`` → one JVM call for the
      entire document → per-page JSON with bbox + semantic type for every element.
    - pip dependency: ``opendataloader-pdf>=2.2.0``  (bundles the Java CLI JAR).
    - requires Java 11+ at runtime.
  FALLBACK: pdfplumber.
    - always available if the ``pdfplumber`` wheel is installed.
    - used transparently when ODL is unavailable (no Java / import fail) or
      fails for a specific document.
  FINAL FALLBACK: PyMuPDF (fitz) block dict — last-resort if pdfplumber also fails.
"""
from __future__ import annotations

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import fitz  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from yaruk.core.config import YarukSettings

log = logging.getLogger(__name__)

try:
    import pdfplumber
    _PDFPLUMBER = True
except ImportError:
    _PDFPLUMBER = False

try:
    import opendataloader_pdf  # noqa: F401
    _ODL_SDK = True
except ImportError:
    _ODL_SDK = False

_ODL_TYPE_MAP: dict[str, str] = {
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


@dataclass(frozen=True)
class PageSegment:
    """A single content region detected on a page."""
    page_number: int
    block_type: str
    bbox: tuple[float, float, float, float]
    text_hint: str = ""
    confidence: float = 0.8


@dataclass
class PageLayout:
    """All segments detected on a single page."""
    page_number: int
    width: float
    height: float
    segments: list[PageSegment] = field(default_factory=list)


_MATH_CHARS = frozenset("∑∫∂∇≈≠±∞αβγδεθλμσωπΣΔΩ")
_MATH_KW_RE = re.compile(r"\\\\frac|\\\\int|\\\\sum|E\s*=\s*mc")


def score_layout_quality(layout: PageLayout) -> float:
    """Heuristic 0..1: segment count, bbox sanity, structure diversity."""
    segs = layout.segments
    if not segs:
        return 0.0
    w, h = max(layout.width, 1.0), max(layout.height, 1.0)
    score = min(1.0, 0.08 * len(segs))
    types = {s.block_type for s in segs}
    if "table" in types or "figure" in types:
        score += 0.12
    out = 0
    for s in segs:
        x0, y0, x1, y1 = s.bbox
        if x0 < -10 or y0 < -10 or x1 > w + 10 or y1 > h + 10 or x1 <= x0 or y1 <= y0:
            out += 1
    score -= min(0.4, 0.05 * out)
    return max(0.0, min(1.0, score))


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


def _parse_odl_json_page(elements: list[dict[str, Any]], page_num: int) -> list[PageSegment]:
    """Convert ODL JSON elements list for a single page into PageSegments.

    ODL JSON bbox format: ``[left, bottom, right, top]`` (PDF points, origin
    bottom-left).  We normalise to ``(x0, y0_top, x1, y1_bottom)`` with
    y-axis top-down so coordinates align with pdfplumber / PyMuPDF.
    """
    segments: list[PageSegment] = []
    for elem in elements:
        raw_bb = elem.get("bounding box") or elem.get("bbox")
        if not raw_bb or len(raw_bb) < 4:
            continue
        left, bottom, right, top = (float(v) for v in raw_bb[:4])
        y0 = min(bottom, top)
        y1 = max(bottom, top)
        x0 = min(left, right)
        x1 = max(left, right)
        if x1 - x0 < 1 or y1 - y0 < 1:
            continue

        raw_type = str(elem.get("type", "paragraph")).lower()
        mapped = _ODL_TYPE_MAP.get(raw_type, "paragraph")
        content = str(elem.get("content", ""))[:500]

        segments.append(PageSegment(
            page_number=page_num,
            block_type=mapped,
            bbox=(x0, y0, x1, y1),
            text_hint=content,
            confidence=0.92,
        ))
    return segments


def page_layout_from_odl_json(data: dict[str, Any], fallback_page: int) -> PageLayout | None:
    """Parse JSON from an external OpenDataLoader-PDF compatible CLI."""
    try:
        pn = int(data.get("page_number", fallback_page))
        width = float(data.get("width", 612))
        height = float(data.get("height", 792))
        raw = data.get("segments") or []
        segments: list[PageSegment] = []
        for s in raw:
            bb = s.get("bbox")
            if not bb or len(bb) < 4:
                continue
            x0, y0, x1, y1 = float(bb[0]), float(bb[1]), float(bb[2]), float(bb[3])
            segments.append(
                PageSegment(
                    page_number=pn,
                    block_type=str(s.get("block_type", "paragraph")),
                    bbox=(x0, y0, x1, y1),
                    text_hint=str(s.get("text_hint", ""))[:500],
                    confidence=float(s.get("confidence", 0.82)),
                ),
            )
        if not segments:
            return None
        return PageLayout(page_number=pn, width=width, height=height, segments=segments)
    except Exception:
        return None


def try_run_odl_pdf_segment_cli(cmd: str, pdf_path: Path, page_number: int) -> PageLayout | None:
    """Run `cmd <pdf_path> <page_1based>`; expect stdout JSON for one PageLayout."""
    try:
        parts = shlex.split(cmd)
        proc = subprocess.run(
            [*parts, str(pdf_path), str(page_number)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if proc.returncode != 0:
            log.debug("ODL-PDF CLI rc=%s stderr=%s", proc.returncode, (proc.stderr or "")[:200])
            return None
        data = json.loads(proc.stdout)
        return page_layout_from_odl_json(data, page_number)
    except subprocess.TimeoutExpired:
        log.warning("ODL-PDF segment CLI timed out for page %s", page_number)
    except json.JSONDecodeError as e:
        log.debug("ODL-PDF CLI invalid JSON: %s", str(e)[:120])
    except Exception as e:
        log.debug("ODL-PDF CLI error: %s", str(e)[:120])
    return None


def _has_math_pattern(text: str) -> bool:
    if any(c in _MATH_CHARS for c in text):
        return True
    if "$$" in text or "\\[" in text:
        return True
    return bool(_MATH_KW_RE.search(text))


def _classify_text_segment(text: str) -> str:
    stripped = text.strip()
    if not stripped:
        return "paragraph"
    try:
        if stripped.startswith("#") or (len(stripped) < 80 and stripped[0].isupper() and stripped[-1] != "."):
            return "heading"
    except IndexError:
        pass
    if _has_math_pattern(stripped):
        return "equation"
    if stripped.startswith("- ") or stripped.startswith("* ") or re.match(r"^\d+\.\s", stripped):
        return "list"
    if stripped.startswith("```"):
        return "code"
    return "paragraph"


class Segmenter:
    """Extract structured page segments (reference grid) for routing.

    Primary backbone: Hancom OpenDataLoader-PDF Python SDK.
    Fallback: pdfplumber. Final fallback: PyMuPDF (fitz).

    Backend selection (``settings.segmenter_backend``):
      * ``"odl_pdf"`` (default): ODL-PDF SDK; pdfplumber fallback on failure.
      * ``"auto"``: ODL-PDF if SDK available + Java present; else pdfplumber.
      * ``"pdfplumber"``: force pdfplumber only (skip ODL entirely).
    """

    def __init__(self, settings: YarukSettings | None = None) -> None:
        self._settings = settings
        self._odl_cache: dict[str, dict[int, list[PageSegment]]] | None = None
        self._odl_available: bool | None = None
        self._warned_no_odl = False
        self._odl_warned_failure = False

    def _check_odl_available(self) -> bool:
        """One-time check: SDK importable + Java on PATH."""
        if self._odl_available is not None:
            return self._odl_available
        if not _ODL_SDK:
            self._odl_available = False
            return False
        java = shutil.which("java")
        if not java:
            self._odl_available = False
            return False
        self._odl_available = True
        return True

    def _make_subset_pdf_for_pages(self, pdf_path: Path, pages_1based: list[int]) -> tuple[Path, list[int]]:
        """Create a temporary subset PDF containing the given pages.

        Returns (subset_path, page_map) where page_map[i] is the original 1-based
        page number for subset page (i+1).
        """
        pages_sorted = sorted(set(int(p) for p in pages_1based if p > 0))
        doc = fitz.open(str(pdf_path))
        out_dir = Path(tempfile.mkdtemp(prefix="yaruk_pdf_subset_pages_"))
        out_path = out_dir / f"subset_pages_{pages_sorted[0]:04d}_{pages_sorted[-1]:04d}.pdf"
        try:
            new = fitz.open()
            for p1 in pages_sorted:
                p0 = p1 - 1
                if p0 < 0 or p0 >= doc.page_count:
                    continue
                new.insert_pdf(doc, from_page=p0, to_page=p0)
            new.save(str(out_path))
            new.close()
            return out_path, pages_sorted
        finally:
            doc.close()

    def _run_odl_on_page_subset(
        self,
        pdf_path: Path,
        pages_1based: list[int],
    ) -> dict[int, list[PageSegment]]:
        """Run ODL SDK on a physical subset PDF containing only selected pages.

        This avoids ODL traversing the entire original PDF and lets us bisect
        failures (e.g. StackOverflowError on specific pages/tables).
        """
        subset_path, page_map = self._make_subset_pdf_for_pages(pdf_path, pages_1based)
        tmpdir = tempfile.mkdtemp(prefix="yaruk_odl_seg_subset_")
        try:
            import contextlib
            import io

            import opendataloader_pdf

            # Suppress ODL's own stderr prints (Java stack traces on CalledProcessError)
            with contextlib.redirect_stderr(io.StringIO()):
                opendataloader_pdf.convert(
                    input_path=[str(subset_path)],
                    output_dir=tmpdir,
                    format="json",
                    quiet=True,
                )
            json_files = list(Path(tmpdir).rglob("*.json"))
            if not json_files:
                return {}
            all_elements: list[dict[str, Any]] = []
            for jf in json_files:
                try:
                    data = json.loads(jf.read_text(encoding="utf-8"))
                except Exception:
                    continue
                all_elements.extend(_flatten_odl_kids(data))

            subset_groups: dict[int, list[dict[str, Any]]] = {}
            for elem in all_elements:
                pn = int(elem.get("page number", elem.get("page_number", 1)))
                subset_groups.setdefault(pn, []).append(elem)

            out: dict[int, list[PageSegment]] = {}
            for subset_pn, elems in subset_groups.items():
                if subset_pn < 1 or subset_pn > len(page_map):
                    continue
                orig_pn = page_map[subset_pn - 1]
                segs = _parse_odl_json_page(elems, subset_pn)
                if segs:
                    # rewrite page_number to original page
                    out[orig_pn] = [
                        PageSegment(
                            page_number=orig_pn,
                            block_type=s.block_type,
                            bbox=s.bbox,
                            text_hint=s.text_hint,
                            confidence=s.confidence,
                        )
                        for s in segs
                    ]
            return out
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(str(subset_path.parent), ignore_errors=True)

    def _run_odl_with_bisect(
        self,
        pdf_path: Path,
        pages_1based: list[int],
        *,
        chunk_size: int = 50,
    ) -> dict[int, list[PageSegment]]:
        """Best-effort ODL extraction with fault isolation.

        Runs ODL on page chunks. If a chunk fails, split recursively until single
        pages; those pages fall back to pdfplumber.
        """
        pages_sorted = sorted(set(int(p) for p in pages_1based if p > 0))
        out: dict[int, list[PageSegment]] = {}

        def _run_chunk(chunk: list[int]) -> None:
            nonlocal out
            try:
                out.update(self._run_odl_on_page_subset(pdf_path, chunk))
            except Exception as e:
                # Avoid spewing full Java stderr/stack traces; show a compact reason once.
                msg = str(e).replace("\n", " ")
                msg = msg[:220] + ("…" if len(msg) > 220 else "")
                if not self._odl_warned_failure:
                    log.warning(
                        "ODL-PDF SDK chunk failed (%d pages). Will bisect + fallback per-page. Reason: %s",
                        len(chunk),
                        msg,
                    )
                    self._odl_warned_failure = True
                if len(chunk) <= 1:
                    return
                mid = len(chunk) // 2
                _run_chunk(chunk[:mid])
                _run_chunk(chunk[mid:])

        # Chunking outer loop
        for i in range(0, len(pages_sorted), max(1, chunk_size)):
            _run_chunk(pages_sorted[i : i + chunk_size])

        return out

    def _run_odl_sdk_full(self, pdf_path: Path, *, cache: bool = True) -> dict[int, list[PageSegment]]:
        """Run ODL SDK convert() for full document → dict[page_num → segments].

        Single JVM invocation for all pages.  Output JSON files are parsed
        and cached so ``segment_page`` can look up individual pages O(1).
        """
        cache_key = str(pdf_path.resolve())
        if cache and self._odl_cache and cache_key in self._odl_cache:
            return self._odl_cache[cache_key]

        if cache and self._odl_cache is None:
            self._odl_cache = {}

        result: dict[int, list[PageSegment]] = {}
        tmpdir = tempfile.mkdtemp(prefix="yaruk_odl_seg_")
        try:
            import contextlib
            import io

            import opendataloader_pdf
            log.info("ODL-PDF SDK: converting %s (JSON mode)", pdf_path.name)
            with contextlib.redirect_stderr(io.StringIO()):
                opendataloader_pdf.convert(
                    input_path=[str(pdf_path)], output_dir=tmpdir, format="json", quiet=True,
                )
            json_files = list(Path(tmpdir).rglob("*.json"))
            if not json_files:
                log.warning("ODL-PDF SDK produced no JSON output for %s", pdf_path.name)
                if cache and self._odl_cache is not None:
                    self._odl_cache[cache_key] = result
                return result

            for jf in json_files:
                try:
                    data = json.loads(jf.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError) as e:
                    log.debug("ODL JSON parse error %s: %s", jf.name, e)
                    continue

                elements = _flatten_odl_kids(data)
                if not elements:
                    continue

                page_groups: dict[int, list[dict[str, Any]]] = {}
                for elem in elements:
                    pn = int(elem.get("page number", elem.get("page_number", 1)))
                    page_groups.setdefault(pn, []).append(elem)

                for pn, elems in page_groups.items():
                    segs = _parse_odl_json_page(elems, pn)
                    if segs:
                        result.setdefault(pn, []).extend(segs)

            log.info(
                "ODL-PDF SDK: %d pages with segments extracted from %s",
                len(result), pdf_path.name,
            )
        except FileNotFoundError:
            log.warning("ODL-PDF SDK: Java not found (install JDK 11+). Falling back to pdfplumber.")
            self._odl_available = False
        except Exception as e:
            # Keep log compact; Java stderr can be massive.
            msg = str(e).replace("\n", " ")
            msg = msg[:220] + ("…" if len(msg) > 220 else "")
            log.warning("ODL-PDF SDK conversion failed for %s: %s", pdf_path.name, msg)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

        if cache and self._odl_cache is not None:
            self._odl_cache[cache_key] = result
        return result

    def segment_page(self, pdf_path: str | Path, page_number: int) -> PageLayout:
        """Primary ODL-PDF SDK, fallback pdfplumber (MasterPlan 3.2)."""
        pdf_path = Path(pdf_path)

        backend = (self._settings.segmenter_backend if self._settings else "odl_pdf") or "odl_pdf"

        if backend == "pdfplumber":
            return self.segment_page_from_plumber(pdf_path, page_number)

        use_odl = self._check_odl_available()

        if backend == "auto" and not use_odl:
            cmd = self._resolve_custom_cli()
            if cmd:
                return self._segment_via_cli(cmd, pdf_path, page_number)
            if not self._warned_no_odl:
                log.info("segmenter_backend=auto: ODL SDK/Java unavailable, using pdfplumber.")
                self._warned_no_odl = True
            return self.segment_page_from_plumber(pdf_path, page_number)

        if not use_odl:
            cmd = self._resolve_custom_cli()
            if cmd:
                return self._segment_via_cli(cmd, pdf_path, page_number)
            if not self._warned_no_odl:
                log.warning(
                    "segmenter_backend=odl_pdf but ODL SDK/Java unavailable. "
                    "Install: pip install opendataloader-pdf + JDK 11+. "
                    "Falling back to pdfplumber.",
                )
                self._warned_no_odl = True
            return self.segment_page_from_plumber(pdf_path, page_number)

        page_map = self._run_odl_sdk_full(pdf_path)
        odl_segs = page_map.get(page_number)
        if odl_segs:
            doc = fitz.open(str(pdf_path))
            pg = doc.load_page(page_number - 1)
            w, h = float(pg.rect.width), float(pg.rect.height)
            doc.close()
            return PageLayout(
                page_number=page_number, width=w, height=h, segments=list(odl_segs),
            )

        return self.segment_page_from_plumber(pdf_path, page_number)

    def _resolve_custom_cli(self) -> str | None:
        """Check for a user-provided external CLI command."""
        if self._settings and self._settings.opendataloader_pdf_segment_command:
            cmd = self._settings.opendataloader_pdf_segment_command.strip()
            if cmd:
                return cmd
        cmd = os.environ.get("YARUK_ODL_PDF_SEGMENT_COMMAND", "").strip()
        return cmd or None

    def _segment_via_cli(self, cmd: str, pdf_path: Path, page_number: int) -> PageLayout:
        """Segment a page via external CLI, falling back to pdfplumber."""
        odl = try_run_odl_pdf_segment_cli(cmd, pdf_path, page_number)
        if odl is not None and odl.segments:
            return odl
        log.info("External ODL CLI produced no output for page %s -> pdfplumber fallback", page_number)
        return self.segment_page_from_plumber(pdf_path, page_number)

    def segment_page_from_plumber(
        self,
        pdf_path: str | Path,
        page_number: int,
    ) -> PageLayout:
        """Segment a single page using pdfplumber."""
        if not _PDFPLUMBER:
            return self._fallback_pymupdf(str(pdf_path), page_number)

        try:
            pdf = pdfplumber.open(str(pdf_path))
            if page_number < 1 or page_number > len(pdf.pages):
                pdf.close()
                return PageLayout(page_number=page_number, width=612, height=792)

            page = pdf.pages[page_number - 1]
            layout = PageLayout(
                page_number=page_number,
                width=float(page.width),
                height=float(page.height),
            )

            table_bboxes: list[tuple[float, float, float, float]] = []
            tables = page.find_tables() or []
            for tbl in tables:
                bbox = tbl.bbox
                table_bboxes.append(bbox)
                layout.segments.append(PageSegment(
                    page_number=page_number,
                    block_type="table",
                    bbox=bbox,
                    text_hint=self._table_hint(tbl),
                    confidence=0.90,
                ))

            for img in (page.images or []):
                x0 = float(img.get("x0", 0))
                y0 = float(img.get("top", 0))
                x1 = float(img.get("x1", page.width))
                y1 = float(img.get("bottom", page.height))
                if (x1 - x0) < 10 or (y1 - y0) < 10:
                    continue
                layout.segments.append(PageSegment(
                    page_number=page_number,
                    block_type="figure",
                    bbox=(x0, y0, x1, y1),
                    text_hint="[image]",
                    confidence=0.85,
                ))

            words = page.extract_words(keep_blank_chars=True) or []
            clusters = self._cluster_words(words, table_bboxes)
            for cluster_text, bbox in clusters:
                if not cluster_text.strip():
                    continue
                try:
                    btype = _classify_text_segment(cluster_text)
                except Exception:
                    btype = "paragraph"
                layout.segments.append(PageSegment(
                    page_number=page_number,
                    block_type=btype,
                    bbox=bbox,
                    text_hint=cluster_text[:500],
                    confidence=0.82,
                ))

            layout.segments.sort(key=lambda s: (s.bbox[1], s.bbox[0]))
            pdf.close()
            return layout

        except Exception as e:
            log.warning("pdfplumber segmentation failed page %d: %s", page_number, str(e)[:150])
            return self._fallback_pymupdf(str(pdf_path), page_number)

    def segment_document(
        self,
        pdf_path: str | Path,
        max_pages: int | None = None,
    ) -> list[PageLayout]:
        """Segment all pages of a document.

        When using ODL SDK backend, a single JVM call processes the entire
        document and results are cached — individual ``segment_page`` calls
        then become O(1) lookups.
        """
        pdf_path = Path(pdf_path)
        doc = fitz.open(str(pdf_path))
        doc_page_count = doc.page_count
        total = min(doc_page_count, max_pages or doc_page_count)
        doc.close()

        backend = (self._settings.segmenter_backend if self._settings else "odl_pdf") or "odl_pdf"
        # Warm ODL cache once, but do it in a robust chunked way so a single bad page
        # doesn't crash the whole backbone.
        if backend != "pdfplumber" and self._check_odl_available():
            pages = list(range(1, total + 1))
            # For max_pages runs, chunked subset strategy is mandatory to avoid ODL scanning full doc.
            # For full doc, still prefer chunking to isolate ODL Java failures.
            try:
                seg_map = self._run_odl_with_bisect(pdf_path, pages, chunk_size=50)
                self._odl_cache = self._odl_cache or {}
                self._odl_cache[str(pdf_path.resolve())] = seg_map
            except Exception as e:
                msg = str(e).replace("\n", " ")
                msg = msg[:220] + ("…" if len(msg) > 220 else "")
                log.warning("ODL-PDF SDK warmup failed; proceeding with pdfplumber fallback. %s", msg)

        layouts: list[PageLayout] = []
        for pg_num in range(1, total + 1):
            layouts.append(self.segment_page(pdf_path, pg_num))
        return layouts

    def _fallback_pymupdf(self, pdf_path: str, page_number: int) -> PageLayout:
        """Fallback segmentation using PyMuPDF."""
        try:
            doc = fitz.open(pdf_path)
            page = doc.load_page(page_number - 1)
            layout = PageLayout(
                page_number=page_number,
                width=float(page.rect.width),
                height=float(page.rect.height),
            )

            blocks = page.get_text("dict", flags=0).get("blocks", [])
            for b in blocks:
                bbox_raw = b.get("bbox", [0, 0, 612, 792])
                bbox = (float(bbox_raw[0]), float(bbox_raw[1]), float(bbox_raw[2]), float(bbox_raw[3]))

                if b.get("type") == 1:
                    layout.segments.append(PageSegment(
                        page_number=page_number,
                        block_type="figure",
                        bbox=bbox,
                        confidence=0.75,
                    ))
                elif b.get("type") == 0:
                    text = ""
                    for line in b.get("lines", []):
                        for span in line.get("spans", []):
                            text += span.get("text", "")
                        text += "\n"
                    text = text.strip()
                    if not text:
                        continue
                    try:
                        btype = _classify_text_segment(text)
                    except Exception:
                        btype = "paragraph"
                    layout.segments.append(PageSegment(
                        page_number=page_number,
                        block_type=btype,
                        bbox=bbox,
                        text_hint=text[:500],
                        confidence=0.65,
                    ))

            doc.close()
            return layout

        except Exception as e:
            log.warning("pymupdf fallback segmentation failed page %d: %s", page_number, str(e)[:100])
            return PageLayout(page_number=page_number, width=612, height=792)

    @staticmethod
    def _table_hint(tbl: Any) -> str:
        try:
            data = tbl.extract()
            if not data:
                return "[table]"
            header = " | ".join(str(c or "") for c in data[0])
            return f"[table: {header}]"[:200]
        except Exception:
            return "[table]"

    @staticmethod
    def _is_in_table(
        word: dict[str, Any],
        table_bboxes: list[tuple[float, float, float, float]],
    ) -> bool:
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
        cur_words: list[str] = []
        cur_top = float(filtered[0].get("top", 0))
        x0_min = float(filtered[0].get("x0", 0))
        x1_max = float(filtered[0].get("x1", 0))
        bot_max = float(filtered[0].get("bottom", 0))

        for w in filtered:
            w_top = float(w.get("top", 0))
            if abs(w_top - cur_top) > line_tolerance:
                if cur_words:
                    lines.append((" ".join(cur_words), x0_min, cur_top, x1_max, bot_max))
                cur_words = [w.get("text", "")]
                cur_top = w_top
                x0_min = float(w.get("x0", 0))
                x1_max = float(w.get("x1", 0))
                bot_max = float(w.get("bottom", 0))
            else:
                cur_words.append(w.get("text", ""))
                x0_min = min(x0_min, float(w.get("x0", 0)))
                x1_max = max(x1_max, float(w.get("x1", 0)))
                bot_max = max(bot_max, float(w.get("bottom", 0)))

        if cur_words:
            lines.append((" ".join(cur_words), x0_min, cur_top, x1_max, bot_max))

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
