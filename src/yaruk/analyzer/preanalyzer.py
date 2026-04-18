from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import fitz  # PyMuPDF

from yaruk.analyzer.multilang import detect_rtl
from yaruk.models.canonical import AnalysisSignal


@dataclass(frozen=True)
class PreAnalysisConfig:
    max_pages: int | None = None
    page_size_limit_mb: float = 200.0
    max_page_count: int = 5000


EQUATION_MARKERS = (
    "\\frac", "\\int", "\\sum", "\\prod", "\\lim",
    "\\alpha", "\\beta", "\\gamma", "\\delta", "\\theta",
    "\\partial", "\\nabla", "\\infty", "\\sqrt",
)


def _has_equation_signals(text: str) -> bool:
    if "$" in text and text.count("$") >= 2:
        return True
    return any(m in text for m in EQUATION_MARKERS)


def _has_table_signals(page: fitz.Page) -> bool:  # type: ignore[name-defined]
    text = page.get_text("text") or ""
    pipe_lines = sum(1 for line in text.split("\n") if line.count("|") >= 2)
    if pipe_lines >= 3:
        return True
    tab_lines = sum(1 for line in text.split("\n") if "\t" in line and line.count("\t") >= 2)
    if tab_lines >= 3:
        return True
    tables = page.find_tables()
    return bool(tables and len(tables.tables) > 0)


def _estimate_columns(page: fitz.Page) -> int | None:  # type: ignore[name-defined]
    blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE).get("blocks", [])  # type: ignore[attr-defined]
    if not blocks:
        return 1
    x_centers: list[float] = []
    for b in blocks:
        if b.get("type") == 0:
            x_centers.append((b["bbox"][0] + b["bbox"][2]) / 2)
    if len(x_centers) < 4:
        return 1
    page_width = page.rect.width
    if page_width <= 0:
        return 1
    normalized = [x / page_width for x in x_centers]
    left = sum(1 for x in normalized if x < 0.4)
    right = sum(1 for x in normalized if x > 0.6)
    total = len(normalized)
    if left > total * 0.3 and right > total * 0.3:
        return 2
    return 1


def _detect_language(text: str) -> str | None:
    if not text.strip():
        return None
    sample = text[:2000]
    tr_chars = sum(1 for c in sample if c in "çğıöşüÇĞİÖŞÜ")
    if tr_chars > 5:
        return "tr"
    alpha = sum(1 for c in sample if c.isalpha())
    if alpha == 0:
        return None
    ascii_alpha = sum(1 for c in sample if c.isascii() and c.isalpha())
    if ascii_alpha / max(alpha, 1) > 0.9:
        return "en"
    return None


def validate_pdf(path: Path, cfg: PreAnalysisConfig) -> None:
    size_mb = path.stat().st_size / (1024 * 1024)
    if size_mb > cfg.page_size_limit_mb:
        raise ValueError(f"PDF too large: {size_mb:.1f}MB > {cfg.page_size_limit_mb}MB limit")
    with open(path, "rb") as f:
        magic = f.read(5)
    if magic != b"%PDF-":
        raise ValueError(f"Not a valid PDF (magic bytes: {magic!r})")


def analyze_pdf(path: Path, cfg: PreAnalysisConfig | None = None) -> list[AnalysisSignal]:
    cfg = cfg or PreAnalysisConfig()
    validate_pdf(path, cfg)
    signals: list[AnalysisSignal] = []

    doc = fitz.open(str(path))
    total_pages = doc.page_count
    if total_pages > cfg.max_page_count:
        raise ValueError(f"PDF has {total_pages} pages, exceeds limit of {cfg.max_page_count}")
    limit = min(total_pages, cfg.max_pages) if cfg.max_pages else total_pages

    for i in range(limit):
        page = doc.load_page(i)
        text = page.get_text("text") or ""
        has_text_layer = bool(text.strip())
        text_density = min(len(text) / 8000.0, 1.0) if has_text_layer else 0.0

        signals.append(
            AnalysisSignal(
                page_number=i + 1,
                has_text_layer=has_text_layer,
                text_density=text_density,
                has_equation_signals=_has_equation_signals(text),
                has_table_signals=_has_table_signals(page),
                column_count_estimate=_estimate_columns(page),
                language=_detect_language(text),
                is_rtl=detect_rtl(text),
            )
        )

    doc.close()
    return signals

