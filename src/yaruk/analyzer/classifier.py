from __future__ import annotations

from dataclasses import dataclass

from yaruk.models.canonical import AnalysisSignal


@dataclass(frozen=True)
class DocumentProfile:
    complexity: float
    dominant_type: str
    has_math: bool
    has_tables: bool
    is_scanned: bool
    multi_column: bool
    page_count: int


def estimate_document_complexity(signals: list[AnalysisSignal]) -> float:
    if not signals:
        return 0.0
    score = 0.0
    for s in signals:
        score += s.text_density
        if s.has_equation_signals:
            score += 0.2
        if s.has_table_signals:
            score += 0.2
        if (s.column_count_estimate or 1) > 1:
            score += 0.1
    return min(score / len(signals), 1.0)


def profile_document(signals: list[AnalysisSignal]) -> DocumentProfile:
    if not signals:
        return DocumentProfile(
            complexity=0.0, dominant_type="empty", has_math=False,
            has_tables=False, is_scanned=True, multi_column=False, page_count=0,
        )

    has_math = any(s.has_equation_signals for s in signals)
    has_tables = any(s.has_table_signals for s in signals)
    is_scanned = sum(1 for s in signals if not s.has_text_layer) > len(signals) * 0.5
    multi_column = any((s.column_count_estimate or 1) > 1 for s in signals)

    if has_math and has_tables:
        dominant = "academic"
    elif has_tables:
        dominant = "tabular"
    elif has_math:
        dominant = "scientific"
    elif is_scanned:
        dominant = "scanned"
    elif multi_column:
        dominant = "multi-column"
    else:
        dominant = "text-heavy"

    return DocumentProfile(
        complexity=estimate_document_complexity(signals),
        dominant_type=dominant,
        has_math=has_math,
        has_tables=has_tables,
        is_scanned=is_scanned,
        multi_column=multi_column,
        page_count=len(signals),
    )


def suggest_providers(profile: DocumentProfile) -> list[str]:
    if profile.dominant_type == "academic":
        return ["mineru", "docling", "marker"]
    if profile.dominant_type == "tabular":
        return ["docling", "mineru", "marker"]
    if profile.dominant_type == "scientific":
        return ["mineru", "marker", "docling"]
    if profile.dominant_type == "scanned":
        return ["marker", "docling", "mineru"]
    return ["marker", "docling", "markitdown"]

