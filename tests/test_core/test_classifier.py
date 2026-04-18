from __future__ import annotations

from yaruk.analyzer.classifier import (
    estimate_document_complexity,
    profile_document,
    suggest_providers,
)
from yaruk.models.canonical import AnalysisSignal


def _signal(
    equations: bool = False,
    tables: bool = False,
    density: float = 0.5,
    text: bool = True,
    columns: int = 1,
) -> AnalysisSignal:
    return AnalysisSignal(
        page_number=1,
        has_text_layer=text,
        text_density=density,
        has_equation_signals=equations,
        has_table_signals=tables,
        column_count_estimate=columns,
    )


def test_empty_signals_zero_complexity() -> None:
    assert estimate_document_complexity([]) == 0.0


def test_high_density_raises_complexity() -> None:
    signals = [_signal(density=0.9)]
    assert estimate_document_complexity(signals) > 0.5


def test_profile_academic() -> None:
    signals = [_signal(equations=True, tables=True)]
    profile = profile_document(signals)
    assert profile.dominant_type == "academic"
    assert profile.has_math
    assert profile.has_tables


def test_profile_scanned() -> None:
    signals = [_signal(text=False, density=0.0)]
    profile = profile_document(signals)
    assert profile.is_scanned


def test_suggest_providers_academic() -> None:
    signals = [_signal(equations=True, tables=True)]
    profile = profile_document(signals)
    providers = suggest_providers(profile)
    assert "mineru" in providers
