from __future__ import annotations

from yaruk.core.config import YarukSettings
from yaruk.core.router import DynamicRouter
from yaruk.models.canonical import AnalysisSignal


def _make_signal(
    page: int = 1,
    has_text: bool = True,
    density: float = 0.5,
    equations: bool = False,
    tables: bool = False,
) -> AnalysisSignal:
    return AnalysisSignal(
        page_number=page,
        has_text_layer=has_text,
        text_density=density,
        has_equation_signals=equations,
        has_table_signals=tables,
    )


def test_route_text_page() -> None:
    router = DynamicRouter(YarukSettings(), available_providers=["marker", "markitdown"])
    signal = _make_signal(density=0.7)
    decision = router.route_page(signal)
    assert len(decision.provider_chain) > 0
    assert "marker" in decision.provider_chain


def test_route_equation_page() -> None:
    router = DynamicRouter(YarukSettings(), available_providers=["mineru", "marker"])
    signal = _make_signal(equations=True)
    decision = router.route_page(signal)
    assert decision.provider_chain[0] in ("mineru", "marker")
    assert "has-equations" in decision.reason


def test_route_table_page() -> None:
    router = DynamicRouter(YarukSettings(), available_providers=["docling", "marker"])
    signal = _make_signal(tables=True)
    decision = router.route_page(signal)
    assert len(decision.provider_chain) > 0
    assert "has-tables" in decision.reason


def test_route_respects_availability() -> None:
    router = DynamicRouter(YarukSettings(), available_providers=["markitdown"])
    signal = _make_signal()
    decision = router.route_page(signal)
    assert all(p in ("markitdown", "marker") for p in decision.provider_chain)


def test_fallback_chain_from_config() -> None:
    settings = YarukSettings(fallback_chains={"paragraph": ["custom1", "custom2"]})
    router = DynamicRouter(settings)
    signal = _make_signal()
    decision = router.route_page(signal)
    assert "custom1" in decision.provider_chain or "custom2" in decision.provider_chain
