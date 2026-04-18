from __future__ import annotations

from yaruk.core.config import YarukSettings
from yaruk.core.router import DEFAULT_CHAIN, DynamicRouter
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


def test_all_five_engines_available() -> None:
    all_engines = ["marker", "docling", "mineru", "markitdown", "opendataloader"]
    router = DynamicRouter(YarukSettings(), available_providers=all_engines)

    eq_signal = _make_signal(equations=True)
    decision = router.route_page(eq_signal)
    assert decision.provider_chain[0] == "mineru"
    assert "marker" in decision.provider_chain

    tbl_signal = _make_signal(tables=True)
    decision = router.route_page(tbl_signal)
    assert decision.provider_chain[0] == "docling"

    text_signal = _make_signal(density=0.8)
    decision = router.route_page(text_signal)
    assert decision.provider_chain[0] == "marker"


def test_default_chains_cover_all_types() -> None:
    for block_type in ["paragraph", "table", "equation", "figure", "heading",
                       "list", "code", "caption", "footer", "header", "other"]:
        assert block_type in DEFAULT_CHAIN
        assert len(DEFAULT_CHAIN[block_type]) >= 2


def test_chain_filters_unavailable_engines() -> None:
    router = DynamicRouter(YarukSettings(), available_providers=["markitdown", "opendataloader"])
    signal = _make_signal(density=0.8)
    decision = router.route_page(signal)
    for provider in decision.provider_chain:
        assert provider in ("markitdown", "opendataloader", "marker")


def test_equation_routing_prefers_mineru() -> None:
    chain = DEFAULT_CHAIN["equation"]
    assert chain[0] == "mineru"


def test_table_routing_prefers_docling() -> None:
    chain = DEFAULT_CHAIN["table"]
    assert chain[0] == "docling"


def test_figure_routing_prefers_marker() -> None:
    chain = DEFAULT_CHAIN["figure"]
    assert chain[0] == "marker"
