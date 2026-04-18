from __future__ import annotations

from pathlib import Path

from yaruk.core.config import YarukSettings
from yaruk.core.orchestrator import (
    Orchestrator,
    OrchestratorConfig,
    _discover_available_engines,
)
from yaruk.core.registry import ProviderRegistry


def test_orchestrator_empty_result() -> None:
    cfg = OrchestratorConfig(settings=YarukSettings(), output_dir=Path("/tmp"))
    orch = Orchestrator(cfg)
    result = orch.empty_result(Path("test.pdf"))
    assert result.total_pages == 0
    assert result.processing_info.trace_id


def test_orchestrator_can_use_gpu() -> None:
    cfg = OrchestratorConfig(settings=YarukSettings(), output_dir=Path("/tmp"))
    orch = Orchestrator(cfg)
    result = orch.can_use_gpu_now()
    assert isinstance(result, bool)


def test_discover_engines_finds_at_least_one() -> None:
    engines = _discover_available_engines()
    assert len(engines) >= 1
    assert "marker" in engines or len(engines) > 0


def test_discover_engines_with_registry() -> None:
    registry = ProviderRegistry()
    engines = _discover_available_engines(registry)
    assert len(engines) >= 1


def test_orchestrator_subprocess_mode() -> None:
    settings = YarukSettings(use_subprocess=True)
    cfg = OrchestratorConfig(settings=settings, output_dir=Path("/tmp"))
    orch = Orchestrator(cfg)
    assert orch._use_subprocess is True


def test_orchestrator_inprocess_mode() -> None:
    settings = YarukSettings(use_subprocess=False)
    cfg = OrchestratorConfig(settings=settings, output_dir=Path("/tmp"))
    orch = Orchestrator(cfg)
    assert orch._use_subprocess is False


def test_orchestrator_end_to_end_with_fixture(tmp_path: Path) -> None:
    """End-to-end test with a real (small) PDF fixture."""
    fixture = Path(__file__).parent.parent / "fixtures" / "pdfs" / "test_2page.pdf"
    if not fixture.exists():
        return

    settings = YarukSettings(use_subprocess=False)
    cfg = OrchestratorConfig(settings=settings, output_dir=tmp_path)
    orch = Orchestrator(cfg)

    result = orch.process_sync(fixture, max_pages=2)

    assert result.total_pages == 2
    assert len(result.pages) == 2
    assert result.processing_info.trace_id

    for page in result.pages:
        assert page.page_number in (1, 2)
        assert len(page.blocks) > 0
        for block in page.blocks:
            assert block.source_provider
            assert block.text

    output_dir = tmp_path
    assert any(output_dir.rglob("*.json"))
    assert any(output_dir.rglob("merged.md"))
