from __future__ import annotations

from yaruk.core.provider import AnalysisContext, BaseProvider, ProviderHealth, Segment
from yaruk.core.registry import ProviderRegistry
from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType


class DummyProvider(BaseProvider):
    name = "dummy"
    version = "1.0.0"
    min_yaruk_version = "0.1.0"
    supported_ir_versions = ("v1",)

    def supports(self, block_type: BlockType, context: AnalysisContext) -> float:
        return 0.5

    async def analyze(self, segment: Segment) -> list[DocumentBlock]:
        return []

    async def extract(self, _page_image: bytes, bbox: BoundingBox) -> DocumentBlock:
        return DocumentBlock(
            page=1, block_id="d0", type=BlockType.PARAGRAPH,
            text="", bbox=bbox, confidence=0.0,
            source_provider="dummy", source_version="1.0.0", reading_order=0,
        )

    def health_check(self) -> ProviderHealth:
        return ProviderHealth(ok=True, detail="dummy provider")


def test_registry_register_and_get() -> None:
    registry = ProviderRegistry()
    registry.register("dummy", DummyProvider)
    assert "dummy" in registry.list()
    prov = registry.get("dummy")
    assert prov.name == "dummy"
    assert prov.health_check().ok


def test_registry_version_check() -> None:
    registry = ProviderRegistry()

    class OldProvider(DummyProvider):
        min_yaruk_version = "999.0.0"

    assert not registry._version_compatible(OldProvider())


def test_registry_ir_check() -> None:
    registry = ProviderRegistry()

    class BadIRProvider(DummyProvider):
        supported_ir_versions = ("v999",)

    assert not registry._ir_compatible(BadIRProvider())


def test_registry_list_empty() -> None:
    registry = ProviderRegistry()
    assert registry.list() == []
