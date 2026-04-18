from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from yaruk.models.canonical import BoundingBox, DocumentBlock
from yaruk.models.enums import BlockType


@dataclass(frozen=True)
class ProviderHealth:
    ok: bool
    detail: str | None = None


@dataclass(frozen=True)
class AnalysisContext:
    signals: dict[str, Any]


@dataclass(frozen=True)
class Segment:
    page: int
    bbox: BoundingBox


class BaseProvider(ABC):
    name: str
    version: str
    min_yaruk_version: str = "0.1.0"
    supported_ir_versions: tuple[str, ...] = ("v1",)

    @abstractmethod
    def supports(self, block_type: BlockType, context: AnalysisContext) -> float:
        """0.0-1.0 arasi yetenek skoru dondurur."""

    @abstractmethod
    async def analyze(self, segment: Segment) -> list[DocumentBlock]:
        """Segmenti isle, Canonical IR bloklari dondur."""

    @abstractmethod
    async def extract(self, _page_image: bytes, bbox: BoundingBox) -> DocumentBlock:
        """Belirli bir bounding box bolgesini isle."""

    @abstractmethod
    def health_check(self) -> ProviderHealth:
        """Provider durumunu kontrol et."""

