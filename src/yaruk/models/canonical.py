from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from yaruk.models.enums import BlockType

SchemaVersion = Literal["v1"]


class BoundingBox(BaseModel):
    x0: float
    y0: float
    x1: float
    y1: float


class AssetRef(BaseModel):
    asset_id: str
    rel_path: str
    mime_type: str | None = None
    sha256: str | None = None
    width: int | None = None
    height: int | None = None


class AssetIndex(BaseModel):
    assets: dict[str, AssetRef] = Field(default_factory=dict)
    block_to_assets: dict[str, list[str]] = Field(default_factory=dict)


class BlockRelation(BaseModel):
    type: str
    target_block_id: str


class DocumentBlock(BaseModel):
    page: int
    block_id: str
    type: BlockType
    text: str = ""
    bbox: BoundingBox
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    source_provider: str
    source_version: str
    schema_version: SchemaVersion = "v1"
    reading_order: int = 0
    language: str | None = None
    style: dict[str, Any] | None = None
    relations: list[BlockRelation] = Field(default_factory=list)
    raw_payload: dict[str, Any] | None = None


class PageResult(BaseModel):
    page_number: int
    width: float
    height: float
    blocks: list[DocumentBlock] = Field(default_factory=list)


class DocumentMetadata(BaseModel):
    title: str | None = None
    author: str | None = None
    subject: str | None = None
    keywords: list[str] = Field(default_factory=list)


class ProcessingInfo(BaseModel):
    trace_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    config_snapshot: dict[str, Any] = Field(default_factory=dict)
    provider_versions: dict[str, str] = Field(default_factory=dict)
    routing_decisions: list[dict[str, Any]] = Field(default_factory=list)


class DocumentResult(BaseModel):
    source_path: Path
    total_pages: int
    pages: list[PageResult] = Field(default_factory=list)
    assets: AssetIndex = Field(default_factory=AssetIndex)
    metadata: DocumentMetadata = Field(default_factory=DocumentMetadata)
    processing_info: ProcessingInfo


class AnalysisSignal(BaseModel):
    page_number: int
    has_text_layer: bool
    text_density: float = Field(ge=0.0, le=1.0)
    has_equation_signals: bool = False
    has_table_signals: bool = False
    column_count_estimate: int | None = None
    language: str | None = None
    is_rtl: bool = False

