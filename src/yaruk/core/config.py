from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GPUPolicy(StrEnum):
    AUTO = "auto"
    CPU_ONLY = "cpu_only"
    GPU_PREFERRED = "gpu_preferred"


class YarukSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="YARUK_", extra="ignore")

    log_level: str = "INFO"
    vram_threshold_mb: int = 1024
    ram_threshold_mb: int = 2048
    confidence_threshold: float = Field(default=0.65, ge=0.0, le=1.0)
    gpu_policy: GPUPolicy = GPUPolicy.AUTO

    provider_priority: dict[str, int] = Field(default_factory=dict)
    fallback_chains: dict[str, list[str]] = Field(default_factory=dict)
    cache_enabled: bool = True
    cache_dir: Path | None = Field(
        default=None,
        description="Directory for persistent disk cache (PDF hash-keyed). None = no disk cache.",
    )
    cache_max_entries: int = Field(
        default=512,
        ge=16,
        description="Max number of distinct input-hash cache directories before LRU-style eviction.",
    )
    cache_ttl_seconds: int | None = Field(
        default=None,
        description="If set (>=60), drop cache entries older than this many seconds (dir mtime).",
    )
    use_subprocess: bool = True

    # --- Segmenter (MasterPlan 3.2): reference grid / bbox pipeline ---
    # Primary backbone = Hancom OpenDataLoader-PDF (or compatible) CLI.
    # Fallback = pdfplumber (always available).
    # "odl_pdf": ODL-PDF required (hard fail -> pdfplumber fallback per-page, warn once)
    # "pdfplumber": force pdfplumber only
    # "auto": try ODL-PDF if CLI configured/discovered, else pdfplumber.
    segmenter_backend: Literal["auto", "pdfplumber", "odl_pdf"] = "odl_pdf"
    opendataloader_pdf_segment_command: str | None = Field(
        default=None,
        description=(
            "CLI command template: `cmd <pdf_path> <page_1based>` -> stdout JSON PageLayout. "
            "If unset, Yaruk tries to discover an `opendataloader-pdf` binary on PATH. "
            "If neither is available, pdfplumber fallback is used."
        ),
    )
    use_engine_full_markdown: bool = Field(
        default=False,
        description=(
            "If True, merged.md prefers a single engine's full-document markdown (Marker first). "
            "If False (default), merged.md is assembled from per-page IR (avoids garbled full-doc tables)."
        ),
    )

    # --- REST API (yaruk serve) ---
    api_max_upload_bytes: int = Field(
        default=200 * 1024 * 1024,
        ge=64 * 1024,
        description="POST /convert upload body limit (DoS hardening).",
    )
    api_require_pdf_magic: bool = Field(
        default=True,
        description="Reject uploads that do not start with %PDF- magic bytes.",
    )


