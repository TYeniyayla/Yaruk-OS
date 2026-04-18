"""VLM model manifest: metadata classes and loader."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)

_MANIFEST_PATH = Path(__file__).resolve().parent.parent.parent.parent / "models" / "vlm" / "manifest.json"


@dataclass
class VLMModelSpec:
    model_id: str
    hf_repo: str
    tier: int
    params_b: float
    min_vram_mb: int
    quality_rank: int
    license: str
    languages: list[str] = field(default_factory=list)
    lang_count: int = 0
    architecture: str = ""
    description: str = ""
    loader: str = "generic_vlm"
    quantization: str = "none"
    hf_revision: str | None = None

    @property
    def is_multilingual(self) -> bool:
        return "multilingual" in self.languages or self.lang_count > 5

    def supports_language(self, lang_code: str) -> bool:
        if self.is_multilingual:
            return True
        return lang_code.lower() in [lang.lower() for lang in self.languages]


@dataclass
class VLMManifest:
    models: list[VLMModelSpec] = field(default_factory=list)
    schema_version: str = "1.0"

    def get_by_tier(self, tier: int) -> VLMModelSpec | None:
        for m in self.models:
            if m.tier == tier:
                return m
        return None

    def get_by_id(self, model_id: str) -> VLMModelSpec | None:
        for m in self.models:
            if m.model_id == model_id:
                return m
        return None

    def models_by_quality(self) -> list[VLMModelSpec]:
        return sorted(self.models, key=lambda m: m.quality_rank)

    def models_fitting_vram(self, free_vram_mb: int) -> list[VLMModelSpec]:
        fitting = [m for m in self.models if m.min_vram_mb <= free_vram_mb]
        return sorted(fitting, key=lambda m: m.quality_rank)


def load_manifest(path: Path | None = None) -> VLMManifest:
    p = path or _MANIFEST_PATH
    if not p.exists():
        log.warning("VLM manifest not found at %s", p)
        return VLMManifest()

    data = json.loads(p.read_text())
    models = []
    for entry in data.get("models", []):
        models.append(VLMModelSpec(
            model_id=entry["model_id"],
            hf_repo=entry["hf_repo"],
            tier=entry["tier"],
            params_b=entry["params_b"],
            min_vram_mb=entry["min_vram_mb"],
            quality_rank=entry["quality_rank"],
            license=entry["license"],
            languages=entry.get("languages", []),
            lang_count=entry.get("lang_count", 0),
            architecture=entry.get("architecture", ""),
            description=entry.get("description", ""),
            loader=entry.get("loader", "generic_vlm"),
            quantization=entry.get("quantization", "none"),
            hf_revision=entry.get("hf_revision"),
        ))
    return VLMManifest(models=models, schema_version=data.get("schema_version", "1.0"))
