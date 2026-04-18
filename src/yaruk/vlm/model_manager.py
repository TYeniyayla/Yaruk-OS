"""VLM Model Manager: download, cache, verify, and load models.

Download priority:
  1. Local cache (~/.cache/yaruk/vlm/ or project models/vlm/weights/)
  2. HuggingFace Hub (primary)
  3. GitHub LFS mirror (fallback)
"""
from __future__ import annotations

import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from yaruk.vlm.lfs_chunks import reassemble_lfs_weight_shards
from yaruk.vlm.manifest import VLMModelSpec

log = logging.getLogger(__name__)

ProgressCallback = Callable[[str, int, int], None] | None

_PROJECT_WEIGHTS = Path(__file__).resolve().parent.parent.parent.parent / "models" / "vlm" / "weights"
_USER_CACHE = Path.home() / ".cache" / "yaruk" / "vlm"


class ModelManager:
    """Manages VLM model downloads and local caching."""

    def __init__(
        self,
        project_dir: Path | None = None,
        cache_dir: Path | None = None,
    ) -> None:
        self._project_dir = project_dir or _PROJECT_WEIGHTS
        self._cache_dir = cache_dir or _USER_CACHE
        self._loaded_models: dict[str, Any] = {}

    def model_dir(self, spec: VLMModelSpec) -> Path:
        for base in (self._project_dir, self._cache_dir):
            d = base / spec.model_id
            if d.exists() and any(d.iterdir()):
                return d
        return self._cache_dir / spec.model_id

    def is_downloaded(self, spec: VLMModelSpec) -> bool:
        for base in (self._project_dir, self._cache_dir):
            d = base / spec.model_id
            if d.exists():
                config = d / "config.json"
                if config.exists():
                    return True
                if any(d.glob("*.safetensors")) or any(d.glob("*.bin")):
                    return True
        return False

    def ensure_model(
        self,
        spec: VLMModelSpec,
        progress_cb: ProgressCallback = None,
    ) -> Path:
        """Ensure model is available locally. Downloads if needed."""
        if self.is_downloaded(spec):
            return self.model_dir(spec)

        log.info("Model %s not found locally, downloading...", spec.model_id)
        if progress_cb:
            progress_cb(f"Downloading VLM: {spec.model_id} ({spec.params_b}B params)...", 0, 100)

        target = self._project_dir / spec.model_id
        target.mkdir(parents=True, exist_ok=True)

        try:
            return self._download_from_hf(spec, target, progress_cb)
        except Exception as e:
            log.warning("HuggingFace download failed for %s: %s", spec.model_id, str(e)[:200])
            try:
                return self._download_from_github(spec, target, progress_cb)
            except Exception as e2:
                log.error("All download sources failed for %s: %s", spec.model_id, str(e2)[:200])
                raise RuntimeError(
                    f"Could not download VLM '{spec.model_id}'. "
                    f"Check internet connection or download manually from {spec.hf_repo}"
                ) from e2

    def _download_from_hf(
        self,
        spec: VLMModelSpec,
        target: Path,
        progress_cb: ProgressCallback = None,
    ) -> Path:
        from huggingface_hub import snapshot_download

        log.info("Downloading %s from HuggingFace: %s", spec.model_id, spec.hf_repo)

        # NOTE: Some models (e.g. Gemma) are gated and require:
        # - HuggingFace login token with accepted terms
        # - Token available via env var HF_TOKEN (do NOT log it)
        token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")

        revision = spec.hf_revision or os.environ.get("YARUK_HF_REVISION") or "main"
        path = snapshot_download(
            repo_id=spec.hf_repo,
            revision=revision,
            local_dir=str(target),
            local_dir_use_symlinks=False,
            resume_download=True,
            token=token,
        )

        if progress_cb:
            progress_cb(f"Downloaded VLM: {spec.model_id}", 100, 100)
        log.info("Model %s downloaded to %s", spec.model_id, path)
        return Path(path)

    def _download_from_github(
        self,
        spec: VLMModelSpec,
        target: Path,
        progress_cb: ProgressCallback = None,
    ) -> Path:
        log.info("Attempting GitHub LFS fallback for %s", spec.model_id)
        raise NotImplementedError(
            "GitHub LFS download not yet implemented. "
            f"Please download manually: git lfs pull models/vlm/weights/{spec.model_id}"
        )

    def load_model(
        self,
        spec: VLMModelSpec,
        progress_cb: ProgressCallback = None,
    ) -> tuple[Any, Any]:
        """Load model and processor. Returns (model, processor) tuple.

        Model stays in GPU memory until explicitly unloaded (sticky per job).
        """
        if spec.model_id in self._loaded_models:
            return self._loaded_models[spec.model_id]

        model_path = self.ensure_model(spec, progress_cb)
        reassemble_lfs_weight_shards(model_path)

        if progress_cb:
            progress_cb(f"Loading VLM: {spec.model_id}...", 50, 100)

        model, processor = self._load_with_quantization(spec, model_path)
        self._loaded_models[spec.model_id] = (model, processor)

        if progress_cb:
            progress_cb(f"VLM ready: {spec.model_id}", 100, 100)

        return model, processor

    def _load_with_quantization(
        self, spec: VLMModelSpec, model_path: Path,
    ) -> tuple[Any, Any]:
        import torch
        from transformers import AutoProcessor

        processor = AutoProcessor.from_pretrained(
            str(model_path),
            trust_remote_code=True,
            local_files_only=True,
        )

        load_kwargs: dict[str, Any] = {
            "trust_remote_code": True,
            "device_map": "auto",
            "local_files_only": True,
        }

        if spec.quantization == "bnb_4bit" and torch.cuda.is_available():
            try:
                from transformers import BitsAndBytesConfig
                load_kwargs["quantization_config"] = BitsAndBytesConfig(
                    load_in_4bit=True,
                    bnb_4bit_compute_dtype=torch.bfloat16,
                    bnb_4bit_quant_type="nf4",
                )
                log.info("Loading %s with 4-bit quantization", spec.model_id)
            except ImportError:
                log.warning("bitsandbytes not available, loading in FP16")
                load_kwargs["torch_dtype"] = torch.float16
        else:
            load_kwargs["torch_dtype"] = torch.bfloat16 if torch.cuda.is_available() else torch.float32

        if spec.loader == "qwen_vl":
            model = self._load_qwen_vl(model_path, load_kwargs)
        elif spec.loader == "phi_vision":
            model = self._load_phi_vision(model_path, load_kwargs)
        elif spec.loader == "smolvlm":
            model = self._load_smolvlm(model_path, load_kwargs)
        else:
            model = self._load_generic(model_path, load_kwargs)

        return model, processor

    def _load_qwen_vl(self, path: Path, kwargs: dict) -> Any:
        from transformers import AutoModelForImageTextToText
        return AutoModelForImageTextToText.from_pretrained(str(path), **kwargs)

    def _load_phi_vision(self, path: Path, kwargs: dict) -> Any:
        from transformers import AutoModelForCausalLM
        kwargs.setdefault("attn_implementation", "eager")
        return AutoModelForCausalLM.from_pretrained(str(path), **kwargs)

    def _load_smolvlm(self, path: Path, kwargs: dict) -> Any:
        from transformers import AutoModelForImageTextToText
        return AutoModelForImageTextToText.from_pretrained(str(path), **kwargs)

    def _load_generic(self, path: Path, kwargs: dict) -> Any:
        try:
            from transformers import AutoModelForImageTextToText
            return AutoModelForImageTextToText.from_pretrained(str(path), **kwargs)
        except Exception:
            from transformers import AutoModelForCausalLM
            return AutoModelForCausalLM.from_pretrained(str(path), **kwargs)

    def unload_all(self) -> None:
        """Free all loaded models from GPU memory."""
        import gc
        for model_id, (model, _proc) in self._loaded_models.items():
            log.info("Unloading VLM: %s", model_id)
            del model
        self._loaded_models.clear()
        gc.collect()
        try:
            import torch
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except ImportError:
            pass
