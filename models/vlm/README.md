# Vision–language models (VLM)

This directory holds metadata and optional **local checkpoints** for Yaruk-OS figure-captioning and vision features.

## Tier list

The authoritative list of model IDs, Hugging Face repos, VRAM hints, and loaders is **`manifest.json`**. Tiers rank from highest quality (1) to smallest footprint (10). At runtime Yaruk-OS selects the best model that fits available GPU memory.

## How it works

1. Free VRAM is estimated on the machine.  
2. The best-fitting tier from `manifest.json` is chosen.  
3. If weights are not present under `weights/<model_id>/`, they can be downloaded from Hugging Face (token may be required for gated models — use `HF_TOKEN`).  
4. Downloaded models are cached under `~/.cache/yaruk/vlm/` by default; project-local `models/vlm/weights/` is preferred when populated.

## Layout

```
models/vlm/
  manifest.json     # Model registry (tiers, repos, loaders)
  README.md         # This file
  weights/          # Optional local checkpoints (often gitignored; use LFS or HF)
    <model_id>/
```

## Licenses

Each upstream model has its own license (Apache-2.0, MIT, Gemma terms, etc.). Review the vendor terms before production use.
