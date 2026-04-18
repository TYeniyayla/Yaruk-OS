#!/usr/bin/env python3
"""Split large safetensors for GitHub LFS (2 GiB per-blob limit).

Run from repo root:
  PYTHONPATH=src python scripts/vlm_split_lfs_parts.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from yaruk.vlm.lfs_chunks import CHUNK_BYTES_DEFAULT, split_oversized_safetensors  # noqa: E402, I001


def main() -> int:
    p = argparse.ArgumentParser(
        description="Split large VLM safetensors into LFS-safe shards",
    )
    p.add_argument(
        "--root",
        type=Path,
        default=ROOT / "models" / "vlm" / "weights",
        help="Weights root (default: models/vlm/weights)",
    )
    p.add_argument(
        "--chunk-mib",
        type=int,
        default=CHUNK_BYTES_DEFAULT // (1024 * 1024),
        help=f"Shard size in MiB (default {CHUNK_BYTES_DEFAULT // (1024 * 1024)})",
    )
    args = p.parse_args()
    chunk_bytes = int(args.chunk_mib) * 1024 * 1024
    if chunk_bytes > 2000 * 1024 * 1024:
        print("chunk-mib must not exceed 2000 (GitHub 2 GiB limit).", file=sys.stderr)
        return 2

    out = split_oversized_safetensors(args.root, chunk_bytes=chunk_bytes)
    for rel in out:
        print("split:", rel)
    print(f"done: {len(out)} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
