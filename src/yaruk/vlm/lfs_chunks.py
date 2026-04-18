"""Split safetensors for GitHub LFS (max ~2 GiB per blob).

On disk: `model.safetensors.__lfs.part00`, `.__lfs.part01`, … plus
`model.safetensors.__lfs_manifest.json`. At runtime, shards are concatenated
back to the original filename before loading.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# GitHub.com LFS: object size <= 2^31 bytes; stay below with margin
CHUNK_BYTES_DEFAULT = 1900 * 1024 * 1024
MANIFEST_SUFFIX = ".__lfs_manifest.json"
BUF_SIZE = 8 * 1024 * 1024


def _manifest_path_for_target(target: Path) -> Path:
    return target.parent / f"{target.name}{MANIFEST_SUFFIX}"


def reassemble_lfs_weight_shards(model_dir: Path) -> None:
    """Concatenate LFS shard parts under model_dir back to original filenames."""
    for manifest_path in sorted(model_dir.rglob(f"*{MANIFEST_SUFFIX}")):
        _reassemble_one_manifest(manifest_path)


def _reassemble_one_manifest(manifest_path: Path) -> None:
    try:
        data: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("Cannot read LFS manifest %s: %s", manifest_path, e)
        return

    if data.get("version") != 1:
        log.warning("Unsupported LFS manifest version: %s", manifest_path)
        return

    target_name = data.get("target")
    parts = data.get("parts")
    total_bytes = data.get("total_bytes")
    expected_sha = data.get("sha256")

    if not target_name or not isinstance(parts, list) or not parts or total_bytes is None:
        log.warning("Invalid LFS manifest: %s", manifest_path)
        return

    target = manifest_path.parent / target_name
    if target.exists() and target.stat().st_size == int(total_bytes):
        if expected_sha:
            if _file_sha256(target) == expected_sha:
                return
        else:
            return

    log.info("Reassembling VLM weight shards: %s", target_name)

    hasher = hashlib.sha256()
    try:
        with target.open("wb") as out:
            for part_name in parts:
                part_path = manifest_path.parent / part_name
                if not part_path.is_file():
                    raise FileNotFoundError(f"Missing shard: {part_path}")
                with part_path.open("rb") as inp:
                    while True:
                        buf = inp.read(BUF_SIZE)
                        if not buf:
                            break
                        hasher.update(buf)
                        out.write(buf)
    except OSError:
        if target.exists():
            with contextlib.suppress(OSError):
                target.unlink()
        raise

    if target.stat().st_size != int(total_bytes):
        target.unlink(missing_ok=True)
        raise RuntimeError(f"Reassembled size mismatch: {target}")

    if expected_sha and hasher.hexdigest() != expected_sha:
        target.unlink(missing_ok=True)
        raise RuntimeError(f"SHA256 mismatch after reassemble: {target}")


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            buf = f.read(BUF_SIZE)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def split_oversized_safetensors(
    weights_root: Path,
    chunk_bytes: int = CHUNK_BYTES_DEFAULT,
) -> list[str]:
    """Split *.safetensors larger than chunk_bytes; write sidecar manifests.

    Returns relative paths (under weights_root) that were split.
    """
    done: list[str] = []
    weights_root = weights_root.resolve()
    if not weights_root.is_dir():
        raise FileNotFoundError(weights_root)

    for path in sorted(weights_root.rglob("*.safetensors")):
        if ".__lfs.part" in path.name or path.name.endswith(MANIFEST_SUFFIX):
            continue

        size = path.stat().st_size
        if size <= chunk_bytes:
            continue

        manifest = _manifest_path_for_target(path)
        if manifest.exists():
            continue

        parts, digest = _write_chunks(path, chunk_bytes)
        if not parts:
            continue
        assembled = sum((path.parent / p).stat().st_size for p in parts)
        if assembled != size:
            for p in parts:
                (path.parent / p).unlink(missing_ok=True)
            raise RuntimeError(f"Shard size sum mismatch: {path}")

        payload = {
            "version": 1,
            "target": path.name,
            "parts": parts,
            "total_bytes": size,
            "sha256": digest,
        }
        manifest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        path.unlink()
        done.append(str(path.relative_to(weights_root)))

    return done


def _write_chunks(path: Path, chunk_bytes: int) -> tuple[list[str], str]:
    base = path.name
    parts: list[str] = []
    hasher = hashlib.sha256()
    part_idx = 0

    with path.open("rb") as src:
        while True:
            part_name = f"{base}.__lfs.part{part_idx:02d}"
            part_path = path.parent / part_name
            written = 0
            with part_path.open("wb") as out:
                while written < chunk_bytes:
                    n = min(BUF_SIZE, chunk_bytes - written)
                    buf = src.read(n)
                    if not buf:
                        break
                    hasher.update(buf)
                    out.write(buf)
                    written += len(buf)

            if written == 0:
                if part_path.exists():
                    part_path.unlink()
                break

            parts.append(part_name)
            part_idx += 1
            if written < chunk_bytes:
                break

    return parts, hasher.hexdigest()
