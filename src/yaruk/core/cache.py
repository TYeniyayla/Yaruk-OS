"""Input hash cache: avoids reprocessing unchanged PDFs.

MasterPlan — Cache System:
- Input hash (PDF SHA-256) deduplication
- Page-level granular cache
- Optional TTL + max-entry eviction
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_BUF_SIZE = 256 * 1024  # 256 KB read chunks
_HASH_HEX_LEN = 64


def _is_sha256_hex_dirname(name: str) -> bool:
    if len(name) != _HASH_HEX_LEN:
        return False
    try:
        int(name, 16)
    except ValueError:
        return False
    return True


def file_sha256(path: Path) -> str:
    """Compute SHA-256 hex digest of a file (streaming, memory-safe)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_BUF_SIZE)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


class DiskCache:
    """Flat-file JSON cache keyed by (file_hash, engine, schema_version).

    Directory layout::

        cache_dir/
          {sha256}/
            {engine}.json      # full conversion result
            meta.json           # {source_path, mtime, sha256, schema_version}
    """

    def __init__(
        self,
        cache_dir: Path,
        *,
        max_entries: int = 512,
        ttl_seconds: int | None = None,
    ) -> None:
        self._root = cache_dir
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds

    def _entry_dir(self, file_hash: str, engine: str) -> Path:
        return self._root / file_hash

    def _entry_path(self, file_hash: str, engine: str) -> Path:
        return self._root / file_hash / f"{engine}.json"

    def get(self, file_hash: str, engine: str, schema_version: str = "v1") -> dict[str, Any] | None:
        p = self._entry_path(file_hash, engine)
        if not p.exists():
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if data.get("schema_version") != schema_version:
                log.info("cache schema mismatch for %s/%s — invalidating", file_hash[:12], engine)
                p.unlink(missing_ok=True)
                return None
            log.info("disk cache hit: %s/%s", file_hash[:12], engine)
            return data  # type: ignore[no-any-return]
        except Exception:
            log.debug("disk cache read error for %s/%s", file_hash[:12], engine, exc_info=True)
            return None

    def put(
        self,
        file_hash: str,
        engine: str,
        data: dict[str, Any],
        source_path: Path | None = None,
        schema_version: str = "v1",
    ) -> None:
        d = self._entry_dir(file_hash, engine)
        d.mkdir(parents=True, exist_ok=True)
        data["schema_version"] = schema_version
        self._entry_path(file_hash, engine).write_text(
            json.dumps(data, default=str), encoding="utf-8",
        )
        meta_path = d / "meta.json"
        if not meta_path.exists() and source_path:
            meta = {
                "sha256": file_hash,
                "source_path": str(source_path),
                "schema_version": schema_version,
            }
            meta_path.write_text(json.dumps(meta), encoding="utf-8")
        log.info("disk cache write: %s/%s", file_hash[:12], engine)
        self._maybe_evict()

    def _hash_dirs(self) -> list[Path]:
        if not self._root.is_dir():
            return []
        out: list[Path] = []
        for p in self._root.iterdir():
            if p.is_dir() and _is_sha256_hex_dirname(p.name):
                out.append(p)
        return out

    def _dir_mtime(self, d: Path) -> float:
        try:
            return max((f.stat().st_mtime for f in d.iterdir()), default=d.stat().st_mtime)
        except OSError:
            return 0.0

    def _maybe_evict(self) -> None:
        """TTL purge first, then LRU by oldest directory mtime until under max_entries."""
        dirs = self._hash_dirs()
        now = time.time()

        if self._ttl_seconds and self._ttl_seconds > 0:
            cutoff = now - float(self._ttl_seconds)
            for d in dirs:
                if self._dir_mtime(d) < cutoff:
                    try:
                        for f in d.iterdir():
                            f.unlink(missing_ok=True)
                        d.rmdir()
                        log.info("disk cache TTL evicted: %s", d.name[:12])
                    except OSError as e:
                        log.debug("cache ttl evict failed %s: %s", d, e)
            dirs = self._hash_dirs()

        if len(dirs) <= self._max_entries:
            return

        scored = sorted(((self._dir_mtime(d), d) for d in dirs), key=lambda x: x[0])
        to_remove = len(dirs) - self._max_entries
        for _, d in scored[:to_remove]:
            try:
                for f in d.iterdir():
                    f.unlink(missing_ok=True)
                d.rmdir()
                log.info("disk cache LRU evicted: %s", d.name[:12])
            except OSError as e:
                log.debug("cache lru evict failed %s: %s", d, e)

    def invalidate(self, file_hash: str, engine: str | None = None) -> None:
        if engine:
            p = self._entry_path(file_hash, engine)
            p.unlink(missing_ok=True)
        else:
            d = self._root / file_hash
            if d.is_dir():
                for f in d.iterdir():
                    f.unlink(missing_ok=True)
                d.rmdir()
        log.info("disk cache invalidated: %s/%s", file_hash[:12], engine or "*")
