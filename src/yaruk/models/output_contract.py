from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel


def _safe_job_dir(output_root: Path, job_id: str) -> Path:
    """Resolve job directory under *output_root*; reject path traversal (.., separators)."""
    if not job_id or not job_id.strip():
        msg = "job_id is required"
        raise ValueError(msg)
    if os.sep in job_id or (os.altsep and os.altsep in job_id):
        msg = "job_id must not contain path separators"
        raise ValueError(msg)
    root = output_root.expanduser().resolve()
    job_dir = (root / job_id).resolve()
    try:
        job_dir.relative_to(root)
    except ValueError as e:
        msg = "job_id escapes output directory"
        raise ValueError(msg) from e
    return job_dir


class OutputLayout(BaseModel):
    """Sabitlenmis cikti klasor/dosya semasi. Tum fazlar ve testler buna uyar."""

    job_dir: Path
    metadata_json: Path
    pages_dir: Path
    assets_dir: Path
    asset_index_json: Path
    merged_md: Path
    merged_json: Path

    @classmethod
    def for_job(cls, output_root: Path, job_id: str) -> OutputLayout:
        job_dir = _safe_job_dir(output_root, job_id)
        return cls(
            job_dir=job_dir,
            metadata_json=job_dir / "metadata.json",
            pages_dir=job_dir / "pages",
            assets_dir=job_dir / "assets",
            asset_index_json=job_dir / "assets" / "asset_index.json",
            merged_md=job_dir / "merged.md",
            merged_json=job_dir / "merged.json",
        )

    def ensure_dirs(self) -> None:
        self.job_dir.mkdir(parents=True, exist_ok=True)
        self.pages_dir.mkdir(parents=True, exist_ok=True)
        self.assets_dir.mkdir(parents=True, exist_ok=True)
