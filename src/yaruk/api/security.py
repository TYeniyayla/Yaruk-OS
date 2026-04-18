"""API-facing path and filename guards (untrusted uploads, URL parameters)."""
from __future__ import annotations

import re
from pathlib import Path

# Orchestrator/API ile uyumlu: job-{8 hex}
_JOB_ID_RE = re.compile(r"^job-[a-f0-9]{8}$")


def validate_api_job_id(job_id: str) -> None:
    """Reject path traversal and unexpected id shapes for HTTP endpoints."""
    if not _JOB_ID_RE.fullmatch(job_id):
        raise ValueError("invalid job id")


_PDF_MAGIC = b"%PDF-"


def validate_pdf_magic(content: bytes) -> None:
    """Ensure buffer looks like a PDF (first bytes)."""
    if len(content) < 5 or not content.startswith(_PDF_MAGIC):
        raise ValueError("upload does not look like a PDF (missing %PDF- header)")


def safe_upload_basename(original_name: str | None, default: str = "input.pdf") -> str:
    """Strip directory components and unsafe characters from upload filename."""
    base = Path(original_name or default).name
    if not base or base in (".", ".."):
        return default
    safe = "".join(c for c in base if c.isalnum() or c in "._-")
    if not safe:
        return default
    max_len = 200
    return safe[:max_len]
