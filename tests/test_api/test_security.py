from __future__ import annotations

import pytest

from yaruk.api.security import safe_upload_basename, validate_api_job_id


def test_validate_api_job_id_accepts_orchestrator_format() -> None:
    validate_api_job_id("job-abcdef12")


def test_validate_api_job_id_rejects_traversal() -> None:
    with pytest.raises(ValueError):
        validate_api_job_id("../job-abcdef12")
    with pytest.raises(ValueError):
        validate_api_job_id("job-../../x")


def test_safe_upload_basename_strips_paths() -> None:
    assert safe_upload_basename("../../../etc/passwd") == "passwd"
    assert safe_upload_basename("doc.pdf") == "doc.pdf"
