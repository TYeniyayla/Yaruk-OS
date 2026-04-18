from __future__ import annotations

import pytest

from yaruk.models.output_contract import OutputLayout


def test_for_job_resolves_under_root(tmp_path) -> None:
    layout = OutputLayout.for_job(tmp_path, "test-job")
    assert layout.job_dir.name == "test-job"
    assert layout.job_dir.is_relative_to(tmp_path.resolve())


def test_for_job_rejects_traversal(tmp_path) -> None:
    with pytest.raises(ValueError):
        OutputLayout.for_job(tmp_path, "..")
    with pytest.raises(ValueError):
        OutputLayout.for_job(tmp_path, "a/b")
