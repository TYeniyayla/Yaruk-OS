from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def tmp_db_path(tmp_path: Path) -> Path:
    return tmp_path / "yaruk.sqlite"


@pytest.fixture
def fixtures_path() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def test_output_dir(tmp_path: Path) -> Path:
    out = tmp_path / "output"
    out.mkdir(exist_ok=True)
    return out
