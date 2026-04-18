from __future__ import annotations

import re

import yaruk
from yaruk.version import get_version


def test_get_version_matches_semver_pattern() -> None:
    v = get_version()
    assert re.match(r"^\d+\.\d+\.\d+", v)


def test_package_version_exported() -> None:
    assert yaruk.__version__ == get_version()
