from __future__ import annotations

import tempfile
from pathlib import Path

from yaruk.engines.base_worker import _is_safe_worker_result_file


def test_safe_result_file_accepts_temp_yaruk_prefix() -> None:
    p = Path(tempfile.gettempdir()) / "yaruk_rpc_abcd.json"
    assert _is_safe_worker_result_file(str(p))


def test_safe_result_file_rejects_outside_temp() -> None:
    assert not _is_safe_worker_result_file("/etc/passwd")
    assert not _is_safe_worker_result_file(str(Path.home() / "yaruk_rpc_x.json"))


def test_safe_result_file_rejects_wrong_name() -> None:
    p = Path(tempfile.gettempdir()) / "other.json"
    assert not _is_safe_worker_result_file(str(p))


def test_safe_result_file_rejects_traversal() -> None:
    assert not _is_safe_worker_result_file(str(Path(tempfile.gettempdir()) / ".." / "etc" / "passwd"))
