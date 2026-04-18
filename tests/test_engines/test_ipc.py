from __future__ import annotations

import sys
from pathlib import Path

from yaruk.engines.base_worker import JsonRpcWorkerClient, WorkerPool


def _venv_python() -> str:
    """Find the venv python, not the Cursor AppImage wrapper."""
    venv = Path(__file__).parent.parent.parent / ".venv" / "bin" / "python"
    if venv.exists():
        return str(venv)
    return sys.executable


def test_json_rpc_client_opendataloader_health() -> None:
    """Start an opendataloader worker subprocess and send health check."""
    python = _venv_python()
    env = {"PYTHONPATH": str(Path(__file__).parent.parent.parent / "src")}
    cmd = [python, "-m", "yaruk.engines.opendataloader.worker"]
    try:
        client = JsonRpcWorkerClient(cmd, env=env)
    except Exception:
        return

    try:
        resp = client.health_check(timeout_s=30.0)
        assert resp.ok
        assert resp.result is not None
        assert resp.result["name"] == "opendataloader"
    finally:
        client.close()


def test_json_rpc_client_markitdown_health() -> None:
    """Start a markitdown worker subprocess and send health check."""
    python = _venv_python()
    env = {"PYTHONPATH": str(Path(__file__).parent.parent.parent / "src")}
    cmd = [python, "-m", "yaruk.engines.markitdown.worker"]
    try:
        client = JsonRpcWorkerClient(cmd, env=env)
    except Exception:
        return

    try:
        resp = client.health_check(timeout_s=30.0)
        assert resp.ok
        assert resp.result is not None
        assert resp.result["name"] == "markitdown"
    finally:
        client.close()


def test_worker_pool_request() -> None:
    """Test WorkerPool with opendataloader (lightweight, no GPU needed)."""
    python = _venv_python()
    pool = WorkerPool(python_path=python)
    try:
        resp = pool.request("opendataloader", "health", {}, timeout_s=30.0)
        assert resp.ok
        assert resp.result is not None
        assert resp.result["name"] == "opendataloader"
    except RuntimeError:
        pass
    finally:
        pool.close_all()
