from __future__ import annotations

import contextlib
import json
import logging
import os
import resource
import select
import subprocess
import sys
import tempfile
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

log = logging.getLogger(__name__)

# Large RPC payloads spill to a temp file; lines on stdin/stdout stay bounded.
LARGE_RESULT_THRESHOLD = 50 * 1024 * 1024  # 50MB serialized JSON cap before spill-to-file
MAX_JSONRPC_LINE_BYTES = LARGE_RESULT_THRESHOLD + 5 * 1024 * 1024  # 55MB hard per-line cap
MAX_RESULT_FILE_READ_BYTES = LARGE_RESULT_THRESHOLD + 2 * 1024 * 1024  # spilled file on disk

_MAX_RSS_BYTES = 16 * 1024 * 1024 * 1024  # 16 GB per worker
_MAX_CPU_SECONDS = 7200  # 2 hours


def _set_worker_resource_limits() -> None:
    """Called in the child process before exec to enforce resource limits."""
    with contextlib.suppress(ValueError, OSError):
        resource.setrlimit(resource.RLIMIT_AS, (_MAX_RSS_BYTES, _MAX_RSS_BYTES))
    with contextlib.suppress(ValueError, OSError):
        resource.setrlimit(resource.RLIMIT_CPU, (_MAX_CPU_SECONDS, _MAX_CPU_SECONDS))


def _drain_stderr(pipe: Any, buffer: list[str] | None = None, max_lines: int = 200) -> None:
    """Read stderr in a background thread to prevent pipe buffer deadlock.

    If *buffer* is provided, keeps the last *max_lines* lines for diagnostic snapshots.
    """
    try:
        for line in pipe:
            if buffer is not None:
                if len(buffer) >= max_lines:
                    buffer.pop(0)
                buffer.append(line.rstrip("\n"))
    except Exception:
        log.debug("stderr drain pipe error", exc_info=True)

def _is_safe_worker_result_file(path_str: str) -> bool:
    """Reject path traversal / arbitrary file read via forged ``_result_file`` in RPC JSON.

    Legitimate spill files are created with :func:`tempfile.mkstemp` under the system temp
    directory, prefix ``yaruk_rpc_``, suffix ``.json``.
    """
    try:
        p = Path(path_str).resolve()
        tmp_root = Path(tempfile.gettempdir()).resolve()
        if not p.is_relative_to(tmp_root):
            return False
    except (ValueError, OSError):
        return False
    name = p.name
    return name.startswith("yaruk_rpc_") and name.endswith(".json")


def _read_stdout_line_capped(stdout: TextIO, max_bytes: int) -> str | None:
    """Read one line from worker stdout with a byte cap (DoS hardening)."""
    raw = stdout.buffer.readline(max_bytes + 1)
    if len(raw) > max_bytes:
        log.warning("worker stdout line exceeded %s bytes", max_bytes)
        return None
    if not raw:
        return None
    return raw.decode("utf-8", errors="replace")


ENGINE_MODULES: dict[str, str] = {
    "marker": "yaruk.engines.marker.worker",
    "docling": "yaruk.engines.docling.worker",
    "mineru": "yaruk.engines.mineru.worker",
    "markitdown": "yaruk.engines.markitdown.worker",
    "opendataloader": "yaruk.engines.opendataloader.worker",
}


@dataclass(frozen=True)
class WorkerRequest:
    id: str
    method: str
    params: dict[str, Any]


@dataclass(frozen=True)
class WorkerResponse:
    id: str
    ok: bool
    result: dict[str, Any] | None = None
    error: str | None = None


class JsonRpcWorkerClient:
    """Manages a subprocess worker communicating via JSON-RPC over stdin/stdout."""

    def __init__(
        self,
        cmd: list[str],
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        merged_env = dict(os.environ)
        if env:
            merged_env.update(env)

        self._proc = subprocess.Popen(
            cmd,
            cwd=str(cwd) if cwd else None,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=merged_env,
            preexec_fn=_set_worker_resource_limits,
        )
        if not self._proc.stdin or not self._proc.stdout:
            raise RuntimeError("Worker pipes not available")

        self._stderr_lines: list[str] = []
        self._stderr_thread = threading.Thread(
            target=_drain_stderr, args=(self._proc.stderr, self._stderr_lines), daemon=True,
        )
        self._stderr_thread.start()

    @property
    def alive(self) -> bool:
        return self._proc.poll() is None

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc else None

    def stderr_snapshot(self, tail: int = 30) -> str:
        """Return the last *tail* lines of captured stderr."""
        return "\n".join(self._stderr_lines[-tail:])

    def _read_response(self, req_id: str) -> WorkerResponse:
        """Blocking read of a single JSON-RPC response line from stdout."""
        assert self._proc.stdout is not None
        line = _read_stdout_line_capped(self._proc.stdout, MAX_JSONRPC_LINE_BYTES)
        if line is None:
            return WorkerResponse(
                id=req_id, ok=False, error="response-line-too-large-or-empty",
            )
        line = line.strip()
        if not line:
            return WorkerResponse(id=req_id, ok=False, error="no-response: worker closed stdout")

        obj = json.loads(line)
        result = obj.get("result")

        if isinstance(result, dict) and "_result_file" in result:
            result_path = result["_result_file"]
            if not isinstance(result_path, str) or not _is_safe_worker_result_file(result_path):
                log.warning("rejected unsafe _result_file path from worker")
                return WorkerResponse(id=req_id, ok=False, error="invalid-result-file-path")
            try:
                sz = os.path.getsize(result_path)
                if sz > MAX_RESULT_FILE_READ_BYTES:
                    with contextlib.suppress(OSError):
                        os.unlink(result_path)
                    return WorkerResponse(id=req_id, ok=False, error="result-file-too-large")
                with open(result_path, encoding="utf-8") as rf:
                    full_obj = json.load(rf)
                os.unlink(result_path)
                result = full_obj.get("result")
            except Exception as e:
                log.warning("result-file-read failed for %s: %s", result_path, e)
                return WorkerResponse(id=req_id, ok=False, error=f"result-file-read: {e}")

        return WorkerResponse(
            id=str(obj.get("id", req_id)),
            ok=bool(obj.get("ok", False)),
            result=result,
            error=obj.get("error"),
        )

    def request(self, req: WorkerRequest, timeout_s: float | None = None) -> WorkerResponse:
        """Send a request and block until a response arrives.

        ``timeout_s=None`` (default) means wait indefinitely.
        """
        if not self.alive:
            return WorkerResponse(id=req.id, ok=False, error="worker-dead")

        if not self._proc.stdin or not self._proc.stdout:
            raise RuntimeError("Worker not started")

        payload = {"id": req.id, "method": req.method, "params": req.params}
        try:
            self._proc.stdin.write(json.dumps(payload) + "\n")
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            return WorkerResponse(id=req.id, ok=False, error=f"write-error: {e}")

        try:
            if timeout_s is not None:
                ready, _, _ = select.select([self._proc.stdout], [], [], timeout_s)
                if not ready:
                    log.warning("worker request timed out after %ss, NOT killing — will keep waiting", timeout_s)
            return self._read_response(req.id)
        except json.JSONDecodeError as e:
            return WorkerResponse(id=req.id, ok=False, error=f"invalid-json: {e}")
        except Exception as e:
            return WorkerResponse(id=req.id, ok=False, error=f"read-error: {e}")

    def request_with_watchdog(
        self,
        req: WorkerRequest,
        stall_timeout_s: float = 1800.0,
        grace_s: float = 120.0,
    ) -> WorkerResponse:
        """Send a request with a stall watchdog.

        Unlike a hard timeout, the watchdog only intervenes when no progress is
        detected for *stall_timeout_s* seconds.  The worker process keeps running
        as long as its stderr shows new output (indicating work is happening).

        Sequence on stall:
        1. Log warning + stderr snapshot
        2. Send health ping in background
        3. Wait *grace_s* more
        4. If still stalled → kill worker, return error
        """
        if not self.alive:
            return WorkerResponse(id=req.id, ok=False, error="worker-dead")
        if not self._proc.stdin or not self._proc.stdout:
            raise RuntimeError("Worker not started")

        payload = {"id": req.id, "method": req.method, "params": req.params}
        try:
            self._proc.stdin.write(json.dumps(payload) + "\n")
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            return WorkerResponse(id=req.id, ok=False, error=f"write-error: {e}")

        import time
        poll_interval = 30.0
        last_stderr_len = len(self._stderr_lines)
        stall_start: float | None = None

        while True:
            ready, _, _ = select.select([self._proc.stdout], [], [], poll_interval)
            if ready:
                try:
                    return self._read_response(req.id)
                except json.JSONDecodeError as e:
                    return WorkerResponse(id=req.id, ok=False, error=f"invalid-json: {e}")
                except Exception as e:
                    return WorkerResponse(id=req.id, ok=False, error=f"read-error: {e}")

            if not self.alive:
                return WorkerResponse(id=req.id, ok=False, error="worker-died-during-request")

            cur_stderr_len = len(self._stderr_lines)
            if cur_stderr_len > last_stderr_len:
                last_stderr_len = cur_stderr_len
                stall_start = None
                continue

            if stall_start is None:
                stall_start = time.monotonic()
                continue

            stalled_for = time.monotonic() - stall_start
            if stalled_for < stall_timeout_s:
                continue

            log.warning(
                "STALL WATCHDOG: worker %s stalled for %.0fs (threshold %.0fs). "
                "stderr snapshot:\n%s",
                req.method, stalled_for, stall_timeout_s,
                self.stderr_snapshot(20),
            )

            log.info("STALL WATCHDOG: grace period %.0fs starting for %s", grace_s, req.method)
            grace_ready, _, _ = select.select([self._proc.stdout], [], [], grace_s)
            if grace_ready:
                try:
                    return self._read_response(req.id)
                except Exception as e:
                    return WorkerResponse(id=req.id, ok=False, error=f"read-error-after-grace: {e}")

            log.error(
                "STALL WATCHDOG: killing stalled worker (pid=%s) for %s after %.0fs total stall",
                self.pid, req.method, stalled_for + grace_s,
            )
            self.close()
            return WorkerResponse(
                id=req.id, ok=False,
                error=f"stall-watchdog: no progress for {stalled_for + grace_s:.0f}s",
            )

    def health_check(self, timeout_s: float | None = 60.0) -> WorkerResponse:
        return self.request(
            WorkerRequest(id=f"health-{uuid.uuid4().hex[:6]}", method="health", params={}),
            timeout_s=timeout_s,
        )

    def close(self) -> None:
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._proc.kill()

    def __del__(self) -> None:
        self.close()


def _resolve_venv_python() -> str:
    """Find the .venv/bin/python in the project tree, falling back to sys.executable."""
    candidates = [
        Path(__file__).resolve().parents[3] / ".venv" / "bin" / "python",
        Path.cwd() / ".venv" / "bin" / "python",
    ]
    for p in candidates:
        if p.is_file():
            return str(p)
    return sys.executable


class WorkerPool:
    """Manages a pool of worker subprocess clients by engine name."""

    def __init__(self, python_path: str | None = None) -> None:
        self._clients: dict[str, JsonRpcWorkerClient] = {}
        self._python = python_path or _resolve_venv_python()

    def get_or_start(self, engine_name: str, timeout_s: float | None = None) -> JsonRpcWorkerClient:
        client = self._clients.get(engine_name)
        if client and client.alive:
            return client

        module = ENGINE_MODULES.get(engine_name)
        if not module:
            raise ValueError(f"Unknown engine: {engine_name}")

        src_dir = str(Path(__file__).resolve().parents[2])

        venv_site_pkgs = ""
        venv_base = Path(self._python).parent.parent
        venv_lib = venv_base / "lib"
        if venv_lib.is_dir():
            for d in venv_lib.iterdir():
                sp = d / "site-packages"
                if sp.is_dir():
                    venv_site_pkgs = str(sp)
                    break

        if not venv_site_pkgs:
            resolved_base = Path(self._python).resolve().parents[1]
            resolved_lib = resolved_base / "lib"
            if resolved_lib.is_dir():
                for d in resolved_lib.iterdir():
                    sp = d / "site-packages"
                    if sp.is_dir():
                        venv_site_pkgs = str(sp)
                        break

        pypath_parts = [src_dir]
        if venv_site_pkgs:
            pypath_parts.append(venv_site_pkgs)
        env = {"PYTHONPATH": ":".join(pypath_parts)}

        cmd = [self._python, "-m", module]
        log.info("starting worker subprocess: %s (env PYTHONPATH=%s)", " ".join(cmd), env["PYTHONPATH"])
        client = JsonRpcWorkerClient(cmd, env=env)

        health = client.health_check(timeout_s=60.0)
        if not health.ok:
            client.close()
            raise RuntimeError(
                f"Worker {engine_name} health check failed: {health.error}"
            )

        self._clients[engine_name] = client
        return client

    def request(
        self,
        engine_name: str,
        method: str,
        params: dict[str, Any],
        timeout_s: float | None = None,
        use_watchdog: bool = True,
        stall_timeout_s: float = 1800.0,
        grace_s: float = 120.0,
    ) -> WorkerResponse:
        client = self.get_or_start(engine_name, timeout_s=timeout_s)
        req = WorkerRequest(
            id=f"{engine_name}-{uuid.uuid4().hex[:8]}",
            method=method,
            params=params,
        )
        if use_watchdog:
            resp = client.request_with_watchdog(req, stall_timeout_s=stall_timeout_s, grace_s=grace_s)
        else:
            resp = client.request(req, timeout_s=timeout_s)
        if not resp.ok and resp.error and ("worker-dead" in resp.error or "stall-watchdog" in resp.error):
            log.warning("worker %s unavailable (%s), restarting...", engine_name, (resp.error or "")[:80])
            self._clients.pop(engine_name, None)
            client = self.get_or_start(engine_name, timeout_s=timeout_s)
            if use_watchdog:
                resp = client.request_with_watchdog(req, stall_timeout_s=stall_timeout_s, grace_s=grace_s)
            else:
                resp = client.request(req, timeout_s=timeout_s)
        return resp

    def stderr_snapshot(self, engine_name: str, tail: int = 30) -> str:
        """Get recent stderr from a running worker."""
        client = self._clients.get(engine_name)
        if client:
            return client.stderr_snapshot(tail)
        return ""

    def close_all(self) -> None:
        for client in self._clients.values():
            client.close()
        self._clients.clear()

    def close(self, engine_name: str) -> None:
        client = self._clients.pop(engine_name, None)
        if client:
            client.close()


class _Heartbeat:
    """Periodic stderr heartbeat so the parent's stall watchdog sees activity."""

    def __init__(self, interval_s: float = 60.0) -> None:
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._method = ""

    def start(self, method: str) -> None:
        self._method = method
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _loop(self) -> None:
        import time as _time
        seq = 0
        while not self._stop.wait(self._interval):
            seq += 1
            print(
                f"[heartbeat] {self._method} alive seq={seq} t={_time.monotonic():.0f}",
                file=sys.stderr, flush=True,
            )


def run_worker_server(handler: Any) -> None:
    """Minimal JSON-RPC over stdin/stdout server loop.
    handler must implement: handle(method: str, params: dict[str, Any]) -> dict[str, Any]

    Large results (>50MB serialized) are written to a temp file and only
    the file path is sent over the pipe to avoid memory/pipe issues.
    A background heartbeat thread writes to stderr while processing so the
    parent's stall watchdog sees continued activity.
    """
    heartbeat = _Heartbeat(interval_s=60.0)
    stdin = sys.stdin.buffer
    while True:
        raw = stdin.readline(MAX_JSONRPC_LINE_BYTES + 1)
        if not raw:
            break
        if len(raw) > MAX_JSONRPC_LINE_BYTES:
            sys.stdout.write(
                json.dumps(
                    {"id": "", "ok": False, "error": f"request-line-exceeds-{MAX_JSONRPC_LINE_BYTES}-bytes"},
                )
                + "\n",
            )
            sys.stdout.flush()
            continue
        line = raw.decode("utf-8", errors="replace").strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            rid = str(req.get("id", ""))
            method = str(req.get("method", ""))
            params = req.get("params") or {}
            heartbeat.start(method)
            result = handler.handle(method, params)
            heartbeat.stop()
            payload = json.dumps({"id": rid, "ok": True, "result": result})

            if len(payload) > LARGE_RESULT_THRESHOLD:
                fd, tmp_path = tempfile.mkstemp(prefix="yaruk_rpc_", suffix=".json")
                with os.fdopen(fd, "w") as f:
                    f.write(payload)
                ref = json.dumps({"id": rid, "ok": True, "result": {"_result_file": tmp_path}})
                sys.stdout.write(ref + "\n")
            else:
                sys.stdout.write(payload + "\n")
            sys.stdout.flush()
        except Exception as e:
            heartbeat.stop()
            rid = ""
            try:
                rid = str(json.loads(line).get("id", ""))
            except Exception:
                rid = ""
            sys.stdout.write(json.dumps({"id": rid, "ok": False, "error": str(e)}) + "\n")
            sys.stdout.flush()
