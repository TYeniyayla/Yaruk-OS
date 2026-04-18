"""Progress reporting for Orchestrator stages."""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class ProgressEvent:
    stage: str
    current: int
    total: int
    elapsed_s: float
    eta_s: float | None = None
    message: str = ""
    detail: dict[str, object] = field(default_factory=dict)


class ProgressCallback(Protocol):
    def __call__(self, event: ProgressEvent) -> None: ...


class ProgressTracker:
    """Track and report progress through processing stages."""

    def __init__(self, callback: ProgressCallback | None = None) -> None:
        self._callback = callback
        self._stage_start: float = 0.0
        self._stage: str = ""
        self._total: int = 0

    def begin_stage(self, stage: str, total: int) -> None:
        self._stage = stage
        self._total = total
        self._stage_start = time.monotonic()
        self._emit(0)

    def update(self, current: int, message: str = "", **detail: object) -> None:
        self._emit(current, message, detail)

    def finish_stage(self) -> None:
        self._emit(self._total, "done")

    def _emit(self, current: int, message: str = "", detail: dict[str, object] | None = None) -> None:
        if not self._callback:
            return
        elapsed = time.monotonic() - self._stage_start
        eta: float | None = None
        if current > 0 and current < self._total:
            per_item = elapsed / current
            eta = per_item * (self._total - current)
        self._callback(ProgressEvent(
            stage=self._stage,
            current=current,
            total=self._total,
            elapsed_s=elapsed,
            eta_s=eta,
            message=message,
            detail=detail or {},
        ))


def _format_time(seconds: float | None) -> str:
    if seconds is None:
        return "??:??"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def cli_progress_callback(event: ProgressEvent) -> None:
    """Rich-style CLI progress bar printed to stderr."""
    pct = (event.current / event.total * 100) if event.total > 0 else 0
    bar_width = 30
    filled = int(bar_width * event.current / event.total) if event.total > 0 else 0
    bar = "\u2588" * filled + "\u2591" * (bar_width - filled)

    elapsed_str = _format_time(event.elapsed_s)
    eta_str = _format_time(event.eta_s)

    engine_info = ""
    if event.detail.get("engine"):
        engine_info = f" [{event.detail['engine']}]"
    if event.detail.get("providers"):
        engine_info = f" [{', '.join(str(p) for p in event.detail['providers'])}]"  # type: ignore[union-attr]

    msg = event.message
    is_stall = "stalled" in msg.lower() or "fallback" in msg.lower()
    if len(msg) > 50:
        msg = msg[:47] + "..."

    line = (
        f"\r\033[K{event.stage}: {bar} {pct:5.1f}% "
        f"({event.current}/{event.total}) "
        f"[{elapsed_str} < {eta_str}]"
        f"{engine_info}"
    )
    if msg:
        if is_stall:
            line += f" \033[33m{msg}\033[0m"
        else:
            line += f" {msg}"

    sys.stderr.write(line)
    sys.stderr.flush()

    if is_stall:
        sys.stderr.write(
            "\n\033[33m  >> Engine stalled, switching to fallback. Output preserved.\033[0m\n"
        )
        sys.stderr.flush()

    if event.current >= event.total and event.total > 0:
        sys.stderr.write("\n")
        sys.stderr.flush()
