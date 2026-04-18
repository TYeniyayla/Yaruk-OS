from __future__ import annotations

import tempfile
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static

from yaruk.core.config import YarukSettings
from yaruk.core.hardware import probe_hardware
from yaruk.core.memory_guard import DynamicMemoryGuard
from yaruk.core.orchestrator import Orchestrator, OrchestratorConfig
from yaruk.core.progress import ProgressEvent
from yaruk.models.enums import JobStatus
from yaruk.queue.manager import QueueConfig, QueueManager


def _fmt_time(seconds: float | None) -> str:
    if seconds is None:
        return "??:??"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"


class YarukTui(App[None]):
    TITLE = "Yaruk-OS"
    CSS = """
    #left-panel { width: 55%; }
    #right-panel { width: 45%; }
    #preview { height: 1fr; overflow: auto; }
    #hw-bar { dock: bottom; height: 3; }
    #progress-bar { dock: bottom; height: 3; }
    #input-bar { dock: bottom; height: 3; }
    """
    BINDINGS = [  # noqa: RUF012
        Binding("q", "quit", "Cikis"),
        Binding("r", "refresh", "Yenile"),
        Binding("d", "toggle_diff", "Diff"),
        Binding("a", "add_file", "Dosya Ekle"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._output_dir = Path(tempfile.mkdtemp(prefix="yaruk-tui-"))
        self._db_path = self._output_dir / ".yaruk_tui.sqlite"
        self._settings = YarukSettings()
        self._queue = QueueManager(QueueConfig(db_path=self._db_path))
        self._preview_text = ""

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="left-panel"):
                yield Static("[b]Gorev Kuyrugu[/b]")
                yield DataTable(id="job-table")
            with Vertical(id="right-panel"):
                yield Static("[b]On Izleme[/b]")
                yield Static("", id="preview")
        yield Static("", id="progress-bar")
        yield self._hw_status_bar()
        yield Input(placeholder="PDF dosya yolu girin ve Enter'a basin...", id="input-bar")
        yield Footer()

    def _hw_status_bar(self) -> Static:
        hw = probe_hardware()
        guard = DynamicMemoryGuard(self._settings)
        mem = guard.decide()
        gpu = "GPU OK" if mem.can_use_gpu else "CPU-only"
        ram = f"RAM: {hw.total_ram_mb or '?'}MB"
        return Static(f"[dim]{gpu} | {ram} | Batch: {mem.max_batch_size}[/dim]", id="hw-bar")

    def on_mount(self) -> None:
        table = self.query_one("#job-table", DataTable)
        table.add_columns("ID", "Dosya", "Durum", "Sayfa")
        self._refresh_table()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        path_str = event.value.strip()
        if not path_str:
            return
        pdf_path = Path(path_str)
        if not pdf_path.exists():
            self.notify(f"Dosya bulunamadi: {pdf_path}", severity="error")
            return
        self._start_conversion(pdf_path)
        event.input.value = ""

    def _start_conversion(self, pdf_path: Path) -> None:
        import uuid
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        self._queue.create_job(job_id, pdf_path)
        self._refresh_table()
        self.notify(f"Kuyruga eklendi: {pdf_path.name}")
        self.run_worker(self._process_job(job_id, pdf_path))

    def _tui_progress_callback(self, event: ProgressEvent) -> None:
        pct = (event.current / event.total * 100) if event.total > 0 else 0
        bar_w = 25
        filled = int(bar_w * event.current / event.total) if event.total > 0 else 0
        bar_str = "\u2588" * filled + "\u2591" * (bar_w - filled)
        elapsed = _fmt_time(event.elapsed_s)
        eta = _fmt_time(event.eta_s)
        engine = ""
        if event.detail.get("providers"):
            engine = f" [{', '.join(str(p) for p in event.detail['providers'])}]"  # type: ignore[union-attr]
        elif event.detail.get("engine"):
            engine = f" [{event.detail['engine']}]"

        text = (
            f"[bold]{event.stage}[/bold]: {bar_str} {pct:5.1f}% "
            f"({event.current}/{event.total}) "
            f"[{elapsed} < {eta}]{engine}"
        )
        if event.message:
            text += f" {event.message}"

        self.call_from_thread(self._update_progress_bar, text)

    def _update_progress_bar(self, text: str) -> None:
        import contextlib
        with contextlib.suppress(Exception):
            self.query_one("#progress-bar", Static).update(text)

    async def _process_job(self, job_id: str, pdf_path: Path) -> None:
        self._queue.update_job_status(job_id, JobStatus.RUNNING)
        self._refresh_table()

        cfg = OrchestratorConfig(
            settings=self._settings,
            output_dir=self._output_dir,
            db_path=self._db_path,
        )
        orch = Orchestrator(cfg, progress_callback=self._tui_progress_callback)

        try:
            result = await orch.process(pdf_path)
            self._queue.update_job_status(job_id, JobStatus.DONE)
            preview_lines = [f"# {pdf_path.name}", f"Sayfa: {result.total_pages}", ""]
            for page in result.pages[:3]:
                preview_lines.append(f"## Sayfa {page.page_number}")
                for block in page.blocks[:5]:
                    preview_lines.append(f"  [{block.type.value}] {block.text[:80]}")
                preview_lines.append("")
            self._preview_text = "\n".join(preview_lines)
            self.query_one("#preview", Static).update(self._preview_text)
            self.notify(f"Tamamlandi: {pdf_path.name} ({result.total_pages} sayfa)")
        except Exception as exc:
            self._queue.update_job_status(job_id, JobStatus.FAILED, str(exc))
            self.notify(f"Hata: {exc}", severity="error")

        self.call_from_thread(self._update_progress_bar, "[dim]Bosta[/dim]")
        self._refresh_table()

    def _refresh_table(self) -> None:
        table = self.query_one("#job-table", DataTable)
        table.clear()
        jobs = self._queue.list_jobs()
        for job in jobs:
            fname = Path(job.source_path).name if job.source_path else "?"
            table.add_row(job.id, fname, job.status.value, str(job.progress))

    def action_refresh(self) -> None:
        self._refresh_table()

    def action_toggle_diff(self) -> None:
        preview = self.query_one("#preview", Static)
        if self._preview_text:
            preview.update(self._preview_text)
        else:
            preview.update("[dim]Henuz sonuc yok[/dim]")

    def action_add_file(self) -> None:
        self.query_one("#input-bar", Input).focus()


def main() -> None:
    YarukTui().run()


if __name__ == "__main__":
    main()

