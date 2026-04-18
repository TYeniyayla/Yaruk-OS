from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any

from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from yaruk.core.config import YarukSettings
from yaruk.core.hardware import probe_hardware
from yaruk.core.memory_guard import DynamicMemoryGuard
from yaruk.core.orchestrator import Orchestrator, OrchestratorConfig


class ConvertThread(QThread):
    finished = Signal(str, int)
    error = Signal(str, str)

    def __init__(self, pdf_path: Path, output_dir: Path, parent: Any = None) -> None:
        super().__init__(parent)
        self._pdf = pdf_path
        self._out = output_dir

    def run(self) -> None:
        try:
            settings = YarukSettings()
            cfg = OrchestratorConfig(settings=settings, output_dir=self._out)
            orch = Orchestrator(cfg)
            result = orch.process_sync(self._pdf)
            self.finished.emit(str(self._pdf), result.total_pages)
        except Exception as exc:
            self.error.emit(str(self._pdf), str(exc))


class YarukMainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Yaruk-OS")
        self.setMinimumSize(800, 500)
        self._output_dir = Path(tempfile.mkdtemp(prefix="yaruk-gui-"))
        self._threads: list[ConvertThread] = []

        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)

        left = QVBoxLayout()
        self._btn_open = QPushButton("PDF Dosya Sec")
        self._btn_open.clicked.connect(self._open_file)
        left.addWidget(self._btn_open)
        self._job_list = QListWidget()
        left.addWidget(QLabel("Kuyruk:"))
        left.addWidget(self._job_list)

        hw = probe_hardware()
        guard = DynamicMemoryGuard(YarukSettings())
        mem = guard.decide()
        gpu_text = "GPU Kullanilabilir" if mem.can_use_gpu else "CPU-only mod"
        left.addWidget(QLabel(f"{gpu_text} | RAM: {hw.total_ram_mb or '?'}MB"))

        right = QVBoxLayout()
        right.addWidget(QLabel("Onizleme:"))
        self._preview = QTextEdit()
        self._preview.setReadOnly(True)
        right.addWidget(self._preview)

        layout.addLayout(left, 1)
        layout.addLayout(right, 2)

    def _open_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "PDF Sec", "", "PDF Files (*.pdf)")
        if not path:
            return
        pdf_path = Path(path)
        self._job_list.addItem(f"{pdf_path.name} - isleniyor...")
        thread = ConvertThread(pdf_path, self._output_dir, self)
        thread.finished.connect(self._on_done)
        thread.error.connect(self._on_error)
        self._threads.append(thread)
        thread.start()

    def _on_done(self, path: str, pages: int) -> None:
        name = Path(path).name
        for i in range(self._job_list.count()):
            item = self._job_list.item(i)
            if item and name in (item.text() or ""):
                item.setText(f"{name} - tamamlandi ({pages} sayfa)")
        self._preview.setPlainText(f"{name}: {pages} sayfa basariyla islendi.\nCikti: {self._output_dir}")

    def _on_error(self, path: str, error: str) -> None:
        name = Path(path).name
        for i in range(self._job_list.count()):
            item = self._job_list.item(i)
            if item and name in (item.text() or ""):
                item.setText(f"{name} - HATA")
        QMessageBox.warning(self, "Donusum Hatasi", f"{name}: {error}")


def main() -> None:
    app = QApplication(sys.argv)
    win = YarukMainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

