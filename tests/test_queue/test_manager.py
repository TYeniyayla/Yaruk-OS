from __future__ import annotations

from pathlib import Path

from yaruk.models.enums import JobStatus
from yaruk.queue.manager import QueueConfig, QueueManager


def test_queue_recovery(tmp_db_path: Path) -> None:
    qm = QueueManager(QueueConfig(db_path=tmp_db_path))
    job = qm.create_job("j1", Path("a.pdf"))
    qm.update_job_status(job.id, JobStatus.RUNNING)
    recovered = qm.recover_running_jobs()
    assert "j1" in recovered

