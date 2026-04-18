from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlmodel import Session, SQLModel, create_engine, select

from yaruk.models.enums import JobStatus
from yaruk.queue.models import Job, PageTask


@dataclass(frozen=True)
class QueueConfig:
    db_path: Path


class QueueManager:
    def __init__(self, cfg: QueueConfig) -> None:
        self._engine = create_engine(f"sqlite:///{cfg.db_path}")
        SQLModel.metadata.create_all(self._engine)

    def create_job(self, job_id: str, source_path: Path) -> Job:
        job = Job(id=job_id, source_path=str(source_path), status=JobStatus.PENDING)
        with Session(self._engine) as s:
            s.add(job)
            s.commit()
            s.refresh(job)
        return job

    def update_job_status(self, job_id: str, status: JobStatus, error_msg: str | None = None) -> None:
        with Session(self._engine) as s:
            job = s.get(Job, job_id)
            if not job:
                return
            job.status = status
            job.updated_at = datetime.now(tz=UTC)
            job.error_msg = error_msg
            s.add(job)
            s.commit()

    def get_job(self, job_id: str) -> Job | None:
        with Session(self._engine) as s:
            return s.get(Job, job_id)

    def list_jobs(self) -> list[Job]:
        with Session(self._engine) as s:
            return list(s.exec(select(Job).order_by(Job.created_at.desc())))

    def add_page_tasks(self, job_id: str, total_pages: int) -> None:
        with Session(self._engine) as s:
            for p in range(1, total_pages + 1):
                task = PageTask(id=f"{job_id}-p{p}", job_id=job_id, page_number=p)
                s.add(task)
            s.commit()

    def set_page_done(self, job_id: str, page_number: int) -> None:
        with Session(self._engine) as s:
            tasks = list(
                s.exec(
                    select(PageTask).where(
                        PageTask.job_id == job_id, PageTask.page_number == page_number,
                    )
                )
            )
            for t in tasks:
                t.status = JobStatus.DONE
                s.add(t)
            s.commit()

    def last_completed_page(self, job_id: str) -> int:
        with Session(self._engine) as s:
            tasks = list(
                s.exec(
                    select(PageTask)
                    .where(PageTask.job_id == job_id, PageTask.status == JobStatus.DONE)
                    .order_by(PageTask.page_number.desc())
                )
            )
            return tasks[0].page_number if tasks else 0

    def recover_running_jobs(self) -> list[str]:
        recovered: list[str] = []
        with Session(self._engine) as s:
            jobs = list(s.exec(select(Job).where(Job.status == JobStatus.RUNNING)))
            for job in jobs:
                job.status = JobStatus.PENDING
                job.updated_at = datetime.now(tz=UTC)
                s.add(job)
                recovered.append(job.id)
            s.commit()
        return recovered

