from __future__ import annotations

from datetime import UTC, datetime

from sqlmodel import Field, SQLModel

from yaruk.models.enums import JobStatus


class Job(SQLModel, table=True):
    id: str = Field(primary_key=True)
    source_path: str
    status: JobStatus = Field(default=JobStatus.PENDING)
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    progress: float = Field(default=0.0)
    error_msg: str | None = None


class PageTask(SQLModel, table=True):
    id: str = Field(primary_key=True)
    job_id: str = Field(index=True)
    page_number: int
    status: JobStatus = Field(default=JobStatus.PENDING)
    assigned_provider: str | None = None
    result_cache_key: str | None = None

