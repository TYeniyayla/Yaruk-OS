from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile
from fastapi.responses import FileResponse

from yaruk.api.security import (
    safe_upload_basename,
    validate_api_job_id,
    validate_pdf_magic,
)
from yaruk.core.config import YarukSettings
from yaruk.core.orchestrator import Orchestrator, OrchestratorConfig
from yaruk.queue.manager import QueueConfig, QueueManager

router = APIRouter()

_OUTPUT_ROOT = Path(tempfile.gettempdir()) / "yaruk-api-output"
_DB_PATH = _OUTPUT_ROOT / ".yaruk_api.sqlite"
_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

_processing_jobs: dict[str, str] = {}


def _get_queue() -> QueueManager:
    return QueueManager(QueueConfig(db_path=_DB_PATH))


def _get_orchestrator() -> Orchestrator:
    settings = YarukSettings()
    cfg = OrchestratorConfig(settings=settings, output_dir=_OUTPUT_ROOT, db_path=_DB_PATH)
    return Orchestrator(cfg)


def _process_job(job_id: str, pdf_path: Path) -> None:
    orch = _get_orchestrator()
    try:
        _processing_jobs[job_id] = "running"
        orch.process_sync(pdf_path)
        _processing_jobs[job_id] = "done"
    except Exception as exc:
        _processing_jobs[job_id] = f"failed: {exc}"
    finally:
        if pdf_path.exists():
            pdf_path.unlink(missing_ok=True)


@router.post("/convert")
async def convert(
    file: UploadFile,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    settings = YarukSettings()
    job_id = f"job-{uuid.uuid4().hex[:8]}"
    temp_dir = _OUTPUT_ROOT / "uploads"
    temp_dir.mkdir(parents=True, exist_ok=True)
    safe_name = safe_upload_basename(file.filename)
    pdf_path = temp_dir / f"{job_id}_{safe_name}"

    content = await file.read()
    if len(content) > settings.api_max_upload_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Upload too large (max {settings.api_max_upload_bytes} bytes)",
        )
    if settings.api_require_pdf_magic:
        try:
            validate_pdf_magic(content)
        except ValueError as e:
            raise HTTPException(status_code=415, detail=str(e)) from None
    pdf_path.write_bytes(content)

    qm = _get_queue()
    qm.create_job(job_id, pdf_path)
    _processing_jobs[job_id] = "accepted"

    background_tasks.add_task(_process_job, job_id, pdf_path)

    return {"job_id": job_id, "status": "accepted"}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str) -> dict[str, Any]:
    try:
        validate_api_job_id(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job id") from None
    qm = _get_queue()
    jobs = qm.list_jobs()
    for job in jobs:
        if job.id == job_id:
            return {
                "job_id": job.id,
                "status": job.status.value,
                "source": job.source_path,
                "progress": job.progress,
                "error": job.error_msg,
                "created_at": job.created_at.isoformat(),
            }
    runtime_status = _processing_jobs.get(job_id)
    if runtime_status:
        return {"job_id": job_id, "status": runtime_status}
    raise HTTPException(status_code=404, detail="Job not found")


@router.get("/jobs/{job_id}/result")
async def get_result(job_id: str) -> dict[str, Any]:
    from yaruk.models.output_contract import OutputLayout

    try:
        validate_api_job_id(job_id)
        layout = OutputLayout.for_job(_OUTPUT_ROOT, job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job id") from None
    if not layout.merged_json.exists():
        raise HTTPException(status_code=404, detail="Result not ready or job not found")

    import json
    result_data = json.loads(layout.merged_json.read_text(encoding="utf-8"))
    return {"job_id": job_id, "status": "done", "result": result_data}


@router.get("/jobs/{job_id}/download")
async def download_result(job_id: str) -> FileResponse:
    from yaruk.models.output_contract import OutputLayout

    try:
        validate_api_job_id(job_id)
        layout = OutputLayout.for_job(_OUTPUT_ROOT, job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job id") from None
    if not layout.merged_md.exists():
        raise HTTPException(status_code=404, detail="Result not ready")
    return FileResponse(
        path=str(layout.merged_md),
        filename=f"{job_id}_merged.md",
        media_type="text/markdown",
    )


@router.get("/jobs")
async def list_jobs() -> dict[str, Any]:
    qm = _get_queue()
    jobs = qm.list_jobs()
    return {
        "jobs": [
            {
                "job_id": j.id,
                "status": j.status.value,
                "source": j.source_path,
                "created_at": j.created_at.isoformat(),
            }
            for j in jobs
        ]
    }
