from __future__ import annotations

import json

from yaruk.engines.base_worker import WorkerRequest, WorkerResponse


def test_worker_request_serialization() -> None:
    req = WorkerRequest(id="1", method="health", params={})
    payload = {"id": req.id, "method": req.method, "params": req.params}
    serialized = json.dumps(payload)
    parsed = json.loads(serialized)
    assert parsed["method"] == "health"


def test_worker_response_ok() -> None:
    resp = WorkerResponse(id="1", ok=True, result={"name": "marker"})
    assert resp.ok
    assert resp.result is not None
    assert resp.error is None


def test_worker_response_error() -> None:
    resp = WorkerResponse(id="1", ok=False, error="something went wrong")
    assert not resp.ok
    assert resp.error == "something went wrong"
