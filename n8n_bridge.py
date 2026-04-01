"""
outreach/n8n_bridge.py
Durable execution bridge for trades demo jobs.

Python remains the decision layer.
This bridge is only responsible for dispatching durable jobs and recording
what happened, whether the executor is n8n, direct Google Calendar, or manual fallback.
"""
from __future__ import annotations

import json
import os
from typing import Any
from urllib import error, request

from .google_calendar import calendar_configured, create_calendar_event
from .storage.db import create_workflow_job, update_workflow_job


def _workflow_webhook_url(workflow_key: str) -> str:
    normalized = workflow_key.upper()
    specific = os.environ.get(f"N8N_{normalized}_WEBHOOK_URL", "").strip()
    if specific:
        return specific
    return os.environ.get("N8N_TRADES_DEMO_WEBHOOK_URL", "").strip()


def dispatch_workflow(
    conn,
    workflow_key: str,
    payload: dict[str, Any],
    *,
    entity_type: str = "",
    entity_id: str = "",
) -> dict[str, Any]:
    """
    Dispatch a durable job.

    Priority:
    1. n8n webhook if configured
    2. direct Google Calendar for booking workflow
    3. manual local fallback with durable job record
    """
    payload_json = json.dumps(payload, ensure_ascii=True)
    job_id = create_workflow_job(
        conn,
        workflow_key,
        payload_json,
        entity_type=entity_type,
        entity_id=entity_id,
    )

    webhook_url = _workflow_webhook_url(workflow_key)
    if webhook_url:
        try:
            req = request.Request(
                webhook_url,
                data=payload_json.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8") or "{}"
            update_workflow_job(
                conn,
                job_id,
                status="completed",
                external_ref=workflow_key,
                result_payload=body,
            )
            return {"job_id": job_id, "status": "completed", "mode": "n8n", "result": body}
        except error.URLError as exc:
            update_workflow_job(conn, job_id, status="failed", last_error=str(exc))
            return {"job_id": job_id, "status": "failed", "mode": "n8n", "error": str(exc)}

    if workflow_key == "trades_demo_booking" and calendar_configured():
        try:
            result = create_calendar_event(
                summary=payload["summary"],
                description=payload["description"],
                start_iso=payload["start_iso"],
                end_iso=payload["end_iso"],
                timezone_name=payload["timezone"],
                attendee_email=payload.get("attendee_email", ""),
            )
            update_workflow_job(
                conn,
                job_id,
                status="completed",
                external_ref=result.get("id", ""),
                result_payload=json.dumps(result, ensure_ascii=True),
            )
            return {"job_id": job_id, "status": "completed", "mode": "google_calendar", **result}
        except Exception as exc:  # pragma: no cover - external dependency path
            update_workflow_job(conn, job_id, status="failed", last_error=str(exc))
            return {"job_id": job_id, "status": "failed", "mode": "google_calendar", "error": str(exc)}

    update_workflow_job(
        conn,
        job_id,
        status="manual",
        last_error="No n8n webhook or Google Calendar credentials configured.",
    )
    return {
        "job_id": job_id,
        "status": "manual",
        "mode": "local_fallback",
        "error": "No n8n webhook or Google Calendar credentials configured.",
    }
