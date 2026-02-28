"""Programmatic trigger and status endpoints for external callers (CI/CD, scripts, etc.).

These endpoints reuse the exact same code path as the UI-driven optimization trigger,
including OBO authentication via the Databricks Apps proxy.
"""

from __future__ import annotations

import logging

from fastapi import HTTPException

from ..core import Dependencies, create_router
from ..models import RunStatusResponse, TriggerRequest, TriggerResponse
from ..utils import ensure_utc_iso, safe_float
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)


@router.post(
    "/trigger",
    response_model=TriggerResponse,
    operation_id="triggerOptimization",
)
def trigger_optimization(
    body: TriggerRequest,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
):
    """Trigger an optimization run programmatically.

    Behaves identically to the UI-driven ``POST /spaces/{space_id}/optimize``
    endpoint.  The Databricks Apps proxy injects the caller's OBO token and
    identity headers automatically for any authenticated request.
    """
    from .spaces import do_start_optimization

    result = do_start_optimization(
        space_id=body.space_id,
        ws=ws,
        sp_ws=sp_ws,
        config=config,
        headers=headers,
        apply_mode=body.apply_mode,
    )
    return TriggerResponse(
        runId=result.runId,
        jobRunId=result.jobRunId,
        jobUrl=result.jobUrl,
        status="IN_PROGRESS",
    )


@router.get(
    "/trigger/status/{run_id}",
    response_model=RunStatusResponse,
    operation_id="getTriggerStatus",
)
def get_trigger_status(
    run_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
):
    """Poll the status of a triggered optimization run.

    Returns a lightweight status payload suitable for polling loops.
    """
    from genie_space_optimizer.optimization.state import load_run
    from .runs import _reconcile_single_run

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    run_data = _reconcile_single_run(spark, run_data, sp_ws, config.catalog, config.schema_name)

    status = run_data.get("status", "QUEUED")
    if status == "IN_PROGRESS":
        status = "RUNNING"

    return RunStatusResponse(
        runId=run_id,
        status=status,
        spaceId=run_data.get("space_id", ""),
        startedAt=ensure_utc_iso(run_data.get("started_at")),
        completedAt=ensure_utc_iso(run_data.get("completed_at")),
        baselineScore=safe_float(run_data.get("best_accuracy")),
        optimizedScore=safe_float(run_data.get("best_accuracy")),
        convergenceReason=run_data.get("convergence_reason"),
    )
