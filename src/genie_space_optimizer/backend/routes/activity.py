"""Activity endpoint: recent optimization runs for the Dashboard.

Permission filtering: results are restricted to Genie Spaces where the
calling user has CAN_MANAGE or CAN_EDIT.  This prevents leaking run
history for spaces the user cannot access.
"""

from __future__ import annotations

import logging

from ..core import Dependencies, create_router
from ..models import ActivityItem
from ..utils import ensure_utc_iso, safe_float
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)


def _get_accessible_space_ids(
    ws, sp_ws, caller_email: str,
) -> set[str] | None:
    """Return space IDs the user can manage/edit, or None to skip filtering."""
    from genie_space_optimizer.common.genie_client import (
        list_spaces,
        user_can_edit_space,
    )

    try:
        all_spaces: list[dict] = []
        for client in [ws, sp_ws]:
            try:
                all_spaces = list_spaces(client)
                break
            except Exception:
                continue
        if not all_spaces:
            return set()
        return {
            s["id"]
            for s in all_spaces
            if user_can_edit_space(ws, s["id"], user_email=caller_email, acl_client=sp_ws)
        }
    except Exception:
        logger.warning("Could not resolve accessible spaces for activity filter", exc_info=True)
        return None


@router.get("/activity", response_model=list[ActivityItem], operation_id="getActivity")
def get_activity(
    config: Dependencies.Config,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    headers: Dependencies.Headers,
    space_id: str | None = None,
    limit: int = 20,
):
    """Recent optimization runs for the Dashboard activity table.

    Results are filtered to only include spaces where the calling user
    has CAN_MANAGE or CAN_EDIT permission.
    """
    from genie_space_optimizer.optimization.state import load_recent_activity

    caller_email = (headers.user_email or headers.user_name or "").lower()
    accessible_ids = _get_accessible_space_ids(ws, sp_ws, caller_email)

    if space_id and accessible_ids is not None and space_id not in accessible_ids:
        return []

    try:
        spark = get_spark()
        df = load_recent_activity(
            spark, config.catalog, config.schema_name,
            space_id=space_id, limit=limit * 3 if accessible_ids is not None else limit,
        )
    except Exception:
        logger.debug("Delta tables not yet available, returning empty activity")
        return []

    items: list[ActivityItem] = []
    if not df.empty:
        baseline_scores = _load_baseline_scores(
            spark, list(df["run_id"]), config.catalog, config.schema_name,
        )
        for _, row in df.iterrows():
            row_space_id = row.get("space_id", "")
            if accessible_ids is not None and row_space_id not in accessible_ids:
                continue
            run_id_val = row.get("run_id", "")
            items.append(
                ActivityItem(
                    runId=run_id_val,
                    spaceId=row_space_id,
                    spaceName=row.get("domain", ""),
                    status=row.get("status", ""),
                    initiatedBy=row.get("triggered_by") or "system",
                    baselineScore=safe_float(baseline_scores.get(run_id_val)),
                    optimizedScore=safe_float(row.get("best_accuracy")),
                    timestamp=ensure_utc_iso(row.get("started_at")) or "",
                )
            )
            if len(items) >= limit:
                break
    return items


def _load_baseline_scores(
    spark, run_ids: list[str], catalog: str, schema: str,
) -> dict[str, float | None]:
    """Bulk-fetch baseline accuracy (iteration 0, full eval) for a list of runs."""
    from genie_space_optimizer.common.config import TABLE_ITERATIONS
    from genie_space_optimizer.common.delta_helpers import _fqn, run_query

    if not run_ids:
        return {}
    fqn = _fqn(catalog, schema, TABLE_ITERATIONS)
    ids_csv = ", ".join(f"'{rid}'" for rid in run_ids)
    try:
        df = run_query(
            spark,
            f"SELECT run_id, overall_accuracy FROM {fqn} "
            f"WHERE run_id IN ({ids_csv}) AND iteration = 0 AND eval_scope = 'full'",
        )
    except Exception:
        return {}
    result: dict[str, float | None] = {}
    if not df.empty:
        for _, row in df.iterrows():
            result[row.get("run_id", "")] = safe_float(row.get("overall_accuracy"))
    return result
