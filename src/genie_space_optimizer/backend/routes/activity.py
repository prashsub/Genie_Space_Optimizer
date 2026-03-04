"""Activity endpoint: recent optimization runs for the Dashboard."""

from __future__ import annotations

import logging

from ..core import Dependencies, create_router
from ..models import ActivityItem
from ..utils import ensure_utc_iso, safe_float
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)


@router.get("/activity", response_model=list[ActivityItem], operation_id="getActivity")
def get_activity(
    config: Dependencies.Config,
    space_id: str | None = None,
    limit: int = 20,
):
    """Recent optimization runs for the Dashboard activity table."""
    from genie_space_optimizer.optimization.state import load_recent_activity

    try:
        spark = get_spark()
        df = load_recent_activity(
            spark, config.catalog, config.schema_name,
            space_id=space_id, limit=limit,
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
            run_id_val = row.get("run_id", "")
            items.append(
                ActivityItem(
                    runId=run_id_val,
                    spaceId=row.get("space_id", ""),
                    spaceName=row.get("domain", ""),
                    status=row.get("status", ""),
                    initiatedBy=row.get("triggered_by") or "system",
                    baselineScore=safe_float(baseline_scores.get(run_id_val)),
                    optimizedScore=safe_float(row.get("best_accuracy")),
                    timestamp=ensure_utc_iso(row.get("started_at")) or "",
                )
            )
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
