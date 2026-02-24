"""Activity endpoint: recent optimization runs for the Dashboard."""

from __future__ import annotations

import logging
import math

from ..core import Dependencies, create_router
from ..models import ActivityItem
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
        for _, row in df.iterrows():
            items.append(
                ActivityItem(
                    runId=row.get("run_id", ""),
                    spaceId=row.get("space_id", ""),
                    spaceName=row.get("domain", ""),
                    status=row.get("status", ""),
                    initiatedBy=row.get("triggered_by") or "system",
                    baselineScore=_safe_float(row.get("best_accuracy")),
                    optimizedScore=_safe_float(row.get("best_accuracy")),
                    timestamp=str(row.get("started_at", "")),
                )
            )
    return items


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None
