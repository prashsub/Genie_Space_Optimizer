"""Suggestion endpoints: list, review, apply."""

from __future__ import annotations

import json
import logging

from fastapi import HTTPException

from ..core import Dependencies, create_router
from ..models import SuggestionOut, SuggestionReviewRequest
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)


def _row_to_suggestion(row: dict) -> SuggestionOut:
    aff_raw = row.get("affected_questions", "[]")
    try:
        affected = json.loads(aff_raw) if isinstance(aff_raw, str) else (aff_raw or [])
    except (json.JSONDecodeError, TypeError):
        affected = []

    return SuggestionOut(
        suggestionId=row.get("suggestion_id", ""),
        runId=row.get("run_id", ""),
        spaceId=row.get("space_id", ""),
        iteration=row.get("iteration"),
        suggestionType=row.get("type", ""),
        title=row.get("title", ""),
        rationale=row.get("rationale"),
        definition=row.get("definition"),
        affectedQuestions=affected,
        estimatedImpact=row.get("estimated_impact"),
        status=row.get("status", "PROPOSED"),
        reviewedBy=row.get("reviewed_by"),
        reviewedAt=row.get("reviewed_at"),
    )


@router.get(
    "/runs/{run_id}/suggestions",
    response_model=list[SuggestionOut],
    operation_id="getImprovementSuggestions",
)
def get_suggestions(run_id: str, config: Dependencies.Config):
    from genie_space_optimizer.optimization.state import load_suggestions

    spark = get_spark()
    df = load_suggestions(spark, run_id, config.catalog, config.schema_name)

    if df.empty:
        return []

    return [_row_to_suggestion(row.to_dict()) for _, row in df.iterrows()]


@router.post(
    "/suggestions/{suggestion_id}/review",
    response_model=SuggestionOut,
    operation_id="reviewSuggestion",
)
def review_suggestion(
    suggestion_id: str,
    body: SuggestionReviewRequest,
    config: Dependencies.Config,
    user_ws: Dependencies.UserClient,
):
    from genie_space_optimizer.optimization.state import (
        load_suggestion_by_id,
        update_suggestion_status,
    )

    if body.status not in {"ACCEPTED", "REJECTED"}:
        raise HTTPException(status_code=400, detail="status must be ACCEPTED or REJECTED")

    spark = get_spark()

    existing = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Suggestion not found")

    try:
        reviewer = user_ws.current_user.me().user_name
    except Exception:
        reviewer = None

    update_suggestion_status(
        spark, suggestion_id, config.catalog, config.schema_name,
        status=body.status, reviewed_by=reviewer,
    )

    updated = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not updated:
        raise HTTPException(status_code=404, detail="Suggestion not found after update")
    return _row_to_suggestion(updated)


@router.post(
    "/suggestions/{suggestion_id}/apply",
    response_model=SuggestionOut,
    operation_id="applySuggestion",
)
def apply_suggestion(
    suggestion_id: str,
    config: Dependencies.Config,
    ws: Dependencies.Client,
):
    from genie_space_optimizer.optimization.state import (
        load_suggestion_by_id,
        update_suggestion_status,
    )

    spark = get_spark()

    row = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not row:
        raise HTTPException(status_code=404, detail="Suggestion not found")

    if row.get("status") != "ACCEPTED":
        raise HTTPException(status_code=400, detail="Suggestion must be ACCEPTED before applying")

    definition = (row.get("definition") or "").strip()
    if not definition:
        raise HTTPException(status_code=400, detail="Suggestion has no SQL definition")

    try:
        ws.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=definition,
            wait_timeout="120s",
        )
    except Exception as exc:
        logger.error("Failed to apply suggestion %s: %s", suggestion_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {exc}") from exc

    update_suggestion_status(
        spark, suggestion_id, config.catalog, config.schema_name,
        status="IMPLEMENTED",
    )

    updated = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not updated:
        raise HTTPException(status_code=404, detail="Suggestion not found after apply")
    return _row_to_suggestion(updated)
