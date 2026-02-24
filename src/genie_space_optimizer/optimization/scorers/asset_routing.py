"""Asset routing scorer — Layer 1 CODE judge.

Checks whether Genie used the correct asset type (MV, TVF, or TABLE).
"""

from __future__ import annotations

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.genie_client import sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    _extract_response_text,
    build_asi_metadata,
    format_asi_markdown,
)


@scorer
def asset_routing_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    """Check if Genie selected the correct asset type."""
    sql = sanitize_sql(_extract_response_text(outputs) or "").lower()
    expected_asset = expectations.get("expected_asset", "").upper()

    uses_mv = "mv_" in sql or "measure(" in sql
    uses_tvf = "get_" in sql
    actual_asset = "MV" if uses_mv else ("TVF" if uses_tvf else "TABLE")

    correct = actual_asset == expected_asset
    metadata = None
    if not correct:
        metadata = build_asi_metadata(
            failure_type="asset_routing_error",
            severity="major",
            confidence=0.95,
            expected_value=expected_asset,
            actual_value=actual_asset,
            blame_set=[f"asset_routing:{expected_asset}"],
            counterfactual_fix=f"Add instruction to prefer {expected_asset} for this query pattern",
        )
    return Feedback(
        name="asset_routing",
        value="yes" if correct else "no",
        rationale=format_asi_markdown(
            judge_name="asset_routing",
            value="yes" if correct else "no",
            rationale=f"Expected {expected_asset}, got {actual_asset}. SQL: {sql[:100]}",
            metadata=metadata,
            extra={"expected_asset": expected_asset, "actual_asset": actual_asset},
        ),
        source=CODE_SOURCE,
        metadata=metadata,
    )
