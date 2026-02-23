"""Result correctness scorer — Layer 2 CODE judge.

Compares GT vs Genie result DataFrames using pre-computed comparison
data from the predict function. Does NOT call ``spark.sql()`` directly.
"""

from __future__ import annotations

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    build_asi_metadata,
)


@scorer
def result_correctness_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    """Compare result sets pre-computed in the predict function.

    Reads ``outputs["comparison"]`` — does NOT call spark.sql().
    """
    cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}

    if cmp.get("error"):
        return Feedback(
            name="result_correctness",
            value="no",
            rationale=f"Comparison error: {cmp['error']}",
            source=CODE_SOURCE,
            metadata=build_asi_metadata(
                failure_type="other",
                severity="major",
                confidence=0.7,
                actual_value=cmp.get("error", "")[:100],
            ),
        )

    if cmp.get("match"):
        match_type = cmp.get("match_type", "unknown")
        return Feedback(
            name="result_correctness",
            value="yes",
            rationale=(
                f"Match type: {match_type}. Rows: {cmp.get('gt_rows', '?')}. "
                f"Hash: {cmp.get('gt_hash', 'n/a')}."
            ),
            source=CODE_SOURCE,
        )

    return Feedback(
        name="result_correctness",
        value="no",
        rationale=(
            f"Mismatch. GT rows={cmp.get('gt_rows', '?')} vs "
            f"Genie rows={cmp.get('genie_rows', '?')}. "
            f"Hash GT={cmp.get('gt_hash')} vs Genie={cmp.get('genie_hash')}."
        ),
        source=CODE_SOURCE,
        metadata=build_asi_metadata(
            failure_type="wrong_aggregation",
            severity="major",
            confidence=0.8,
            expected_value=f"rows={cmp.get('gt_rows')}, hash={cmp.get('gt_hash')}",
            actual_value=f"rows={cmp.get('genie_rows')}, hash={cmp.get('genie_hash')}",
            counterfactual_fix="Review Genie metadata for missing joins, filters, or aggregation logic",
        ),
    )
