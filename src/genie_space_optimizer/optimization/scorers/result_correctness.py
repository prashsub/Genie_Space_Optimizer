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
    format_asi_markdown,
)


@scorer
def result_correctness_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    """Compare result sets pre-computed in the predict function.

    Reads ``outputs["comparison"]`` — does NOT call spark.sql().
    """
    cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}

    if cmp.get("error"):
        error_type = cmp.get("error_type", "")
        gt_rows = cmp.get("gt_rows", -1)

        if error_type == "genie_result_unavailable" and gt_rows == 0:
            return Feedback(
                name="result_correctness",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="result_correctness",
                    value="yes",
                    rationale=(
                        "GT returned 0 rows and Genie results unavailable — "
                        "treated as matching empty results (null-result defense)."
                    ),
                    extra={"comparison": cmp},
                ),
                source=CODE_SOURCE,
            )

        _GT_INFRA_ERROR_TYPES = frozenset({
            "infrastructure", "permission_blocked", "query_execution",
        })
        if error_type in _GT_INFRA_ERROR_TYPES:
            return Feedback(
                name="result_correctness",
                value="excluded",
                rationale=format_asi_markdown(
                    judge_name="result_correctness",
                    value="excluded",
                    rationale=(
                        f"GT-side failure ({error_type}): {cmp['error'][:200]} "
                        f"— excluded from accuracy denominator."
                    ),
                    extra={"comparison": cmp},
                ),
                source=CODE_SOURCE,
            )

        metadata = build_asi_metadata(
            failure_type="other",
            severity="major",
            confidence=0.7,
            actual_value=cmp.get("error", "")[:100],
        )
        return Feedback(
            name="result_correctness",
            value="no",
            rationale=format_asi_markdown(
                judge_name="result_correctness",
                value="no",
                rationale=f"Comparison error: {cmp['error']}",
                metadata=metadata,
                extra={"comparison": cmp},
            ),
            source=CODE_SOURCE,
            metadata=metadata,
        )

    if cmp.get("match"):
        match_type = cmp.get("match_type", "unknown")
        return Feedback(
            name="result_correctness",
            value="yes",
            rationale=format_asi_markdown(
                judge_name="result_correctness",
                value="yes",
                rationale=(
                    f"Match type: {match_type}. Rows: {cmp.get('gt_rows', '?')}. "
                    f"Hash: {cmp.get('gt_hash', 'n/a')}."
                ),
                extra={"comparison": cmp},
            ),
            source=CODE_SOURCE,
        )

    gt_rows = cmp.get("gt_rows", -1)
    genie_rows = cmp.get("genie_rows", -1)
    if gt_rows == 0 and genie_rows == 0:
        return Feedback(
            name="result_correctness",
            value="yes",
            rationale=format_asi_markdown(
                judge_name="result_correctness",
                value="yes",
                rationale=(
                    "Both GT and Genie returned 0 rows — underlying dataset "
                    "has no matching data. Treated as matching empty results."
                ),
                extra={"comparison": cmp},
            ),
            source=CODE_SOURCE,
        )

    metadata = build_asi_metadata(
        failure_type="wrong_aggregation",
        severity="major",
        confidence=0.8,
        expected_value=f"rows={cmp.get('gt_rows')}, hash={cmp.get('gt_hash')}",
        actual_value=f"rows={cmp.get('genie_rows')}, hash={cmp.get('genie_hash')}",
        counterfactual_fix="Review Genie metadata for missing joins, filters, or aggregation logic",
    )
    return Feedback(
        name="result_correctness",
        value="no",
        rationale=format_asi_markdown(
            judge_name="result_correctness",
            value="no",
            rationale=(
                f"Mismatch. GT rows={cmp.get('gt_rows', '?')} vs "
                f"Genie rows={cmp.get('genie_rows', '?')}. "
                f"Hash GT={cmp.get('gt_hash')} vs Genie={cmp.get('genie_hash')}."
            ),
            metadata=metadata,
            extra={"comparison": cmp},
        ),
        source=CODE_SOURCE,
        metadata=metadata,
    )
