"""Repeatability scorer — compares current Genie SQL against a reference SQL.

Detects non-determinism by hashing the current vs. reference SQL outputs.
Used in repeatability evaluation runs to track consistency across iterations.
"""

from __future__ import annotations

import hashlib

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    build_asi_metadata,
    format_asi_markdown,
)


def _sql_hash(sql: str) -> str:
    """Stable 8-char MD5 hash of lowercased SQL."""
    return hashlib.md5((sql or "").strip().lower().encode()).hexdigest()[:8]


@scorer
def repeatability_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    """Judge whether the current Genie SQL is consistent with a reference SQL.

    Compares ``outputs["response"]`` (current run) against
    ``expectations["previous_sql"]`` (reference from a prior iteration).
    """
    previous_sql = (expectations or {}).get("previous_sql", "")
    current_sql = (outputs or {}).get("response", "")

    if not previous_sql:
        return Feedback(
            name="repeatability",
            value="yes",
            rationale=format_asi_markdown(
                judge_name="repeatability",
                value="yes",
                rationale="No reference SQL available; first evaluation — marked consistent.",
            ),
            source=CODE_SOURCE,
        )

    if not current_sql:
        metadata = build_asi_metadata(
            failure_type="repeatability_issue",
            severity="major",
            confidence=0.9,
            expected_value=f"SQL (hash={_sql_hash(previous_sql)})",
            actual_value="No SQL returned",
            counterfactual_fix="Investigate why Genie returned no SQL for this question",
        )
        return Feedback(
            name="repeatability",
            value="no",
            rationale=format_asi_markdown(
                judge_name="repeatability",
                value="no",
                rationale="Genie returned no SQL this run but did in the reference run.",
                metadata=metadata,
            ),
            source=CODE_SOURCE,
            metadata=metadata,
        )

    prev_hash = _sql_hash(previous_sql)
    curr_hash = _sql_hash(current_sql)

    if prev_hash == curr_hash:
        return Feedback(
            name="repeatability",
            value="yes",
            rationale=format_asi_markdown(
                judge_name="repeatability",
                value="yes",
                rationale=f"SQL identical to reference (hash={curr_hash}).",
            ),
            source=CODE_SOURCE,
        )

    metadata = build_asi_metadata(
        failure_type="repeatability_issue",
        severity="minor" if _structurally_similar(previous_sql, current_sql) else "major",
        confidence=0.85,
        expected_value=f"SQL hash={prev_hash}",
        actual_value=f"SQL hash={curr_hash}",
        counterfactual_fix=(
            "Add structured metadata (business_definition, synonyms, "
            "preferred_questions) to reduce non-determinism."
        ),
    )
    return Feedback(
        name="repeatability",
        value="no",
        rationale=format_asi_markdown(
            judge_name="repeatability",
            value="no",
            rationale=(
                f"SQL changed between runs. "
                f"Reference hash={prev_hash}, current hash={curr_hash}."
            ),
            metadata=metadata,
            extra={
                "previous_sql_preview": previous_sql[:200],
                "current_sql_preview": current_sql[:200],
            },
        ),
        source=CODE_SOURCE,
        metadata=metadata,
    )


def _structurally_similar(sql_a: str, sql_b: str) -> bool:
    """Quick heuristic: are two SQL strings structurally similar?

    Checks whether both reference the same tables/aliases even if
    column order or formatting differs.
    """
    import re

    def _tokens(s: str) -> set[str]:
        return set(re.findall(r"\b\w+\b", s.lower()))

    a_tokens = _tokens(sql_a)
    b_tokens = _tokens(sql_b)
    if not a_tokens or not b_tokens:
        return False
    overlap = len(a_tokens & b_tokens) / max(len(a_tokens), len(b_tokens))
    return overlap > 0.7
