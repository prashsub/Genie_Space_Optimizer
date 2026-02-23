"""Semantic equivalence judge — LLM scorer.

Determines whether two SQL queries are semantically equivalent, measuring
the same business metric even if written differently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.genie_client import resolve_sql, sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    LLM_SOURCE,
    _call_llm_for_scoring,
    _extract_response_text,
    build_asi_metadata,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


def _make_semantic_equivalence_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def semantic_equivalence_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: semantic equivalence with structured ASI output."""
        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")

        prompt = (
            "You are a SQL semantics expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the two SQL queries measure the SAME business metric and would "
            "answer the same question, even if written differently.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n\n"
            'Respond with JSON only: {"equivalent": true/false, "failure_type": "<different_metric|different_grain|different_scope>", '
            '"blame_set": ["<metric_or_dimension>"], '
            '"rationale": "<brief explanation>"}\n'
            'If equivalent, set failure_type to "" and blame_set to [].'
        )
        try:
            result = _call_llm_for_scoring(w, prompt)
        except Exception as e:
            return Feedback(
                name="semantic_equivalence",
                value="unknown",
                rationale=f"LLM call failed: {e}",
                source=LLM_SOURCE,
                metadata=build_asi_metadata(
                    failure_type="other",
                    severity="info",
                    confidence=0.0,
                    counterfactual_fix="LLM judge unavailable — retry or check endpoint",
                ),
            )

        if result.get("equivalent", False):
            return Feedback(
                name="semantic_equivalence",
                value="yes",
                rationale=result.get("rationale", "Semantically equivalent"),
                source=LLM_SOURCE,
            )
        return Feedback(
            name="semantic_equivalence",
            value="no",
            rationale=result.get("rationale", "Semantic mismatch"),
            source=LLM_SOURCE,
            metadata=build_asi_metadata(
                failure_type=result.get("failure_type", "different_metric"),
                severity="major",
                confidence=0.80,
                blame_set=result.get("blame_set", []),
                counterfactual_fix="Review measure definitions and grain in Genie metadata",
            ),
        )

    return semantic_equivalence_judge
