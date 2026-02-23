"""Completeness judge — LLM scorer.

Determines whether generated SQL fully answers the user's question without
missing dimensions, measures, or filters.
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


def _make_completeness_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def completeness_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: completeness with structured ASI output."""
        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")

        prompt = (
            "You are a SQL completeness expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the generated SQL fully answers the user's question without "
            "missing dimensions, measures, or filters.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n\n"
            'Respond with JSON only: {"complete": true/false, "failure_type": "<missing_column|missing_filter|missing_temporal_filter|missing_aggregation|partial_answer>", '
            '"blame_set": ["<missing_element>"], '
            '"rationale": "<brief explanation>"}\n'
            'If complete, set failure_type to "" and blame_set to [].'
        )
        try:
            result = _call_llm_for_scoring(w, prompt)
        except Exception as e:
            return Feedback(
                name="completeness",
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

        if result.get("complete", False):
            return Feedback(
                name="completeness",
                value="yes",
                rationale=result.get("rationale", "Complete"),
                source=LLM_SOURCE,
            )
        return Feedback(
            name="completeness",
            value="no",
            rationale=result.get("rationale", "Incomplete"),
            source=LLM_SOURCE,
            metadata=build_asi_metadata(
                failure_type=result.get("failure_type", "missing_column"),
                severity="major",
                confidence=0.80,
                blame_set=result.get("blame_set", []),
                counterfactual_fix="Review column visibility and filter completeness in Genie metadata",
            ),
        )

    return completeness_judge
