"""Logical accuracy judge — LLM scorer.

Determines whether generated SQL applies correct aggregations, filters,
GROUP BY, ORDER BY, and WHERE clauses.
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
    format_asi_markdown,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


def _make_logical_accuracy_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def logical_accuracy_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: logical correctness with structured ASI output."""
        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")

        prompt = (
            "You are a SQL logic expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the GENERATED SQL applies correct aggregations, filters, "
            "GROUP BY, ORDER BY, and WHERE clauses for the business question.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n\n"
            'Respond with JSON only: {"correct": true/false, "failure_type": "<wrong_aggregation|wrong_filter|wrong_groupby|wrong_orderby>", '
            '"wrong_clause": "<the problematic SQL clause>", "blame_set": ["<column_or_function>"], '
            '"rationale": "<brief explanation>"}\n'
            'If correct, set failure_type to "" and blame_set to [].'
        )
        try:
            result = _call_llm_for_scoring(w, prompt)
        except Exception as e:
            metadata = build_asi_metadata(
                failure_type="other",
                severity="info",
                confidence=0.0,
                counterfactual_fix="LLM judge unavailable — retry or check endpoint",
            )
            return Feedback(
                name="logical_accuracy",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="logical_accuracy",
                    value="unknown",
                    rationale=f"LLM call failed: {e}",
                    metadata=metadata,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        if result.get("correct", False):
            return Feedback(
                name="logical_accuracy",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="logical_accuracy",
                    value="yes",
                    rationale=result.get("rationale", "Logic correct"),
                    extra={"llm_response": result},
                ),
                source=LLM_SOURCE,
            )
        metadata = build_asi_metadata(
            failure_type=result.get("failure_type", "wrong_aggregation"),
            severity="major",
            confidence=0.85,
            wrong_clause=result.get("wrong_clause", ""),
            blame_set=result.get("blame_set", []),
            counterfactual_fix="Review aggregation/filter logic in Genie metadata",
        )
        return Feedback(
            name="logical_accuracy",
            value="no",
            rationale=format_asi_markdown(
                judge_name="logical_accuracy",
                value="no",
                rationale=result.get("rationale", "Logic mismatch"),
                metadata=metadata,
                extra={"llm_response": result},
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return logical_accuracy_judge
