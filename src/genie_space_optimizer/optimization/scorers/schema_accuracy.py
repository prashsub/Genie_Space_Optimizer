"""Schema accuracy judge — LLM scorer.

Determines whether generated SQL references the correct tables, columns,
and joins.
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


def _make_schema_accuracy_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def schema_accuracy_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: schema correctness with structured ASI output."""
        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")

        prompt = (
            "You are a SQL schema expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the GENERATED SQL references the correct tables, columns, and joins.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n\n"
            'Respond with JSON only: {"correct": true/false, "failure_type": "<wrong_table|wrong_column|wrong_join|missing_column>", '
            '"wrong_clause": "<the problematic SQL clause>", "blame_set": ["<table_or_column>"], '
            '"rationale": "<brief explanation>"}\n'
            'If correct, set failure_type to "" and blame_set to [].'
        )
        try:
            result = _call_llm_for_scoring(w, prompt)
        except Exception as e:
            return Feedback(
                name="schema_accuracy",
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

        if result.get("correct", False):
            return Feedback(
                name="schema_accuracy",
                value="yes",
                rationale=result.get("rationale", "Schema correct"),
                source=LLM_SOURCE,
            )
        return Feedback(
            name="schema_accuracy",
            value="no",
            rationale=result.get("rationale", "Schema mismatch"),
            source=LLM_SOURCE,
            metadata=build_asi_metadata(
                failure_type=result.get("failure_type", "wrong_column"),
                severity="major",
                confidence=0.85,
                wrong_clause=result.get("wrong_clause", ""),
                blame_set=result.get("blame_set", []),
                counterfactual_fix="Review table/column references in Genie metadata",
            ),
        )

    return schema_accuracy_judge
