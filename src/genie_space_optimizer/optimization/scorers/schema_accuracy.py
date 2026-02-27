"""Schema accuracy judge — LLM scorer.

Determines whether generated SQL references the correct tables, columns,
and joins.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.config import LLM_ENDPOINT
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

logger = logging.getLogger(__name__)


def _make_schema_accuracy_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def schema_accuracy_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: schema correctness with structured ASI output."""
        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")
        cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}

        cmp_summary = ""
        if cmp:
            cmp_summary = (
                f"\nResult comparison (GT vs Genie): match={cmp.get('match')}, "
                f"match_type={cmp.get('match_type', 'unknown')}, "
                f"gt_rows={cmp.get('gt_rows', '?')}, genie_rows={cmp.get('genie_rows', '?')}\n"
                "NOTE: If results match (match=True), the queries are functionally equivalent "
                "even if SQL syntax differs. Weight this heavily in your assessment.\n"
            )

        prompt = (
            "You are a SQL schema expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the GENERATED SQL references the correct tables, columns, and joins.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n"
            f"{cmp_summary}\n"
            'Respond with JSON only: {"correct": true/false, "failure_type": "<wrong_table|wrong_column|wrong_join|missing_column>", '
            '"wrong_clause": "<the problematic SQL clause>", "blame_set": ["<table_or_column>"], '
            '"rationale": "<brief explanation>"}\n'
            'If correct, set failure_type to "" and blame_set to [].'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [schema_accuracy] INPUT ──────────────────────────────────────\n"
            "│ Question: %s\n"
            "│ Genie SQL:\n"
            "│   %s\n"
            "│ GT SQL:\n"
            "│   %s\n"
            "│ Result Comparison: match=%s | type=%s | gt_rows=%s | genie_rows=%s\n"
            "│ Prompt length: %d chars\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question,
            genie_sql or "(none)",
            gt_sql or "(none)",
            cmp.get("match"), cmp.get("match_type"), cmp.get("gt_rows"), cmp.get("genie_rows"),
            len(prompt),
        )

        try:
            result = _call_llm_for_scoring(w, prompt)
        except Exception as e:
            logger.error(
                "\n"
                "┌─── JUDGE [schema_accuracy] ERROR ──────────────────────────────────────\n"
                "│ Question:    %s\n"
                "│ Error:       %s\n"
                "│ Prompt len:  %d chars\n"
                "│ LLM endpoint: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                question[:80], str(e)[:300], len(prompt), LLM_ENDPOINT,
            )
            metadata = build_asi_metadata(
                failure_type="other",
                severity="info",
                confidence=0.0,
                counterfactual_fix="LLM judge unavailable — retry or check endpoint",
            )
            return Feedback(
                name="schema_accuracy",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="schema_accuracy",
                    value="unknown",
                    rationale=f"LLM call failed: {e}",
                    metadata=metadata,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        result_matched = cmp.get("match", False)
        result_override = result_matched and not result.get("correct", False)

        logger.info(
            "\n"
            "┌─── JUDGE [schema_accuracy] VERDICT ────────────────────────────────────\n"
            "│ Question:  %s\n"
            "│ Verdict:   %s\n"
            "│ Rationale: %s\n"
            "│ Failure:   type=%s | blame=%s | clause=%s\n"
            "│ Override:  %s\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question[:80],
            "PASS" if result.get("correct") else "FAIL",
            result.get("rationale", "(none)"),
            result.get("failure_type", "n/a"),
            result.get("blame_set", []),
            result.get("wrong_clause", "n/a"),
            "result_match -> forced PASS" if result_override else "none",
        )

        if result_override:
            return Feedback(
                name="schema_accuracy",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="schema_accuracy",
                    value="yes",
                    rationale=(
                        f"OVERRIDE: Results match ({cmp.get('match_type')}). "
                        f"LLM noted SQL differences: {result.get('rationale', '')}"
                    ),
                    extra={"llm_response": result, "override_reason": "result_match"},
                ),
                source=LLM_SOURCE,
            )

        if result.get("correct", False):
            return Feedback(
                name="schema_accuracy",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="schema_accuracy",
                    value="yes",
                    rationale=result.get("rationale", "Schema correct"),
                    extra={"llm_response": result},
                ),
                source=LLM_SOURCE,
            )

        base_confidence = 0.95
        if cmp.get("match"):
            base_confidence = 0.5
        elif cmp.get("error"):
            base_confidence = 0.6

        metadata = build_asi_metadata(
            failure_type=result.get("failure_type", "wrong_column"),
            severity="major",
            confidence=base_confidence,
            wrong_clause=result.get("wrong_clause", ""),
            blame_set=result.get("blame_set", []),
            counterfactual_fix="Review table/column references in Genie metadata",
        )
        return Feedback(
            name="schema_accuracy",
            value="no",
            rationale=format_asi_markdown(
                judge_name="schema_accuracy",
                value="no",
                rationale=result.get("rationale", "Schema mismatch"),
                metadata=metadata,
                extra={"llm_response": result},
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return schema_accuracy_judge
