"""Completeness judge — LLM scorer.

Determines whether generated SQL fully answers the user's question without
missing dimensions, measures, or filters.
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


def _make_completeness_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def completeness_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: completeness with structured ASI output."""
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

        empty_data_note = ""
        if cmp.get("gt_rows", -1) == 0:
            empty_data_note = (
                "\nIMPORTANT: The ground-truth query returned 0 rows, meaning "
                "the underlying dataset currently has no matching data for this "
                "question. When both queries would return empty results, minor "
                "SQL differences (e.g. filter thresholds) have no practical impact. "
                "Focus your assessment on whether the SQL structure and intent are "
                "correct rather than penalizing differences that only matter when "
                "data exists. If the SQL is structurally sound but differs in a "
                "threshold or filter value that doesn't affect empty results, lean "
                "toward PASS.\n"
            )

        prompt = (
            "You are a SQL completeness expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the generated SQL fully answers the user's question without "
            "missing dimensions, measures, or filters.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n"
            f"{cmp_summary}{empty_data_note}\n"
            'Respond with JSON only: {"complete": true/false, "failure_type": "<missing_column|missing_filter|missing_temporal_filter|missing_aggregation|partial_answer>", '
            '"blame_set": ["<missing_element>"], '
            '"rationale": "<brief explanation>"}\n'
            'If complete, set failure_type to "" and blame_set to [].'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [completeness] INPUT ──────────────────────────────────────────\n"
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
                "┌─── JUDGE [completeness] ERROR ──────────────────────────────────────────\n"
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
                name="completeness",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="completeness",
                    value="unknown",
                    rationale=f"LLM call failed: {e}",
                    metadata=metadata,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        result_matched = cmp.get("match", False)
        result_override = result_matched and not result.get("complete", False)

        logger.info(
            "\n"
            "┌─── JUDGE [completeness] VERDICT ────────────────────────────────────────\n"
            "│ Question:  %s\n"
            "│ Verdict:   %s\n"
            "│ Rationale: %s\n"
            "│ Failure:   type=%s | blame=%s\n"
            "│ Override:  %s\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question[:80],
            "PASS" if result.get("complete") else "FAIL",
            result.get("rationale", "(none)"),
            result.get("failure_type", "n/a"),
            result.get("blame_set", []),
            "result_match -> forced PASS" if result_override else "none",
        )

        if result_override:
            return Feedback(
                name="completeness",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="completeness",
                    value="yes",
                    rationale=(
                        f"OVERRIDE: Results match ({cmp.get('match_type')}). "
                        f"LLM noted SQL differences: {result.get('rationale', '')}"
                    ),
                    extra={"llm_response": result, "override_reason": "result_match"},
                ),
                source=LLM_SOURCE,
            )

        if result.get("complete", False):
            return Feedback(
                name="completeness",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="completeness",
                    value="yes",
                    rationale=result.get("rationale", "Complete"),
                    extra={"llm_response": result},
                ),
                source=LLM_SOURCE,
            )

        base_confidence = 0.95
        if cmp.get("match"):
            base_confidence = 0.5
        elif cmp.get("gt_rows", -1) == 0 and cmp.get("error"):
            base_confidence = 0.3
        elif cmp.get("error"):
            base_confidence = 0.6

        metadata = build_asi_metadata(
            failure_type=result.get("failure_type", "missing_column"),
            severity="major",
            confidence=base_confidence,
            blame_set=result.get("blame_set", []),
            counterfactual_fix="Review column visibility and filter completeness in Genie metadata",
        )
        return Feedback(
            name="completeness",
            value="no",
            rationale=format_asi_markdown(
                judge_name="completeness",
                value="no",
                rationale=result.get("rationale", "Incomplete"),
                metadata=metadata,
                extra={"llm_response": result},
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return completeness_judge
