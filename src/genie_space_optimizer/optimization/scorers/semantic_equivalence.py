"""Semantic equivalence judge — LLM scorer.

Determines whether two SQL queries are semantically equivalent, measuring
the same business metric even if written differently.
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


def _make_semantic_equivalence_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def semantic_equivalence_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: semantic equivalence with structured ASI output."""
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
            "You are a SQL semantics expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the two SQL queries measure the SAME business metric and would "
            "answer the same question, even if written differently.\n\n"
            "IMPORTANT — Equivalence rules:\n"
            "- If the Generated SQL answers the user's question correctly, it IS semantically\n"
            "  equivalent even if the Expected SQL returns extra columns the user did not ask for.\n"
            "  Extra columns in the Expected SQL do not change the business metric.\n"
            "- ILIKE '%value%' vs = 'value' for proper nouns (cities, countries) are equivalent.\n"
            "- Defensive IS NOT NULL guards do not change the business metric being measured.\n"
            "- Focus on whether BOTH queries answer the SAME question, not whether they produce\n"
            "  byte-identical output.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n"
            f"{cmp_summary}{empty_data_note}\n"
            'Respond with JSON only: {"equivalent": true/false, "failure_type": "<different_metric|different_grain|different_scope>", '
            '"blame_set": ["<metric_or_dimension>"], '
            '"rationale": "<brief explanation>"}\n'
            'If equivalent, set failure_type to "" and blame_set to [].'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [semantic_equivalence] INPUT ──────────────────────────────────\n"
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
                "┌─── JUDGE [semantic_equivalence] ERROR ──────────────────────────────────\n"
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
                name="semantic_equivalence",
                value="unknown",
                rationale=format_asi_markdown(
                    judge_name="semantic_equivalence",
                    value="unknown",
                    rationale=f"LLM call failed: {e}",
                    metadata=metadata,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        result_matched = cmp.get("match", False)
        result_override = result_matched and not result.get("equivalent", False)

        logger.info(
            "\n"
            "┌─── JUDGE [semantic_equivalence] VERDICT ────────────────────────────────\n"
            "│ Question:  %s\n"
            "│ Verdict:   %s\n"
            "│ Rationale: %s\n"
            "│ Failure:   type=%s | blame=%s\n"
            "│ Override:  %s\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question[:80],
            "PASS" if result.get("equivalent") else "FAIL",
            result.get("rationale", "(none)"),
            result.get("failure_type", "n/a"),
            result.get("blame_set", []),
            "result_match -> forced PASS" if result_override else "none",
        )

        if result_override:
            return Feedback(
                name="semantic_equivalence",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="semantic_equivalence",
                    value="yes",
                    rationale=(
                        f"OVERRIDE: Results match ({cmp.get('match_type')}). "
                        f"LLM noted SQL differences: {result.get('rationale', '')}"
                    ),
                    extra={"llm_response": result, "override_reason": "result_match"},
                ),
                source=LLM_SOURCE,
            )

        if result.get("equivalent", False):
            return Feedback(
                name="semantic_equivalence",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="semantic_equivalence",
                    value="yes",
                    rationale=result.get("rationale", "Semantically equivalent"),
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
            failure_type=result.get("failure_type", "different_metric"),
            severity="major",
            confidence=base_confidence,
            blame_set=result.get("blame_set", []),
            counterfactual_fix="Review measure definitions and grain in Genie metadata",
        )
        return Feedback(
            name="semantic_equivalence",
            value="no",
            rationale=format_asi_markdown(
                judge_name="semantic_equivalence",
                value="no",
                rationale=result.get("rationale", "Semantic mismatch"),
                metadata=metadata,
                extra={"llm_response": result},
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return semantic_equivalence_judge
