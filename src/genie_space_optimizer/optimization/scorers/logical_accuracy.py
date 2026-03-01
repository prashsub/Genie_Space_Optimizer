"""Logical accuracy judge — LLM scorer.

Determines whether generated SQL applies correct aggregations, filters,
GROUP BY, ORDER BY, and WHERE clauses.
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
    build_temporal_note,
    format_asi_markdown,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _make_logical_accuracy_judge(w: WorkspaceClient, catalog: str, schema: str):
    """Factory that binds the workspace client and SQL resolution context."""

    @scorer
    def logical_accuracy_judge(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """LLM judge: logical correctness with structured ASI output."""
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
        if cmp.get("gt_rows", -1) == 0 and cmp.get("genie_rows", -1) > 0:
            empty_data_note = (
                "\nCRITICAL: The Expected SQL returned 0 rows while the Generated SQL "
                "returned data. This likely means the Expected SQL has overly restrictive "
                "filters NOT present in the user's question. Evaluate the Generated SQL "
                "SOLELY against the user's question. If it correctly answers what the "
                "user asked, it should PASS.\n"
            )
        elif cmp.get("gt_rows", -1) == 0:
            empty_data_note = (
                "\nIMPORTANT: Both queries returned 0 rows — the underlying dataset "
                "has no matching data. Minor SQL differences have no practical impact. "
                "Lean toward PASS if structurally sound.\n"
            )

        prompt = (
            "You are a SQL logic expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the GENERATED SQL applies correct aggregations, filters, "
            "GROUP BY, ORDER BY, and WHERE clauses for the business question.\n\n"
            "IMPORTANT — Evaluation rules:\n"
            "- ILIKE '%value%' and = 'value' are functionally equivalent for proper nouns\n"
            "  (city names, country names, etc.). Do NOT penalize defensive/fuzzy matching.\n"
            "- Extra IS NOT NULL guards are defensive programming, not logical errors.\n"
            "  They should not cause a FAIL unless they provably exclude data the user\n"
            "  intended to see.\n"
            "- A missing or extra ORDER BY is a cosmetic difference, not a logic error — do NOT\n"
            "  flag it as wrong_orderby unless ordering is semantically required by the question\n"
            "  (e.g. 'top 10', 'rank by', 'highest').\n"
            "- GROUP BY ALL vs explicit GROUP BY columns are semantically identical — NOT an error.\n"
            "- MEASURE() on a metric view is logically equivalent to SUM/AVG on the underlying\n"
            "  fact table. Do NOT flag metric view vs fact table as wrong_aggregation.\n"
            "- A metric view query and a TVF (table-valued function) call can both produce\n"
            "  logically correct answers. Do NOT flag wrong_aggregation or wrong_filter solely\n"
            "  because one uses a TVF and the other uses a metric view or fact table.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n"
            f"{cmp_summary}{empty_data_note}{build_temporal_note(cmp)}\n"
            'Respond with JSON only: {"correct": true/false, "failure_type": "<wrong_aggregation|wrong_filter|wrong_groupby|wrong_orderby>", '
            '"wrong_clause": "<the problematic SQL clause>", "blame_set": ["<column_or_function>"], '
            '"rationale": "<brief explanation>"}\n'
            'If correct, set failure_type to "" and blame_set to [].'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [logical_accuracy] INPUT ─────────────────────────────────────\n"
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
                "┌─── JUDGE [logical_accuracy] ERROR ─────────────────────────────────────\n"
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

        result_matched = cmp.get("match", False)
        result_override = result_matched and not result.get("correct", False)

        logger.info(
            "\n"
            "┌─── JUDGE [logical_accuracy] VERDICT ───────────────────────────────────\n"
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
                name="logical_accuracy",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="logical_accuracy",
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

        base_confidence = 0.95
        if cmp.get("match"):
            base_confidence = 0.5
        elif cmp.get("gt_rows", -1) == 0 and cmp.get("error"):
            base_confidence = 0.3
        elif cmp.get("error"):
            base_confidence = 0.6

        metadata = build_asi_metadata(
            failure_type=result.get("failure_type", "wrong_aggregation"),
            severity="major",
            confidence=base_confidence,
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
