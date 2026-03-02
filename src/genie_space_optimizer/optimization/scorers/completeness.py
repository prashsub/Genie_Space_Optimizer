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
    build_temporal_note,
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
        question_id = inputs.get("question_id", "")
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

        column_note = ""
        gt_cols = set(cmp.get("gt_columns") or [])
        genie_cols = set(cmp.get("genie_columns") or [])
        extra_gt_cols = gt_cols - genie_cols
        if extra_gt_cols and genie_cols <= gt_cols:
            column_note = (
                f"\nCOLUMN NOTE: The Expected SQL returns {len(extra_gt_cols)} extra column(s) "
                f"not in Generated SQL: {sorted(extra_gt_cols)}. "
                "All Generated SQL columns exist in Expected SQL — this is a column SUBSET. "
                "Evaluate completeness against the USER'S QUESTION only. If the user did NOT "
                "explicitly ask for those extra columns, the Generated SQL is still complete.\n"
            )

        empty_data_note = ""
        if cmp.get("gt_rows", -1) == 0 and cmp.get("genie_rows", -1) > 0:
            empty_data_note = (
                "\nCRITICAL: The Expected SQL returned 0 rows while the Generated SQL "
                "returned data. This likely means the Expected SQL has overly restrictive "
                "filters NOT present in the user's question. Evaluate completeness "
                "against the user's question ONLY. If the Generated SQL includes "
                "everything the user asked for, it is complete. PASS.\n"
            )
        elif cmp.get("gt_rows", -1) == 0:
            empty_data_note = (
                "\nIMPORTANT: Both queries returned 0 rows — the underlying dataset "
                "has no matching data. Minor SQL differences have no practical impact. "
                "Lean toward PASS if structurally sound.\n"
            )

        prompt = (
            "You are a SQL completeness expert evaluating SQL for a Databricks Genie Space.\n"
            "Determine if the generated SQL fully answers the user's question without "
            "missing dimensions, measures, or filters.\n\n"
            "IMPORTANT RULES:\n"
            "1. Evaluate completeness against the USER'S QUESTION, not the Expected SQL.\n"
            "   The Expected SQL is a reference that may return extra columns, dimensions, or measures\n"
            "   that the user never requested. If the Generated SQL includes everything the user\n"
            "   explicitly asked for, it IS complete — do NOT penalize it for omitting extras\n"
            "   that only appear in the Expected SQL. The user's question is the source of truth.\n"
            "2. MEASURE() on a metric view is a complete alternative to SUM/AVG on a fact table.\n"
            "   Similarly, a metric view query and a TVF call are both complete if they return\n"
            "   everything the user asked for. A TVF may return extra columns beyond what the\n"
            "   user requested — that does NOT make the Generated SQL incomplete.\n"
            "3. Defensive IS NOT NULL or ORDER BY clauses are NOT completeness issues.\n"
            "4. GROUP BY ALL is semantically identical to explicit GROUP BY.\n\n"
            f"User question: {question}\n"
            f"Expected SQL: {gt_sql}\n"
            f"Generated SQL: {genie_sql}\n"
            f"{cmp_summary}{column_note}{empty_data_note}{build_temporal_note(cmp)}\n"
            'Respond with JSON only: {"complete": true/false, "failure_type": "<missing_column|missing_filter|missing_temporal_filter|missing_aggregation|partial_answer>", '
            '"blame_set": ["<missing_element>"], '
            '"counterfactual_fix": "<specific Genie Space metadata change that would fix this, referencing exact table/column names>", '
            '"rationale": "<brief explanation>"}\n'
            'If complete, set failure_type to "", blame_set to [], and counterfactual_fix to "".'
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
                    question_id=question_id,
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

        result_matched = cmp.get("match", False)
        _GENIE_ROW_CAP = 5000
        row_cap_hit = (
            cmp.get("genie_rows") == _GENIE_ROW_CAP
            and (cmp.get("gt_rows") or 0) > _GENIE_ROW_CAP
        )
        result_override = (result_matched or row_cap_hit) and not result.get("complete", False)

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
            (
                "result_match -> forced PASS" if result_matched
                else "row_cap -> forced PASS" if row_cap_hit
                else "none"
            ) if result_override else "none",
        )

        if result_override:
            override_reason = "result_match" if result_matched else "row_cap"
            override_detail = cmp.get("match_type", "unknown") if result_matched else f"genie={cmp.get('genie_rows')}/gt={cmp.get('gt_rows')}"
            return Feedback(
                name="completeness",
                value="yes",
                rationale=format_asi_markdown(
                    judge_name="completeness",
                    value="yes",
                    rationale=(
                        f"OVERRIDE ({override_reason}): {override_detail}. "
                        f"LLM noted SQL differences: {result.get('rationale', '')}"
                    ),
                    extra={"llm_response": result, "override_reason": "result_match"},
                    question_id=question_id,
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
                    question_id=question_id,
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
            counterfactual_fix=result.get("counterfactual_fix") or (
                f"Fix {result.get('failure_type', 'completeness issue')} "
                f"involving {', '.join(result.get('blame_set', ['unknown']))}"
            ),
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
                question_id=question_id,
            ),
            source=LLM_SOURCE,
            metadata=metadata,
        )

    return completeness_judge
