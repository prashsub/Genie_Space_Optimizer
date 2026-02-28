"""Arbiter scorer — Layer 3 conditional LLM judge.

Fires only when result_correctness reports a mismatch. Arbitrates between
the ground-truth SQL and Genie SQL to determine which is correct.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.config import JUDGE_PROMPTS, LLM_ENDPOINT
from genie_space_optimizer.common.genie_client import resolve_sql, sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    LLM_SOURCE,
    _call_llm_for_scoring,
    _extract_response_text,
    build_asi_metadata,
    format_asi_markdown,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


def _parse_arbiter_verdict(rationale: str) -> str:
    """Extract verdict from arbiter feedback text."""
    text = rationale.lower()
    for v in ("genie_correct", "both_correct", "neither_correct", "ground_truth_correct"):
        if v in text:
            return v
    return "ground_truth_correct"


def _make_arbiter_scorer(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    loaded_prompts: dict[str, str] | None = None,
):
    """Factory that binds the workspace client and SQL resolution context."""
    _loaded = loaded_prompts or {}

    @scorer
    def arbiter_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
        """Conditional scorer that fires only when results disagree.

        Returns value="skipped" when results match (no LLM call).
        """
        cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}

        if cmp.get("match"):
            logger.info(
                "\n"
                "┌─── JUDGE [arbiter] SKIPPED ─────────────────────────────────────────────\n"
                "│ Question: %s\n"
                "│ Reason:   Results match (%s) — arbiter not invoked\n"
                "└─────────────────────────────────────────────────────────────────────────",
                inputs.get("question", "")[:80],
                cmp.get("match_type", "unknown"),
            )
            return Feedback(
                name="arbiter",
                value="skipped",
                rationale=format_asi_markdown(
                    judge_name="arbiter",
                    value="skipped",
                    rationale="Results match — arbiter not invoked.",
                    extra={"comparison": cmp},
                ),
                source=CODE_SOURCE,
            )

        if cmp.get("error"):
            error_type = cmp.get("error_type", "")
            gt_rows = cmp.get("gt_rows", -1)
            if error_type == "genie_result_unavailable" and gt_rows == 0:
                logger.info(
                    "\n"
                    "┌─── JUDGE [arbiter] BOTH_CORRECT (null-result defense) ────────────────\n"
                    "│ Question: %s\n"
                    "│ Reason:   GT returned 0 rows, Genie results unavailable\n"
                    "└─────────────────────────────────────────────────────────────────────────",
                    inputs.get("question", "")[:80],
                )
                return Feedback(
                    name="arbiter",
                    value="both_correct",
                    rationale=format_asi_markdown(
                        judge_name="arbiter",
                        value="both_correct",
                        rationale=(
                            "GT returned 0 rows and Genie results unavailable — "
                            "both are effectively correct (empty result set)."
                        ),
                        extra={"comparison": cmp},
                    ),
                    source=CODE_SOURCE,
                )

            logger.info(
                "\n"
                "┌─── JUDGE [arbiter] SKIPPED ─────────────────────────────────────────────\n"
                "│ Question: %s\n"
                "│ Reason:   SQL execution error — cannot arbitrate\n"
                "│ Error:    %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                inputs.get("question", "")[:80],
                str(cmp["error"])[:200],
            )
            return Feedback(
                name="arbiter",
                value="skipped",
                rationale=format_asi_markdown(
                    judge_name="arbiter",
                    value="skipped",
                    rationale=f"SQL execution error — cannot arbitrate: {cmp['error']}",
                    extra={"comparison": cmp},
                ),
                source=CODE_SOURCE,
            )

        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")

        arbiter_instructions = _loaded.get("arbiter", JUDGE_PROMPTS.get("arbiter", ""))
        gt_sample = cmp.get("gt_sample", "(unavailable)")
        genie_sample = cmp.get("genie_sample", "(unavailable)")
        prompt = (
            f"{arbiter_instructions}\n\n"
            f"Question: {question}\n"
            f"Ground Truth SQL: {gt_sql}\n"
            f"Genie SQL: {genie_sql}\n\n"
            f"Result comparison: match={cmp.get('match')}, "
            f"match_type={cmp.get('match_type')}, "
            f"gt_rows={cmp.get('gt_rows')}, genie_rows={cmp.get('genie_rows')}\n\n"
            f"Ground Truth Result (first 5 rows):\n{gt_sample}\n\n"
            f"Genie Result (first 5 rows):\n{genie_sample}\n\n"
            'Respond with JSON only: {"verdict": "<genie_correct|ground_truth_correct|both_correct|neither_correct>", '
            '"failure_type": "<wrong_aggregation|wrong_filter|wrong_table|other>", '
            '"blame_set": ["<blamed_object>"], '
            '"rationale": "<brief explanation>"}'
        )

        logger.info(
            "\n"
            "┌─── JUDGE [arbiter] INPUT ──────────────────────────────────────────────\n"
            "│ Question: %s\n"
            "│ Genie SQL:\n"
            "│   %s\n"
            "│ GT SQL:\n"
            "│   %s\n"
            "│ Comparison: %s\n"
            "│ Trigger:    results MISMATCH (arbiter invoked)\n"
            "│ Prompt length: %d chars\n"
            "└─────────────────────────────────────────────────────────────────────────",
            question,
            genie_sql or "(none)",
            gt_sql or "(none)",
            json.dumps(cmp, indent=2),
            len(prompt),
        )

        try:
            result = _call_llm_for_scoring(w, prompt)
            verdict = result.get("verdict", "ground_truth_correct")
            valid_verdicts = {
                "genie_correct",
                "ground_truth_correct",
                "both_correct",
                "neither_correct",
            }
            if verdict not in valid_verdicts:
                verdict = _parse_arbiter_verdict(result.get("rationale", str(result)))

            logger.info(
                "\n"
                "┌─── JUDGE [arbiter] VERDICT ─────────────────────────────────────────────\n"
                "│ Question:    %s\n"
                "│ Verdict:     %s\n"
                "│ Rationale:   %s\n"
                "│ Failure:     type=%s | blame=%s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                question[:80],
                verdict,
                result.get("rationale", "(none)"),
                result.get("failure_type", "n/a"),
                result.get("blame_set", []),
            )

            _meta = None
            if verdict in ("ground_truth_correct", "neither_correct"):
                _meta = build_asi_metadata(
                    failure_type=result.get("failure_type", "other"),
                    severity="major",
                    confidence=0.85,
                    blame_set=result.get("blame_set", []),
                    counterfactual_fix=result.get("rationale", ""),
                )
            return Feedback(
                name="arbiter",
                value=verdict,
                rationale=format_asi_markdown(
                    judge_name="arbiter",
                    value=verdict,
                    rationale=result.get("rationale", verdict),
                    metadata=_meta,
                    extra={"llm_response": result, "comparison": cmp},
                ),
                source=LLM_SOURCE,
                metadata=_meta,
            )
        except Exception as e:
            logger.error(
                "\n"
                "┌─── JUDGE [arbiter] ERROR ──────────────────────────────────────────────\n"
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
                name="arbiter",
                value="ground_truth_correct",
                rationale=format_asi_markdown(
                    judge_name="arbiter",
                    value="ground_truth_correct",
                    rationale=f"Arbiter LLM call failed, defaulting to ground_truth_correct: {e}",
                    metadata=metadata,
                    extra={"comparison": cmp},
                ),
                source=LLM_SOURCE,
                metadata=metadata,
            )

    return arbiter_scorer
