"""Arbiter scorer — Layer 3 conditional LLM judge.

Fires only when result_correctness reports a mismatch. Arbitrates between
the ground-truth SQL and Genie SQL to determine which is correct.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.config import JUDGE_PROMPTS
from genie_space_optimizer.common.genie_client import resolve_sql, sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    LLM_SOURCE,
    _call_llm_for_scoring,
    _extract_response_text,
    build_asi_metadata,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


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
            return Feedback(
                name="arbiter",
                value="skipped",
                rationale="Results match — arbiter not invoked.",
                source=CODE_SOURCE,
            )

        if cmp.get("error"):
            return Feedback(
                name="arbiter",
                value="skipped",
                rationale=f"SQL execution error — cannot arbitrate: {cmp['error']}",
                source=CODE_SOURCE,
            )

        genie_sql = sanitize_sql(_extract_response_text(outputs))
        gt_sql = resolve_sql(expectations.get("expected_response", ""), catalog, schema)
        question = inputs.get("question", "")

        arbiter_instructions = _loaded.get("arbiter", JUDGE_PROMPTS.get("arbiter", ""))
        prompt = (
            f"{arbiter_instructions}\n\n"
            f"Question: {question}\n"
            f"Ground Truth SQL: {gt_sql}\n"
            f"Genie SQL: {genie_sql}\n"
            f"Result comparison: {json.dumps(cmp)}\n\n"
            'Respond with JSON only: {"verdict": "<genie_correct|ground_truth_correct|both_correct|neither_correct>", '
            '"failure_type": "<wrong_aggregation|wrong_filter|wrong_table|other>", '
            '"blame_set": ["<blamed_object>"], '
            '"rationale": "<brief explanation>"}'
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
                rationale=result.get("rationale", verdict),
                source=LLM_SOURCE,
                metadata=_meta,
            )
        except Exception as e:
            return Feedback(
                name="arbiter",
                value="ground_truth_correct",
                rationale=f"Arbiter LLM call failed, defaulting to ground_truth_correct: {e}",
                source=LLM_SOURCE,
                metadata=build_asi_metadata(
                    failure_type="other",
                    severity="info",
                    confidence=0.0,
                    counterfactual_fix="LLM judge unavailable — retry or check endpoint",
                ),
            )

    return arbiter_scorer
