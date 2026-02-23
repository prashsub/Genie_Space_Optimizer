"""Run endpoints: status, comparison, apply, discard."""

from __future__ import annotations

import json
import logging

from fastapi import HTTPException

from ..core import Dependencies, create_router
from ..models import (
    ActionResponse,
    ComparisonData,
    DimensionScore,
    LeverStatus,
    PipelineRun,
    PipelineStep,
    SpaceConfiguration,
    TableDescription,
)
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)


# ── Stage-to-Step Mapping ───────────────────────────────────────────────


_STEP_DEFINITIONS = [
    {
        "stepNumber": 1,
        "name": "Configuration Analysis",
        "stage_prefixes": ["PREFLIGHT"],
        "summary_template": "Analyzed {tables} tables, {instructions} instructions, {questions} sample questions",
    },
    {
        "stepNumber": 2,
        "name": "Metadata Collection",
        "stage_prefixes": ["PREFLIGHT"],
        "summary_template": "Collected metadata for {columns} columns, {tags} tags from Unity Catalog",
    },
    {
        "stepNumber": 3,
        "name": "Baseline Evaluation",
        "stage_prefixes": ["BASELINE_EVAL"],
        "summary_template": "Evaluated {questions} benchmark questions with 8 judges. Baseline score: {score}%",
    },
    {
        "stepNumber": 4,
        "name": "Configuration Generation",
        "stage_prefixes": ["LEVER_"],
        "summary_template": "Applied {patches} optimizations across {levers} categories. Score improved from {before}% to {after}%",
    },
    {
        "stepNumber": 5,
        "name": "Optimized Evaluation",
        "stage_prefixes": ["FINALIZE", "REPEATABILITY", "DEPLOY", "COMPLETE"],
        "summary_template": "Final evaluation complete. Optimized score: {score}%. Repeatability: {repeatability}%",
    },
]


def _derive_step_status(matching_stages: list[dict]) -> str:
    """Derive user-facing step status from internal stage rows."""
    if not matching_stages:
        return "pending"
    statuses = {str(s.get("status", "")).upper() for s in matching_stages}
    if "FAILED" in statuses:
        return "failed"
    terminal = {"COMPLETE", "SKIPPED", "ROLLED_BACK"}
    if all(s in terminal for s in statuses):
        return "completed"
    if "STARTED" in statuses:
        return "running"
    return "pending"


def _total_duration(matching_stages: list[dict]) -> float | None:
    """Sum duration_seconds across matching stages."""
    total = 0.0
    has_any = False
    for s in matching_stages:
        d = s.get("duration_seconds")
        if d is not None:
            total += float(d)
            has_any = True
    return total if has_any else None


def _parse_detail(stage: dict) -> dict:
    """Parse detail_json from a stage row."""
    raw = stage.get("detail_json")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def map_stages_to_steps(
    stages_rows: list[dict],
    iterations_rows: list[dict],
    run_data: dict,
) -> list[PipelineStep]:
    """Map internal harness stages to 5 user-facing pipeline steps.

    Steps 1-2 both map to PREFLIGHT stages but split by the detail_json:
    Step 1 covers config analysis, Step 2 covers UC metadata collection.
    """
    steps: list[PipelineStep] = []

    for defn in _STEP_DEFINITIONS:
        prefixes = defn["stage_prefixes"]
        step_num = defn["stepNumber"]

        matching = []
        for s in stages_rows:
            stage_name = str(s.get("stage", ""))
            for prefix in prefixes:
                if stage_name.startswith(prefix):
                    matching.append(s)
                    break

        # Steps 1 and 2 split PREFLIGHT stages
        if step_num == 1:
            matching = [s for s in matching if "STARTED" in str(s.get("stage", ""))]
        elif step_num == 2:
            matching = [
                s for s in matching
                if "STARTED" not in str(s.get("stage", ""))
                or str(s.get("status", "")).upper() == "COMPLETE"
            ]

        status = _derive_step_status(matching)
        duration = _total_duration(matching)

        summary = _build_step_summary(defn, matching, iterations_rows, run_data)

        steps.append(
            PipelineStep(
                stepNumber=step_num,
                name=defn["name"],
                status=status,
                durationSeconds=duration,
                summary=summary,
            )
        )

    return steps


def _build_step_summary(
    defn: dict,
    matching: list[dict],
    iterations_rows: list[dict],
    run_data: dict,
) -> str | None:
    """Build a human-readable summary for a pipeline step."""
    if not matching:
        return None

    step_num = defn["stepNumber"]
    detail = {}
    for s in matching:
        detail.update(_parse_detail(s))

    if step_num == 1:
        return defn["summary_template"].format(
            tables=detail.get("table_count", "?"),
            instructions=detail.get("instruction_count", "?"),
            questions=detail.get("benchmark_count", run_data.get("benchmark_count", "?")),
        )
    if step_num == 3:
        baseline_iter = next(
            (r for r in iterations_rows if r.get("iteration") == 0),
            None,
        )
        score = "?"
        questions = "?"
        if baseline_iter:
            score = f"{float(baseline_iter.get('overall_accuracy', 0)):.1f}"
            questions = str(baseline_iter.get("total_questions", "?"))
        return defn["summary_template"].format(questions=questions, score=score)
    if step_num == 4:
        patches = detail.get("patches_applied", 0)
        levers_accepted = detail.get("levers_accepted", [])
        before = f"{float(run_data.get('baseline_accuracy', 0)):.1f}" if run_data.get("baseline_accuracy") else "?"
        after = f"{float(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        return defn["summary_template"].format(
            patches=patches,
            levers=len(levers_accepted) if isinstance(levers_accepted, list) else "?",
            before=before,
            after=after,
        )
    if step_num == 5:
        score = f"{float(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        rep = f"{float(run_data.get('best_repeatability', 0)):.1f}" if run_data.get("best_repeatability") else "?"
        return defn["summary_template"].format(score=score, repeatability=rep)

    return None


def _build_levers(stages_rows: list[dict]) -> list[LeverStatus]:
    """Build lever detail from LEVER_* stage rows."""
    from genie_space_optimizer.common.config import LEVER_NAMES

    lever_data: dict[int, dict] = {}
    for s in stages_rows:
        stage_name = str(s.get("stage", ""))
        if not stage_name.startswith("LEVER_"):
            continue
        lever_num = s.get("lever")
        if lever_num is None:
            try:
                parts = stage_name.split("_")
                lever_num = int(parts[1])
            except (IndexError, ValueError):
                continue
        lever_num = int(lever_num)
        if lever_num not in lever_data:
            lever_data[lever_num] = {"stages": [], "detail": {}}
        lever_data[lever_num]["stages"].append(s)
        lever_data[lever_num]["detail"].update(_parse_detail(s))

    levers: list[LeverStatus] = []
    for lever_num in sorted(lever_data.keys()):
        data = lever_data[lever_num]
        ld = data["detail"]
        status = _derive_lever_status(data["stages"])
        patches_applied = ld.get("patches_applied", 0)

        rollback_reason = None
        if status == "rolled_back":
            rollback_reason = ld.get("reason", "regression")

        levers.append(
            LeverStatus(
                lever=lever_num,
                name=LEVER_NAMES.get(lever_num, f"Lever {lever_num}"),
                status=status,
                patchCount=patches_applied,
                scoreBefore=_safe_float(ld.get("score_before")),
                scoreAfter=_safe_float(ld.get("accuracy")),
                scoreDelta=_safe_float(ld.get("score_delta")),
                rollbackReason=rollback_reason,
            )
        )
    return levers


def _derive_lever_status(stages: list[dict]) -> str:
    statuses = {str(s.get("status", "")).upper() for s in stages}
    if "ROLLED_BACK" in statuses:
        return "rolled_back"
    if "FAILED" in statuses:
        return "failed"
    if "SKIPPED" in statuses:
        return "skipped"
    if "COMPLETE" in statuses:
        return "accepted"
    if "STARTED" in statuses:
        has_eval = any("EVAL" in str(s.get("stage", "")) for s in stages)
        return "evaluating" if has_eval else "running"
    return "pending"


# ── Route Handlers ──────────────────────────────────────────────────────


@router.get("/runs/{run_id}", response_model=PipelineRun, operation_id="getRun")
def get_run(run_id: str, config: Dependencies.Config):
    """Run status with 5 user-facing pipeline steps and lever detail."""
    from genie_space_optimizer.optimization.state import (
        load_iterations,
        load_run,
        load_stages,
    )

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    stages_df = load_stages(spark, run_id, config.catalog, config.schema_name)
    stages_rows = stages_df.to_dict("records") if not stages_df.empty else []

    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []

    baseline_iter = next((r for r in iters_rows if r.get("iteration") == 0), None)
    baseline_accuracy = float(baseline_iter["overall_accuracy"]) if baseline_iter else None
    run_data["baseline_accuracy"] = baseline_accuracy

    steps = map_stages_to_steps(stages_rows, iters_rows, run_data)
    levers = _build_levers(stages_rows)

    status = run_data.get("status", "QUEUED")
    display_status = status
    if status == "IN_PROGRESS":
        display_status = "RUNNING"

    return PipelineRun(
        runId=run_id,
        spaceId=run_data.get("space_id", ""),
        spaceName=run_data.get("space_name", run_data.get("domain", "")),
        status=display_status,
        startedAt=str(run_data.get("started_at", "")),
        completedAt=str(run_data.get("completed_at", "")) if run_data.get("completed_at") else None,
        initiatedBy=run_data.get("triggered_by", "system"),
        baselineScore=baseline_accuracy,
        optimizedScore=_safe_float(run_data.get("best_accuracy")),
        steps=steps,
        levers=levers,
        convergenceReason=run_data.get("convergence_reason"),
    )


@router.get(
    "/runs/{run_id}/comparison",
    response_model=ComparisonData,
    operation_id="getComparison",
)
def get_comparison(run_id: str, ws: Dependencies.UserClient, config: Dependencies.Config):
    """Side-by-side original vs optimized config with per-dimension scores."""
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import (
        load_iterations,
        load_run,
    )

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    space_id = run_data.get("space_id", "")

    original_snapshot = run_data.get("config_snapshot")
    if isinstance(original_snapshot, str):
        try:
            original_snapshot = json.loads(original_snapshot)
        except (json.JSONDecodeError, TypeError):
            original_snapshot = {}
    if not isinstance(original_snapshot, dict):
        original_snapshot = {}

    try:
        current_config = fetch_space_config(ws, space_id)
        current_ss = current_config.get("_parsed_space", {})
    except Exception:
        current_ss = {}

    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []

    baseline_scores = {}
    optimized_scores = {}
    baseline_accuracy = 0.0
    optimized_accuracy = 0.0

    for row in iters_rows:
        if row.get("eval_scope") != "full":
            continue
        scores_raw = row.get("scores_json", {})
        if isinstance(scores_raw, str):
            try:
                scores_raw = json.loads(scores_raw)
            except (json.JSONDecodeError, TypeError):
                scores_raw = {}
        if row.get("iteration") == 0:
            baseline_scores = scores_raw if isinstance(scores_raw, dict) else {}
            baseline_accuracy = float(row.get("overall_accuracy", 0))
        else:
            optimized_scores = scores_raw if isinstance(scores_raw, dict) else {}
            optimized_accuracy = float(row.get("overall_accuracy", 0))

    if not optimized_scores:
        optimized_scores = baseline_scores
        optimized_accuracy = baseline_accuracy

    dimensions: list[DimensionScore] = []
    all_dims = set(baseline_scores.keys()) | set(optimized_scores.keys())
    for dim in sorted(all_dims):
        b = baseline_scores.get(dim, 0.0)
        o = optimized_scores.get(dim, 0.0)
        dimensions.append(
            DimensionScore(
                dimension=dim,
                baseline=float(b),
                optimized=float(o),
                delta=float(o) - float(b),
            )
        )

    improvement_pct = 0.0
    if baseline_accuracy > 0:
        improvement_pct = ((optimized_accuracy - baseline_accuracy) / baseline_accuracy) * 100

    return ComparisonData(
        runId=run_id,
        spaceId=space_id,
        spaceName=run_data.get("domain", ""),
        baselineScore=baseline_accuracy,
        optimizedScore=optimized_accuracy,
        improvementPct=round(improvement_pct, 2),
        perDimensionScores=dimensions,
        original=_extract_space_configuration(original_snapshot),
        optimized=_extract_space_configuration(current_ss),
    )


@router.post(
    "/runs/{run_id}/apply",
    response_model=ActionResponse,
    operation_id="applyOptimization",
)
def apply_optimization(run_id: str, config: Dependencies.Config):
    """Confirm optimized config. Patches are already applied; this marks the run as APPLIED."""
    from genie_space_optimizer.optimization.state import load_run, update_run_status

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    status = run_data.get("status", "")
    terminal = {"CONVERGED", "STALLED", "MAX_ITERATIONS"}
    if status not in terminal:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot apply run in status {status}. Must be one of: {terminal}",
        )

    update_run_status(
        spark, run_id, config.catalog, config.schema_name,
        status="APPLIED",
    )
    return ActionResponse(status="applied", runId=run_id, message="Optimization applied successfully.")


@router.post(
    "/runs/{run_id}/discard",
    response_model=ActionResponse,
    operation_id="discardOptimization",
)
def discard_optimization(
    run_id: str, ws: Dependencies.UserClient, config: Dependencies.Config,
):
    """Discard optimization results and rollback patches to original config."""
    from genie_space_optimizer.optimization.applier import rollback
    from genie_space_optimizer.optimization.state import load_run, update_run_status

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    status = run_data.get("status", "")
    if status in ("DISCARDED", "APPLIED"):
        raise HTTPException(
            status_code=409,
            detail=f"Run already {status.lower()}.",
        )

    space_id = run_data.get("space_id", "")
    original_snapshot = run_data.get("config_snapshot")
    if isinstance(original_snapshot, str):
        try:
            original_snapshot = json.loads(original_snapshot)
        except (json.JSONDecodeError, TypeError):
            original_snapshot = {}

    if original_snapshot and isinstance(original_snapshot, dict):
        apply_log = {"pre_snapshot": original_snapshot}
        rollback(apply_log, ws, space_id)

    update_run_status(
        spark, run_id, config.catalog, config.schema_name,
        status="DISCARDED",
        convergence_reason="user_discarded",
    )

    return ActionResponse(
        status="discarded",
        runId=run_id,
        message="Optimization discarded and patches rolled back.",
    )


# ── Helpers ─────────────────────────────────────────────────────────────


def _extract_space_configuration(ss: dict) -> SpaceConfiguration:
    """Extract SpaceConfiguration from a serialized_space dict."""
    if not ss or not isinstance(ss, dict):
        return SpaceConfiguration(instructions="", sampleQuestions=[], tableDescriptions=[])

    instr = ss.get("instructions", {})
    text_instr = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instructions_str = ""
    if text_instr and isinstance(text_instr, list) and text_instr:
        content = text_instr[0].get("content", [])
        if isinstance(content, list):
            instructions_str = "\n".join(str(c) for c in content)
        else:
            instructions_str = str(content)

    example_qs = instr.get("example_question_sqls", []) if isinstance(instr, dict) else []
    questions = [q.get("question", "") for q in example_qs if isinstance(q, dict)]

    ds = ss.get("data_sources", {})
    tables = ds.get("tables", []) if isinstance(ds, dict) else []
    table_descs: list[TableDescription] = []
    for t in tables:
        ident = t.get("identifier", "")
        desc_val = t.get("description", [])
        desc_str = "\n".join(desc_val) if isinstance(desc_val, list) else str(desc_val or "")
        table_descs.append(TableDescription(tableName=ident, description=desc_str))

    return SpaceConfiguration(
        instructions=instructions_str,
        sampleQuestions=questions,
        tableDescriptions=table_descs,
    )


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
