"""Run endpoints: status, comparison, apply, discard."""

from __future__ import annotations

import json
import logging
import math
from typing import Any, TypedDict

from databricks.sdk import WorkspaceClient
from fastapi import HTTPException

from ..constants import ACTIVE_RUN_STATUSES, TERMINAL_JOB_STATES
from ..core import Dependencies, create_router
from ..utils import ensure_utc_iso, safe_finite, safe_float, safe_int, safe_json_parse, scrub_nan_inf
from .spaces import _genie_client
from ..models import (
    ActionResponse,
    AsiResult,
    AsiSummary,
    ComparisonData,
    DimensionScore,
    IterationSummary,
    LeverStatus,
    PipelineLink,
    PipelineRun,
    PipelineStep,
    ProvenanceRecord,
    ProvenanceSummary,
    SpaceConfiguration,
    TableDescription,
)
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)
_DEFERRED_UC_MODES = {"uc_artifact", "both"}
_UC_WRITE_STAGE = "UC_OBO_WRITE"
_UC_WRITE_TASK_KEY = "obo_uc_write"
_UC_WRITE_READY_STATUSES = {"CONVERGED", "STALLED", "MAX_ITERATIONS"}
_UC_WRITE_TERMINAL_STAGE_STATUSES = {"COMPLETE", "SKIPPED", "FAILED"}
_TERMINAL_RUN_STATUSES = {
    "CONVERGED",
    "STALLED",
    "MAX_ITERATIONS",
    "FAILED",
    "CANCELLED",
    "APPLIED",
    "DISCARDED",
}


class _StepDefinition(TypedDict):
    stepNumber: int
    name: str
    stage_prefixes: list[str]
    summary_template: str


def _is_truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _parse_patch_command(raw: object) -> dict:
    """Parse a patch command JSON column into a dict."""
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        first = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if isinstance(first, dict):
        return first
    if isinstance(first, str):
        try:
            second = json.loads(first)
        except (json.JSONDecodeError, TypeError):
            return {}
        return second if isinstance(second, dict) else {}
    return {}


def _uc_statement_from_patch(patch_type: str, command: dict) -> str | None:
    """Build a UC SQL statement for supported deferred patch types."""
    if patch_type in {"add_column_description", "update_column_description"}:
        table = str(command.get("table") or "").strip()
        column = str(command.get("column") or "").strip()
        text = str(command.get("new_text") or command.get("value") or "").strip()
        if table and column and text:
            escaped = text.replace("'", "''")
            return f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{escaped}'"

    if patch_type in {"add_description", "update_description"}:
        table = str(command.get("target") or "").strip()
        text = str(command.get("new_text") or command.get("value") or "").strip()
        if table and text:
            escaped = text.replace("'", "''")
            return f"COMMENT ON TABLE {table} IS '{escaped}'"

    if patch_type == "update_tvf_sql":
        stmt = str(command.get("new_sql") or "").strip()
        if stmt:
            return stmt

    return None


def _uc_stage_already_terminal(stages_rows: list[dict]) -> bool:
    for stage in stages_rows:
        stage_name = str(stage.get("stage", "")).upper()
        status = str(stage.get("status", "")).upper()
        if stage_name.startswith(_UC_WRITE_STAGE) and status in _UC_WRITE_TERMINAL_STAGE_STATUSES:
            return True
    return False


def _apply_deferred_uc_writes_obo(
    *,
    spark,
    run_id: str,
    run_data: dict,
    stages_rows: list[dict],
    ws,
    config,
) -> str:
    """Replay UC write SQL via OBO after optimization reaches terminal state."""
    from genie_space_optimizer.optimization.state import load_patches, write_stage

    requested_mode = str(run_data.get("apply_mode") or "genie_config").lower()
    run_status = str(run_data.get("status") or "").upper()

    if requested_mode not in _DEFERRED_UC_MODES:
        return "not_required"
    if run_status not in _UC_WRITE_READY_STATUSES:
        raise RuntimeError(f"Run status {run_status} is not ready for deferred UC writes")
    if _uc_stage_already_terminal(stages_rows):
        return "already_recorded"

    try:
        write_stage(
            spark,
            run_id,
            _UC_WRITE_STAGE,
            "STARTED",
            task_key=_UC_WRITE_TASK_KEY,
            detail={"mode": requested_mode},
            catalog=config.catalog,
            schema=config.schema_name,
        )

        if not config.warehouse_id:
            raise RuntimeError("Missing warehouse_id for OBO SQL execution")

        patches_df = load_patches(spark, run_id, config.catalog, config.schema_name)
        if patches_df.empty:
            write_stage(
                spark,
                run_id,
                _UC_WRITE_STAGE,
                "SKIPPED",
                task_key=_UC_WRITE_TASK_KEY,
                detail={"reason": "no_patches"},
                catalog=config.catalog,
                schema=config.schema_name,
            )
            return "skipped_no_patches"

        statements: list[str] = []
        skipped = 0
        for row in patches_df.to_dict("records"):
            if _is_truthy(row.get("rolled_back")):
                continue
            patch_type = str(row.get("patch_type") or "")
            command = _parse_patch_command(row.get("command_json"))
            stmt = _uc_statement_from_patch(patch_type, command)
            if stmt:
                statements.append(stmt)
            else:
                skipped += 1

        if not statements:
            write_stage(
                spark,
                run_id,
                _UC_WRITE_STAGE,
                "SKIPPED",
                task_key=_UC_WRITE_TASK_KEY,
                detail={
                    "reason": "no_supported_uc_commands",
                    "skipped_count": skipped,
                },
                catalog=config.catalog,
                schema=config.schema_name,
            )
            return "skipped_no_supported_commands"

        for statement in statements:
            ws.statement_execution.execute_statement(
                warehouse_id=config.warehouse_id,
                statement=statement,
                wait_timeout="60s",
            )

        write_stage(
            spark,
            run_id,
            _UC_WRITE_STAGE,
            "COMPLETE",
            task_key=_UC_WRITE_TASK_KEY,
            detail={
                "executed_count": len(statements),
                "skipped_count": skipped,
            },
            catalog=config.catalog,
            schema=config.schema_name,
        )
        return "applied"
    except Exception as exc:
        logger.exception("Deferred OBO UC write failed for run %s", run_id)
        try:
            write_stage(
                spark,
                run_id,
                _UC_WRITE_STAGE,
                "FAILED",
                task_key=_UC_WRITE_TASK_KEY,
                error_message=str(exc)[:500],
                detail={"error": str(exc)[:500]},
                catalog=config.catalog,
                schema=config.schema_name,
            )
        except Exception:
            logger.exception("Failed to persist UC write failure stage for run %s", run_id)
        raise RuntimeError(f"Deferred OBO UC write failed: {exc}") from exc


# ── Stage-to-Step Mapping ───────────────────────────────────────────────


_STEP_DEFINITIONS: list[_StepDefinition] = [
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
        "stage_prefixes": ["LEVER_", "AG_"],
        "summary_template": "Applied {patches} optimizations across {levers} categories. Score improved from {before}% to {after}%",
    },
    {
        "stepNumber": 5,
        "name": "Optimized Evaluation",
        "stage_prefixes": ["FINALIZE", "REPEATABILITY", "DEPLOY", "COMPLETE", _UC_WRITE_STAGE],
        "summary_template": "Final evaluation complete. Optimized score: {score}%. Repeatability: {repeatability}%",
    },
]


def _derive_step_status(matching_stages: list[dict]) -> str:
    """Derive user-facing step status from the latest matching stage row."""
    if not matching_stages:
        return "pending"
    latest = matching_stages[-1]
    latest_status = str(latest.get("status", "")).upper()
    if latest_status == "FAILED":
        return "failed"
    if latest_status in {"COMPLETE", "SKIPPED", "ROLLED_BACK"}:
        return "completed"
    if latest_status == "STARTED":
        return "running"
    return "pending"


def _total_duration(matching_stages: list[dict]) -> float | None:
    """Sum duration_seconds across matching stages."""
    total = 0.0
    has_any = False
    for s in matching_stages:
        d = s.get("duration_seconds")
        if d is not None:
            total += _finite(d)
            has_any = True
    return total if has_any else None


def _parse_detail(stage: dict) -> dict:
    """Parse detail_json from a stage row."""
    parsed = safe_json_parse(stage.get("detail_json"))
    return parsed if isinstance(parsed, dict) else {}


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

        # Steps 1 and 2 split PREFLIGHT stages.
        # Stage order: PREFLIGHT_STARTED(STARTED) → PREFLIGHT_METADATA_COLLECTION(COMPLETE) → PREFLIGHT_STARTED(COMPLETE).
        # Step 1 (Config Analysis) is done once metadata collection has started/completed.
        # Step 2 (Metadata Collection) only uses the PREFLIGHT_METADATA_COLLECTION stage.
        has_metadata_stage = any(
            "METADATA" in str(s.get("stage", "")) for s in stages_rows
        )
        if step_num == 1:
            matching = [s for s in matching if "STARTED" in str(s.get("stage", ""))]
        elif step_num == 2:
            matching = [s for s in matching if "METADATA" in str(s.get("stage", ""))]

        status = _derive_step_status(matching)
        if step_num == 1 and status == "running" and has_metadata_stage:
            status = "completed"
        status = _normalize_step_status_for_terminal_run(
            status=status,
            run_status=str(run_data.get("status", "")),
        )
        duration = _total_duration(matching)

        summary = _build_step_summary(defn, matching, iterations_rows, run_data)
        inputs, outputs = _build_step_io(defn, matching, iterations_rows, run_data)

        steps.append(
            PipelineStep(
                stepNumber=step_num,
                name=defn["name"],
                status=status,
                durationSeconds=duration,
                summary=summary,
                inputs=inputs,
                outputs=outputs,
            )
        )

    return steps


def _build_step_io(
    defn: _StepDefinition,
    matching: list[dict],
    iterations_rows: list[dict],
    run_data: dict,
) -> tuple[dict | None, dict | None]:
    """Build rich inputs/outputs payload for pipeline step drill-down."""
    if not matching:
        return None, None

    step_num = defn["stepNumber"]
    detail: dict[str, Any] = {}
    for s in matching:
        detail.update(_parse_detail(s))
    timeline = _build_stage_timeline(matching)
    config_snapshot = run_data.get("config_snapshot") if isinstance(run_data.get("config_snapshot"), dict) else {}

    if step_num == 1:
        parsed = config_snapshot.get("_parsed_space", {}) if isinstance(config_snapshot, dict) else {}
        ds = parsed.get("data_sources", {}) if isinstance(parsed, dict) else {}
        tables = ds.get("tables", []) if isinstance(ds, dict) else []
        functions = ds.get("functions", []) if isinstance(ds, dict) else []
        instructions_node = parsed.get("instructions", {}) if isinstance(parsed, dict) else {}
        text_instructions = (
            instructions_node.get("text_instructions", [])
            if isinstance(instructions_node, dict)
            else []
        )
        examples = (
            instructions_node.get("example_question_sqls", [])
            if isinstance(instructions_node, dict)
            else []
        )
        sample_questions: list[str] = []
        for ex in examples:
            q = str(ex.get("question") or "").strip() if isinstance(ex, dict) else ""
            if q:
                sample_questions.append(q)

        return (
            {
                "spaceId": run_data.get("space_id"),
                "domain": run_data.get("domain"),
            },
            {
                "tableCount": len(tables),
                "tables": [str(t.get("identifier") or "") for t in tables[:12] if isinstance(t, dict)],
                "functionCount": len(functions),
                "instructionCount": len(text_instructions),
                "sampleQuestionCount": len(sample_questions),
                "sampleQuestionsPreview": sample_questions[:5],
                "stageEvents": timeline,
            },
        )

    if step_num == 2:
        def _to_int(value: Any) -> int | None:
            if value is None:
                return None
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                return None
            return parsed if parsed >= 0 else None

        def _to_list_of_str(value: Any) -> list[str]:
            if not isinstance(value, list):
                return []
            out: list[str] = []
            for item in value:
                text = str(item).strip()
                if text:
                    out.append(text)
            return out

        prefetched = (
            config_snapshot.get("_prefetched_uc_metadata", {})
            if isinstance(config_snapshot, dict)
            else {}
        )
        uc_columns = prefetched.get("uc_columns", []) if isinstance(prefetched, dict) else []
        uc_tags = prefetched.get("uc_tags", []) if isinstance(prefetched, dict) else []
        uc_routines = prefetched.get("uc_routines", []) if isinstance(prefetched, dict) else []

        column_samples: list[str] = []
        for col in uc_columns[:12]:
            if not isinstance(col, dict):
                continue
            table_name = str(col.get("table_name") or col.get("table") or "").strip()
            col_name = str(col.get("column_name") or col.get("column") or "").strip()
            if table_name and col_name:
                column_samples.append(f"{table_name}.{col_name}")
            elif col_name:
                column_samples.append(col_name)

        tag_samples: list[str] = []
        for tag in uc_tags[:8]:
            if not isinstance(tag, dict):
                continue
            table_name = str(tag.get("table_name") or "").strip()
            col_name = str(tag.get("column_name") or "").strip()
            tag_name = str(tag.get("tag_name") or tag.get("name") or "").strip()
            tag_value = str(tag.get("tag_value") or tag.get("value") or "").strip()
            target = ".".join(p for p in [table_name, col_name] if p)
            if tag_name:
                tag_samples.append(f"{target}: {tag_name}={tag_value}" if target else f"{tag_name}={tag_value}")

        routine_samples: list[str] = []
        for r in uc_routines[:8]:
            if not isinstance(r, dict):
                continue
            routine_samples.append(str(r.get("routine_name") or r.get("name") or "").strip())

        columns_collected = _to_int(detail.get("columns_collected"))
        if columns_collected is None:
            columns_collected = _to_int(detail.get("columnsCollected"))
        if columns_collected is None:
            columns_collected = len(uc_columns) if isinstance(uc_columns, list) else 0

        tags_collected = _to_int(detail.get("tags_collected"))
        if tags_collected is None:
            tags_collected = _to_int(detail.get("tagsCollected"))
        if tags_collected is None:
            tags_collected = len(uc_tags) if isinstance(uc_tags, list) else 0

        routines_collected = _to_int(detail.get("routines_collected"))
        if routines_collected is None:
            routines_collected = _to_int(detail.get("routinesCollected"))
        if routines_collected is None:
            routines_collected = len(uc_routines) if isinstance(uc_routines, list) else 0

        detail_column_samples = _to_list_of_str(detail.get("column_samples"))
        if not detail_column_samples:
            detail_column_samples = _to_list_of_str(detail.get("columnSamples"))
        detail_tag_samples = _to_list_of_str(detail.get("tag_samples"))
        if not detail_tag_samples:
            detail_tag_samples = _to_list_of_str(detail.get("tagSamples"))
        detail_routine_samples = _to_list_of_str(detail.get("routine_samples"))
        if not detail_routine_samples:
            detail_routine_samples = _to_list_of_str(detail.get("routineSamples"))

        referenced_schemas = _to_list_of_str(detail.get("referenced_schemas"))
        if not referenced_schemas:
            referenced_schemas = _to_list_of_str(detail.get("referencedSchemas"))
        referenced_schema_count = _to_int(detail.get("referenced_schema_count"))
        if referenced_schema_count is None:
            referenced_schema_count = _to_int(detail.get("referencedSchemaCount"))
        if referenced_schema_count is None:
            referenced_schema_count = len(referenced_schemas)

        return (
            {
                "catalog": run_data.get("catalog"),
                "schema": run_data.get("uc_schema"),
            },
            {
                "columnsCollected": columns_collected,
                "tagsCollected": tags_collected,
                "routinesCollected": routines_collected,
                "columnSamples": detail_column_samples or [s for s in column_samples if s],
                "tagSamples": detail_tag_samples or [s for s in tag_samples if s],
                "routineSamples": detail_routine_samples or [s for s in routine_samples if s],
                "tableRefCount": _to_int(detail.get("table_ref_count")),
                "referencedSchemaCount": referenced_schema_count,
                "referencedSchemas": referenced_schemas,
                "collectionScope": detail.get("collection_scope"),
                "metadataSource": detail.get("metadata_source"),
                "stageEvents": timeline,
            },
        )

    if step_num == 3:
        baseline_iter = next(
            (
                r for r in iterations_rows
                if int(r.get("iteration", -1)) == 0
                and str(r.get("eval_scope", "")).lower() == "full"
            ),
            None,
        )
        if not baseline_iter:
            return None, {"stageEvents": timeline}

        scores = baseline_iter.get("scores_json", {})
        if isinstance(scores, str):
            try:
                scores = json.loads(scores)
            except (json.JSONDecodeError, TypeError):
                scores = {}
        if not isinstance(scores, dict):
            scores = {}

        rows_json = baseline_iter.get("rows_json", [])
        if isinstance(rows_json, str):
            try:
                rows_json = json.loads(rows_json)
            except (json.JSONDecodeError, TypeError):
                rows_json = []
        if not isinstance(rows_json, list):
            rows_json = []

        sample_rows: list[dict[str, Any]] = []
        for row in rows_json[:5]:
            if not isinstance(row, dict):
                continue
            question = ""
            if isinstance(row.get("inputs"), dict):
                question = str(row.get("inputs", {}).get("question") or "").strip()
            if not question:
                question = str(row.get("inputs/question") or "").strip()
            sample_rows.append(
                {
                    "question": question,
                    "resultCorrectness": row.get("result_correctness/value", row.get("result_correctness")),
                    "syntaxValidity": row.get("syntax_validity/value", row.get("syntax_validity")),
                    "assetRouting": row.get("asset_routing/value", row.get("asset_routing")),
                    "matchType": (
                        row.get("outputs", {}).get("comparison", {}).get("match_type")
                        if isinstance(row.get("outputs"), dict)
                        else None
                    ),
                    "error": (
                        row.get("outputs", {}).get("comparison", {}).get("error")
                        if isinstance(row.get("outputs"), dict)
                        else None
                    ),
                }
            )

        return (
            {
                "benchmarkCount": baseline_iter.get("total_questions"),
                "iteration": 0,
            },
            {
                "judgeScores": {k: _safe_float(v) for k, v in scores.items()},
                "totalQuestions": baseline_iter.get("total_questions"),
                "correctCount": baseline_iter.get("correct_count"),
                "failedCount": int(
                    _finite(baseline_iter.get("total_questions", 0))
                    - _finite(baseline_iter.get("correct_count", 0))
                ),
                "mlflowRunId": baseline_iter.get("mlflow_run_id"),
                "invalidBenchmarkCount": _safe_int(detail.get("invalid_benchmark_count")),
                "permissionBlockedCount": _safe_int(detail.get("permission_blocked_count")),
                "unresolvedColumnCount": _safe_int(detail.get("unresolved_column_count")),
                "harnessRetryCount": _safe_int(detail.get("harness_retry_count")),
                "sampleQuestions": sample_rows,
                "stageEvents": timeline,
            },
        )

    if step_num == 4:
        patches_applied = detail.get("patches_applied")
        if patches_applied is None:
            patches_applied = detail.get("patches_count")
        iteration_counter = detail.get("iteration_counter")
        if iteration_counter is None:
            iteration_counter = run_data.get("best_iteration")
        levers_accepted = detail.get("levers_accepted", [])
        levers_rolled_back = detail.get("levers_rolled_back", [])

        return (
            {
                "leverCountConfigured": len(run_data.get("levers", []))
                if isinstance(run_data.get("levers"), list)
                else None,
                "maxIterations": run_data.get("max_iterations"),
            },
            {
                "patchesApplied": patches_applied,
                "leversAccepted": levers_accepted,
                "leversRolledBack": levers_rolled_back,
                "iterationCounter": iteration_counter,
                "baselineAccuracy": run_data.get("baseline_accuracy"),
                "bestAccuracy": _safe_float(run_data.get("best_accuracy")),
                "stageEvents": timeline,
            },
        )

    if step_num == 5:
        return (
            {
                "bestIteration": run_data.get("best_iteration"),
            },
            {
                "bestAccuracy": _safe_float(run_data.get("best_accuracy")),
                "repeatability": _safe_float(run_data.get("best_repeatability")),
                "convergenceReason": run_data.get("convergence_reason"),
                "stageEvents": timeline,
            },
        )

    return None, {"stageEvents": timeline}


def _build_stage_timeline(matching: list[dict]) -> list[dict[str, Any]]:
    """Compact stage timeline for UI drill-down."""
    events: list[dict[str, Any]] = []
    for s in matching:
        events.append(
            {
                "stage": s.get("stage"),
                "status": str(s.get("status", "")).lower(),
                "startedAt": ensure_utc_iso(s.get("started_at")),
                "completedAt": ensure_utc_iso(s.get("completed_at")),
                "durationSeconds": _safe_float(s.get("duration_seconds")),
                "errorMessage": s.get("error_message"),
            }
        )
    return events


def _build_step_summary(
    defn: _StepDefinition,
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
    if step_num == 2:
        columns = detail.get("columns_collected", detail.get("columnsCollected", "?"))
        tags = detail.get("tags_collected", detail.get("tagsCollected", "?"))
        routines = detail.get("routines_collected", detail.get("routinesCollected", "?"))
        return (
            f"Collected metadata for {columns} columns, {tags} tags, "
            f"{routines} routines from Unity Catalog"
        )
    if step_num == 3:
        baseline_iter = next(
            (r for r in iterations_rows if r.get("iteration") == 0),
            None,
        )
        score = "?"
        questions = "?"
        if baseline_iter:
            score = f"{_finite(baseline_iter.get('overall_accuracy', 0)):.1f}"
            questions = str(baseline_iter.get("total_questions", "?"))
        return defn["summary_template"].format(questions=questions, score=score)
    if step_num == 4:
        patches = detail.get("patches_applied", 0)
        levers_accepted = detail.get("levers_accepted", [])
        before = f"{_finite(run_data.get('baseline_accuracy', 0)):.1f}" if run_data.get("baseline_accuracy") else "?"
        after = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        return defn["summary_template"].format(
            patches=patches,
            levers=len(levers_accepted) if isinstance(levers_accepted, list) else "?",
            before=before,
            after=after,
        )
    if step_num == 5:
        score = f"{_finite(run_data.get('best_accuracy', 0)):.1f}" if run_data.get("best_accuracy") else "?"
        rep = f"{_finite(run_data.get('best_repeatability', 0)):.1f}" if run_data.get("best_repeatability") else "?"
        return defn["summary_template"].format(score=score, repeatability=rep)

    return None


def _build_levers(
    stages_rows: list[dict],
    *,
    run_status: str = "",
    configured_levers: list[int] | None = None,
    patches_rows: list[dict] | None = None,
    iterations_rows: list[dict] | None = None,
) -> list[LeverStatus]:
    """Build lever detail from LEVER_* stage rows."""
    from genie_space_optimizer.common.config import LEVER_NAMES

    lever_data: dict[int, dict] = {}
    for configured in configured_levers or []:
        try:
            lever_data[int(configured)] = {"stages": [], "detail": {}, "patches": []}
        except (TypeError, ValueError):
            continue

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
        try:
            lever_num_float = float(lever_num)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(lever_num_float):
            continue
        lever_num = int(lever_num_float)
        if lever_num not in lever_data:
            lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
        lever_data[lever_num]["stages"].append(s)
        lever_data[lever_num]["detail"].update(_parse_detail(s))

    for p in patches_rows or []:
        lever_num = _safe_int(p.get("lever"))
        if lever_num is None:
            continue
        if lever_num not in lever_data:
            lever_data[lever_num] = {"stages": [], "detail": {}, "patches": []}
        lever_data[lever_num]["patches"].append(_patch_for_ui(p))

    levers: list[LeverStatus] = []
    for lever_num in sorted(lever_data.keys()):
        data = lever_data[lever_num]
        ld = data["detail"]
        status = _derive_lever_status(data["stages"])
        status = _normalize_lever_status_for_terminal_run(
            status=status,
            run_status=run_status,
        )
        patches_total = len(data.get("patches", []))
        detail_patch_count = _safe_int(ld.get("patches_applied"))
        patches_applied = patches_total if patches_total > 0 else (detail_patch_count or 0)

        rollback_reason = None
        if status == "rolled_back":
            rollback_reason = ld.get("reason", "regression")

        lever_iterations = _build_lever_iterations(
            lever_num=lever_num,
            lever_stages=data.get("stages", []),
            iterations_rows=iterations_rows or [],
            patches_rows=patches_rows or [],
            run_status=run_status,
        )

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
                patches=data.get("patches", []),
                iterations=lever_iterations,
            )
        )
    return levers


def _patch_for_ui(row: dict) -> dict[str, Any]:
    """Convert patch table row to compact UI object."""
    patch = _maybe_json(row.get("patch_json"))
    command = _maybe_json(row.get("command_json"))
    return {
        "patchType": row.get("patch_type"),
        "scope": row.get("scope"),
        "riskLevel": row.get("risk_level"),
        "targetObject": row.get("target_object"),
        "rolledBack": bool(row.get("rolled_back")) if row.get("rolled_back") is not None else False,
        "rollbackReason": row.get("rollback_reason"),
        "command": command if isinstance(command, dict) else command,
        "patch": patch if isinstance(patch, dict) else patch,
        "appliedAt": str(row.get("applied_at")) if row.get("applied_at") is not None else None,
    }


_maybe_json = safe_json_parse


def _build_lever_iterations(
    *,
    lever_num: int,
    lever_stages: list[dict],
    iterations_rows: list[dict],
    patches_rows: list[dict],
    run_status: str,
) -> list[dict[str, Any]]:
    """Build iteration-by-iteration transparency payload for one lever."""
    by_iter: dict[int, dict[str, Any]] = {}

    for stage in lever_stages:
        iteration = _safe_int(stage.get("iteration"))
        if iteration is None:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["stages"].append(stage)
        entry["detail"].update(_parse_detail(stage))

    for row in iterations_rows:
        if _safe_int(row.get("lever")) != lever_num:
            continue
        iteration = _safe_int(row.get("iteration"))
        if iteration is None:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["rows"].append(row)

    for patch_row in patches_rows:
        if _safe_int(patch_row.get("lever")) != lever_num:
            continue
        iteration = _safe_int(patch_row.get("iteration"))
        if iteration is None:
            continue
        entry = by_iter.setdefault(iteration, {"stages": [], "detail": {}, "patches": [], "rows": []})
        entry["patches"].append(_patch_for_ui(patch_row))

    iteration_payloads: list[dict[str, Any]] = []
    for iteration in sorted(by_iter.keys()):
        entry = by_iter[iteration]
        stage_status = _derive_lever_status(entry["stages"])
        status = _normalize_lever_status_for_terminal_run(status=stage_status, run_status=run_status)
        detail = entry["detail"]

        full_row = next(
            (r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "full"),
            None,
        )
        slice_row = next(
            (r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "slice"),
            None,
        )
        p0_row = next(
            (r for r in entry["rows"] if str(r.get("eval_scope", "")).lower() == "p0"),
            None,
        )
        score_after = (
            _safe_float(full_row.get("overall_accuracy")) if full_row else _safe_float(detail.get("accuracy"))
        )
        score_before = _safe_float(detail.get("score_before"))
        score_delta = _safe_float(detail.get("score_delta"))
        if score_delta is None and score_before is not None and score_after is not None:
            score_delta = round(score_after - score_before, 2)

        judge_scores = _iteration_scores(full_row)
        patch_types = [str(p.get("patchType") or "") for p in entry["patches"] if p.get("patchType")]
        rollback_reason = detail.get("reason")
        if not rollback_reason and status == "rolled_back":
            rollback_reason = "regression"

        iteration_payloads.append(
            {
                "iteration": iteration,
                "status": status,
                "patchCount": len(entry["patches"]),
                "patchTypes": patch_types,
                "scoreBefore": score_before,
                "scoreAfter": score_after,
                "scoreDelta": score_delta,
                "sliceAccuracy": _safe_float(slice_row.get("overall_accuracy")) if slice_row else None,
                "p0Accuracy": _safe_float(p0_row.get("overall_accuracy")) if p0_row else None,
                "fullAccuracy": _safe_float(full_row.get("overall_accuracy")) if full_row else None,
                "totalQuestions": _safe_int(full_row.get("total_questions")) if full_row else None,
                "correctCount": _safe_int(full_row.get("correct_count")) if full_row else None,
                "judgeScores": judge_scores,
                "mlflowRunId": full_row.get("mlflow_run_id") if full_row else None,
                "rollbackReason": rollback_reason,
                "patches": entry["patches"],
                "stageEvents": _build_stage_timeline(entry["stages"]),
            }
        )

    return iteration_payloads


def _iteration_scores(iter_row: dict | None) -> dict[str, float | None]:
    """Parse and normalize per-judge score payload for one evaluation row."""
    if not iter_row:
        return {}
    scores = iter_row.get("scores_json", {})
    if isinstance(scores, str):
        try:
            scores = json.loads(scores)
        except (json.JSONDecodeError, TypeError):
            scores = {}
    if not isinstance(scores, dict):
        return {}
    return {str(k): _safe_float(v) for k, v in scores.items()}


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


def _normalize_step_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Avoid stale 'running' steps when the overall run is already terminal."""
    if status != "running":
        return status
    normalized = run_status.upper()
    if normalized == "FAILED":
        return "failed"
    if normalized in {"CANCELLED", "DISCARDED"}:
        return "pending"
    if normalized in _TERMINAL_RUN_STATUSES:
        return "completed"
    return status


def _normalize_lever_status_for_terminal_run(*, status: str, run_status: str) -> str:
    """Avoid stale active lever states after the run is terminal."""
    if status not in {"running", "evaluating"}:
        return status
    normalized = run_status.upper()
    if normalized == "FAILED":
        return "failed"
    if normalized in {"CANCELLED", "DISCARDED"}:
        return "skipped"
    if normalized in _TERMINAL_RUN_STATUSES:
        return "skipped"
    return status


def _get_baseline_and_best_accuracy(iters_rows: list[dict]) -> tuple[float | None, float | None]:
    """Return baseline(full iteration 0) and best full-eval accuracy."""
    full_rows = [r for r in iters_rows if str(r.get("eval_scope", "")).lower() == "full"]
    if not full_rows:
        return None, None

    baseline_row = next((r for r in full_rows if int(r.get("iteration", -1)) == 0), None)
    baseline = _safe_float(baseline_row.get("overall_accuracy")) if baseline_row else None

    scores = [_safe_float(r.get("overall_accuracy")) for r in full_rows]
    finite_scores = [s for s in scores if s is not None]
    if not finite_scores:
        return baseline, None
    return baseline, max(finite_scores)


# ── Route Handlers ──────────────────────────────────────────────────────


_ACTIVE_RUN_STATUSES_LOCAL = ACTIVE_RUN_STATUSES
_TERMINAL_JOB_STATES_LOCAL = TERMINAL_JOB_STATES


def _reconcile_single_run(
    spark,
    run_data: dict,
    sp_ws: WorkspaceClient,
    catalog: str,
    schema_name: str,
) -> dict:
    """If the Delta status is active but the Databricks job has terminated,
    update Delta and return refreshed run_data so the caller sees the real state."""
    from genie_space_optimizer.optimization.state import load_run, update_run_status

    status = str(run_data.get("status") or "")
    if status not in _ACTIVE_RUN_STATUSES_LOCAL:
        return run_data

    job_run_id = run_data.get("job_run_id")
    if not job_run_id:
        return run_data

    try:
        run_obj = sp_ws.jobs.get_run(run_id=int(str(job_run_id)))
        life_cycle = (
            str(run_obj.state.life_cycle_state).split(".")[-1]
            if run_obj.state else ""
        )
        if life_cycle not in _TERMINAL_JOB_STATES_LOCAL:
            return run_data

        result_state = (
            str(run_obj.state.result_state).split(".")[-1].lower()
            if run_obj.state else ""
        )
        new_status = "CANCELLED" if result_state == "canceled" else "FAILED"
        suffix = f":{result_state}" if result_state and result_state != "none" else ""
        update_run_status(
            spark,
            str(run_data["run_id"]),
            catalog,
            schema_name,
            status=new_status,
            convergence_reason=f"job_{life_cycle.lower()}_detected_on_poll{suffix}",
        )
        logger.info(
            "Reconciled run %s → %s (job lifecycle=%s, result=%s)",
            run_data["run_id"], new_status, life_cycle, result_state,
        )
        refreshed = load_run(spark, str(run_data["run_id"]), catalog, schema_name)
        return refreshed if refreshed else run_data
    except Exception:
        logger.debug("Could not reconcile run %s with job API", run_data.get("run_id"), exc_info=True)
        return run_data


@router.get("/runs/{run_id}", response_model=PipelineRun, operation_id="getRun")
def get_run(run_id: str, ws: Dependencies.UserClient, sp_ws: Dependencies.Client, config: Dependencies.Config):
    """Run status with 5 user-facing pipeline steps and lever detail."""
    from genie_space_optimizer.optimization.state import (
        load_iterations,
        load_patches,
        load_run,
        load_stages,
    )

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    run_data = _reconcile_single_run(spark, run_data, sp_ws, config.catalog, config.schema_name)

    stages_df = load_stages(spark, run_id, config.catalog, config.schema_name)
    stages_rows = stages_df.to_dict("records") if not stages_df.empty else []

    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []
    patches_df = load_patches(spark, run_id, config.catalog, config.schema_name)
    patches_rows = patches_df.to_dict("records") if not patches_df.empty else []

    baseline_iter = next((r for r in iters_rows if r.get("iteration") == 0), None)
    baseline_accuracy = _safe_float(baseline_iter.get("overall_accuracy")) if baseline_iter else None
    run_data["baseline_accuracy"] = baseline_accuracy

    steps = map_stages_to_steps(stages_rows, iters_rows, run_data)
    configured_levers = run_data.get("levers", []) if isinstance(run_data.get("levers"), list) else []
    configured_lever_ints: list[int] = []
    for lever in configured_levers:
        try:
            configured_lever_ints.append(int(lever))
        except (TypeError, ValueError):
            continue
    levers = _build_levers(
        stages_rows,
        run_status=str(run_data.get("status", "")),
        configured_levers=configured_lever_ints,
        patches_rows=patches_rows,
        iterations_rows=iters_rows,
    )

    status = run_data.get("status", "QUEUED")
    display_status = status
    if status == "IN_PROGRESS":
        display_status = "RUNNING"

    host = (ws.config.host or "").rstrip("/")
    links = _build_links(host, run_data, iters_rows, config, ws)
    baseline_eval_link = next(
        (
            l.url for l in links
            if l.category == "mlflow" and "baseline" in l.label.lower()
        ),
        None,
    )
    for step in steps:
        if step.stepNumber == 3 and isinstance(step.outputs, dict) and baseline_eval_link:
            step.outputs.setdefault("evaluationRunUrl", baseline_eval_link)

    result = PipelineRun(
        runId=run_id,
        spaceId=run_data.get("space_id", ""),
        spaceName=run_data.get("space_name", run_data.get("domain", "")),
        status=display_status,
        startedAt=ensure_utc_iso(run_data.get("started_at")) or "",
        completedAt=ensure_utc_iso(run_data.get("completed_at")),
        initiatedBy=run_data.get("triggered_by") or "system",
        baselineScore=baseline_accuracy,
        optimizedScore=_safe_float(run_data.get("best_accuracy")),
        steps=steps,
        levers=levers,
        convergenceReason=run_data.get("convergence_reason"),
        links=links,
    )

    try:
        dumped = result.model_dump(mode="json")
        import json as _json
        _json.dumps(dumped, allow_nan=False)
        logger.info("get_run serialization OK for %s", run_id)
    except (ValueError, TypeError) as exc:
        logger.error("get_run NaN detected in model_dump for %s: %s", run_id, exc)
        logger.error("Dumped data: %s", dumped)
    return result


@router.get(
    "/runs/{run_id}/comparison",
    response_model=ComparisonData,
    operation_id="getComparison",
)
def get_comparison(
    run_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
):
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
        client = _genie_client(ws, sp_ws)
        current_config = fetch_space_config(client, space_id)
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
            baseline_accuracy = _finite(row.get("overall_accuracy", 0))
        else:
            optimized_scores = scores_raw if isinstance(scores_raw, dict) else {}
            optimized_accuracy = _finite(row.get("overall_accuracy", 0))

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
                baseline=_finite(b),
                optimized=_finite(o),
                delta=_finite(o) - _finite(b),
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
def apply_optimization(
    run_id: str,
    ws: Dependencies.UserClient,
    config: Dependencies.Config,
):
    """Confirm optimized config.

    For runs requested with deferred UC writes (apply_mode=uc_artifact|both),
    execute those writes via OBO only after:
    1) optimization is terminal, and
    2) evaluation shows an improvement over baseline.
    """
    from genie_space_optimizer.optimization.state import (
        load_iterations,
        load_run,
        load_stages,
        update_run_status,
    )

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

    requested_mode = str(run_data.get("apply_mode") or "genie_config").lower()
    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    iters_rows = iters_df.to_dict("records") if not iters_df.empty else []
    baseline_score, best_score = _get_baseline_and_best_accuracy(iters_rows)

    if requested_mode in _DEFERRED_UC_MODES:
        if baseline_score is None or best_score is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Deferred UC writes require completed full-evaluation results "
                    "(baseline and optimized scores)."
                ),
            )
        if best_score <= baseline_score:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Deferred UC writes require a proven improvement in evaluation "
                    f"(baseline={baseline_score:.1f}%, optimized={best_score:.1f}%)."
                ),
            )

    stages_df = load_stages(spark, run_id, config.catalog, config.schema_name)
    stages_rows = stages_df.to_dict("records") if not stages_df.empty else []
    uc_apply_result = "not_required"
    if requested_mode in _DEFERRED_UC_MODES:
        try:
            uc_apply_result = _apply_deferred_uc_writes_obo(
                spark=spark,
                run_id=run_id,
                run_data=run_data,
                stages_rows=stages_rows,
                ws=ws,
                config=config,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    update_run_status(
        spark, run_id, config.catalog, config.schema_name,
        status="APPLIED",
    )
    message = "Optimization applied successfully."
    if requested_mode in _DEFERRED_UC_MODES:
        if uc_apply_result == "applied":
            message = "Optimization applied and deferred UC writes were executed via OBO."
        elif uc_apply_result == "already_recorded":
            message = "Optimization applied. Deferred UC write stage was already recorded."
        elif uc_apply_result.startswith("skipped_"):
            message = (
                "Optimization applied. Deferred UC writes were not needed for this run "
                f"({uc_apply_result})."
            )
    return ActionResponse(status="applied", runId=run_id, message=message)


@router.post(
    "/runs/{run_id}/discard",
    response_model=ActionResponse,
    operation_id="discardOptimization",
)
def discard_optimization(
    run_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
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
        client = _genie_client(ws, sp_ws)
        rollback(apply_log, client, space_id)

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


# ── Transparency Endpoints ──────────────────────────────────────────────


@router.get(
    "/runs/{run_id}/iterations",
    response_model=list[IterationSummary],
    operation_id="getIterations",
)
def get_iterations(run_id: str, config: Dependencies.Config):
    """All evaluation iterations for a run (baseline + lever evals)."""
    from genie_space_optimizer.optimization.state import load_iterations, load_run

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    iters_df = load_iterations(spark, run_id, config.catalog, config.schema_name)
    if iters_df.empty:
        return []

    results: list[IterationSummary] = []
    for row in iters_df.to_dict("records"):
        scores = row.get("scores_json", {})
        if isinstance(scores, str):
            try:
                scores = json.loads(scores)
            except (json.JSONDecodeError, TypeError):
                scores = {}
        if not isinstance(scores, dict):
            scores = {}

        results.append(
            IterationSummary(
                iteration=int(row.get("iteration", 0)),
                lever=_safe_int(row.get("lever")),
                evalScope=str(row.get("eval_scope", "full")),
                overallAccuracy=_finite(row.get("overall_accuracy", 0)),
                totalQuestions=int(row.get("total_questions", 0)),
                correctCount=int(row.get("correct_count", 0)),
                repeatabilityPct=_safe_float(row.get("repeatability_pct")),
                thresholdsMet=bool(row.get("thresholds_met", False)),
                judgeScores={str(k): _safe_float(v) for k, v in scores.items()},
            )
        )
    return results


@router.get(
    "/runs/{run_id}/asi-results",
    response_model=AsiSummary,
    operation_id="getAsiResults",
)
def get_asi_results(
    run_id: str,
    config: Dependencies.Config,
    iteration: int | None = None,
):
    """ASI judge feedback for a run, with summary statistics."""
    from genie_space_optimizer.optimization.state import load_asi_results, load_run

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    resolved_iteration = iteration if iteration is not None else 0
    df = load_asi_results(
        spark, run_id, config.catalog, config.schema_name, iteration=resolved_iteration,
    )
    if df.empty:
        return AsiSummary(
            runId=run_id, iteration=resolved_iteration,
            totalResults=0, passCount=0, failCount=0,
        )

    records = df.to_dict("records")
    results: list[AsiResult] = []
    pass_count = 0
    fail_count = 0
    failure_types: dict[str, int] = {}
    blame_counts: dict[str, int] = {}
    judge_totals: dict[str, int] = {}
    judge_passes: dict[str, int] = {}

    _PASS_VALUES = {"yes", "genie_correct", "pass", "true"}

    for row in records:
        value = str(row.get("value", "")).lower().strip()
        is_pass = value in _PASS_VALUES
        if is_pass:
            pass_count += 1
        else:
            fail_count += 1

        judge = str(row.get("judge", ""))
        judge_totals[judge] = judge_totals.get(judge, 0) + 1
        if is_pass:
            judge_passes[judge] = judge_passes.get(judge, 0) + 1

        ft = row.get("failure_type")
        if ft and not is_pass:
            ft_str = str(ft)
            failure_types[ft_str] = failure_types.get(ft_str, 0) + 1

        blame_raw = row.get("blame_set")
        blame_list: list[str] = []
        if blame_raw:
            if isinstance(blame_raw, str):
                try:
                    blame_list = json.loads(blame_raw)
                except (json.JSONDecodeError, TypeError):
                    blame_list = []
            elif isinstance(blame_raw, list):
                blame_list = [str(x) for x in blame_raw]

        results.append(
            AsiResult(
                questionId=str(row.get("question_id", "")),
                judge=judge,
                value=str(row.get("value", "")),
                failureType=row.get("failure_type"),
                severity=row.get("severity"),
                confidence=_safe_float(row.get("confidence")),
                blameSet=blame_list,
                counterfactualFix=row.get("counterfactual_fix"),
                wrongClause=row.get("wrong_clause"),
                expectedValue=row.get("expected_value"),
                actualValue=row.get("actual_value"),
            )
        )

    judge_pass_rates = {
        j: round((judge_passes.get(j, 0) / judge_totals[j]) * 100, 1)
        for j in sorted(judge_totals)
        if judge_totals[j] > 0
    }

    return AsiSummary(
        runId=run_id,
        iteration=resolved_iteration,
        totalResults=len(records),
        passCount=pass_count,
        failCount=fail_count,
        failureTypeDistribution=failure_types,
        blameDistribution=blame_counts,
        judgePassRates=judge_pass_rates,
        results=results,
    )


@router.get(
    "/runs/{run_id}/provenance",
    response_model=list[ProvenanceSummary],
    operation_id="getProvenance",
)
def get_provenance(
    run_id: str,
    config: Dependencies.Config,
    iteration: int | None = None,
    lever: int | None = None,
):
    """Provenance lineage for a run, grouped by iteration+lever."""
    from genie_space_optimizer.optimization.state import load_provenance, load_run

    spark = get_spark()
    run_data = load_run(spark, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    df = load_provenance(
        spark, run_id, config.catalog, config.schema_name,
        iteration=iteration, lever=lever,
    )
    if df.empty:
        return []

    groups: dict[tuple[int, int], list[dict]] = {}
    for row in df.to_dict("records"):
        key = (int(row.get("iteration", 0)), int(row.get("lever", 0)))
        groups.setdefault(key, []).append(row)

    summaries: list[ProvenanceSummary] = []
    for (iter_num, lever_num), rows in sorted(groups.items()):
        prov_records: list[ProvenanceRecord] = []
        root_causes: dict[str, int] = {}
        gate_results: dict[str, int] = {}
        clusters: set[str] = set()
        proposals: set[str] = set()

        for row in rows:
            rc = str(row.get("resolved_root_cause", ""))
            if rc:
                root_causes[rc] = root_causes.get(rc, 0) + 1

            gr = row.get("gate_result")
            if gr:
                gr_str = str(gr)
                gate_results[gr_str] = gate_results.get(gr_str, 0) + 1

            cid = str(row.get("cluster_id", ""))
            if cid:
                clusters.add(cid)

            pid = row.get("proposal_id")
            if pid:
                proposals.add(str(pid))

            blame_raw = row.get("blame_set")
            blame_list: list[str] = []
            if blame_raw:
                if isinstance(blame_raw, str):
                    try:
                        blame_list = json.loads(blame_raw)
                    except (json.JSONDecodeError, TypeError):
                        blame_list = []
                elif isinstance(blame_raw, list):
                    blame_list = [str(x) for x in blame_raw]

            prov_records.append(
                ProvenanceRecord(
                    questionId=str(row.get("question_id", "")),
                    signalType=str(row.get("signal_type", "")),
                    judge=str(row.get("judge", "")),
                    judgeVerdict=str(row.get("judge_verdict", "")),
                    resolvedRootCause=rc,
                    resolutionMethod=str(row.get("resolution_method", "")),
                    blameSet=blame_list,
                    counterfactualFix=row.get("counterfactual_fix"),
                    clusterId=cid,
                    proposalId=row.get("proposal_id"),
                    patchType=row.get("patch_type"),
                    gateType=row.get("gate_type"),
                    gateResult=row.get("gate_result"),
                )
            )

        summaries.append(
            ProvenanceSummary(
                runId=run_id,
                iteration=iter_num,
                lever=lever_num,
                totalRecords=len(prov_records),
                clusterCount=len(clusters),
                proposalCount=len(proposals),
                rootCauseDistribution=root_causes,
                gateResults=gate_results,
                records=prov_records,
            )
        )

    return summaries


# ── Helpers ─────────────────────────────────────────────────────────────


def _build_links(
    host: str,
    run_data: dict,
    iters_rows: list[dict],
    config,
    ws: WorkspaceClient,
) -> list[PipelineLink]:
    """Build external Databricks links from run metadata."""
    links: list[PipelineLink] = []
    if not host:
        return links

    space_id = run_data.get("space_id")
    if space_id:
        links.append(PipelineLink(
            label="Genie Space",
            url=f"{host}/genie/rooms/{space_id}",
            category="genie",
        ))

    job_run_id = run_data.get("job_run_id")
    if job_run_id and job_run_id not in ("pending", ""):
        stored_job_id = run_data.get("job_id")
        resolved_job_id: int | None = int(str(stored_job_id)) if stored_job_id else None

        if resolved_job_id is None:
            try:
                run = ws.jobs.get_run(run_id=int(str(job_run_id)))
                if run.job_id is not None:
                    resolved_job_id = int(run.job_id)
            except Exception:
                pass

        if resolved_job_id is not None:
            workspace_id = ws.get_workspace_id()
            job_url = (
                f"{host}/jobs/{resolved_job_id}/runs/{int(str(job_run_id))}"
                f"?o={workspace_id}"
            )
            links.append(PipelineLink(
                label="Optimization Job Run",
                url=job_url,
                category="job",
            ))

    experiment_id = run_data.get("experiment_id")
    experiment_name = run_data.get("experiment_name")
    if experiment_id:
        links.append(PipelineLink(
            label="MLflow Experiment",
            url=f"{host}/ml/experiments/{experiment_id}",
            category="mlflow",
        ))
    elif experiment_name:
        from urllib.parse import quote
        links.append(PipelineLink(
            label="MLflow Experiment",
            url=f"{host}/ml/experiments?searchFilter={quote(experiment_name)}",
            category="mlflow",
        ))

    for row in iters_rows:
        mlflow_run_id = row.get("mlflow_run_id")
        iteration = row.get("iteration")
        if mlflow_run_id and experiment_id:
            label = "Baseline Evaluation" if iteration == 0 else f"Iteration {iteration} Evaluation"
            links.append(PipelineLink(
                label=label,
                url=f"{host}/ml/experiments/{experiment_id}/runs/{mlflow_run_id}",
                category="mlflow",
            ))

    best_model_id = run_data.get("best_model_id")
    if best_model_id:
        links.append(PipelineLink(
            label="Best Model",
            url=f"{host}/ml/models/{best_model_id}",
            category="mlflow",
        ))

    labeling_url = run_data.get("labeling_session_url")
    labeling_name = run_data.get("labeling_session_name")
    if labeling_url:
        links.append(PipelineLink(
            label=f"Human Review: {labeling_name}" if labeling_name else "Human Review Session",
            url=labeling_url,
            category="review",
        ))
    elif labeling_name and experiment_id:
        links.append(PipelineLink(
            label=f"Human Review: {labeling_name}",
            url=f"{host}/ml/experiments/{experiment_id}",
            category="review",
        ))

    catalog = config.catalog
    schema = config.schema_name
    if catalog and schema:
        links.append(PipelineLink(
            label="Runs Table",
            url=f"{host}/explore/data/{catalog}/{schema}/genie_opt_runs",
            category="data",
        ))
        links.append(PipelineLink(
            label="Iterations Table",
            url=f"{host}/explore/data/{catalog}/{schema}/genie_opt_iterations",
            category="data",
        ))

    return links


def _extract_space_configuration(ss: dict) -> SpaceConfiguration:
    """Extract SpaceConfiguration from a serialized_space dict."""
    if not ss or not isinstance(ss, dict):
        return SpaceConfiguration(instructions="", sampleQuestions=[], tableDescriptions=[])

    def _to_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return "\n".join(part for part in (_to_text(v) for v in value) if part)
        if isinstance(value, dict):
            if "question" in value:
                return _to_text(value.get("question"))
            if "content" in value:
                return _to_text(value.get("content"))
            return _to_text(list(value.values()))
        return str(value)

    instr = ss.get("instructions", {})
    text_instr = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instructions_str = ""
    if text_instr and isinstance(text_instr, list) and text_instr:
        content = text_instr[0].get("content", [])
        instructions_str = _to_text(content)

    example_qs = instr.get("example_question_sqls", []) if isinstance(instr, dict) else []
    questions: list[str] = []
    for q in example_qs:
        q_val = q.get("question", "") if isinstance(q, dict) else q
        q_text = _to_text(q_val).strip()
        if q_text:
            questions.append(q_text)

    ds = ss.get("data_sources", {})
    tables = ds.get("tables", []) if isinstance(ds, dict) else []
    table_descs: list[TableDescription] = []
    for t in tables:
        ident = t.get("identifier", "")
        desc_str = _to_text(t.get("description", [])).strip()
        table_descs.append(TableDescription(tableName=ident, description=desc_str))

    return SpaceConfiguration(
        instructions=instructions_str,
        sampleQuestions=questions,
        tableDescriptions=table_descs,
    )


_safe_float = safe_float
_safe_int = safe_int
_finite = safe_finite


# ═══════════════════════════════════════════════════════════════════════
# Pending Reviews
# ═══════════════════════════════════════════════════════════════════════

from ..models import PendingReviewItem, PendingReviewsOut


@router.get(
    "/pending-reviews/{space_id}",
    response_model=PendingReviewsOut,
    operation_id="getPendingReviews",
)
def get_pending_reviews(space_id: str, config: Dependencies.Config) -> PendingReviewsOut:
    """Return counts and details of items awaiting human review for a space."""
    spark = get_spark()
    if not spark:
        return PendingReviewsOut()

    catalog = config.catalog
    schema = config.schema_name

    items: list[PendingReviewItem] = []
    flagged_count = 0
    queued_count = 0
    labeling_url: str | None = None

    try:
        from genie_space_optimizer.optimization.labeling import get_flagged_questions
        flagged = get_flagged_questions(spark, catalog, schema, space_id)
        flagged_count = len(flagged)
        for f in flagged[:5]:
            items.append(PendingReviewItem(
                questionId=f.get("question_id", ""),
                questionText=f.get("question_text", "")[:200],
                reason=f.get("flag_reason", ""),
                itemType="flagged_question",
            ))
    except Exception:
        logger.debug("Could not load flagged questions", exc_info=True)

    try:
        from genie_space_optimizer.optimization.state import get_queued_patches
        queued = get_queued_patches(spark, catalog, schema)
        queued_count = len(queued)
        for q in queued[:5 - len(items)]:
            items.append(PendingReviewItem(
                questionId=q.get("target_identifier", ""),
                reason=f"Queued {q.get('patch_type', '')}",
                confidenceTier=q.get("confidence_tier", ""),
                itemType="queued_patch",
            ))
    except Exception:
        logger.debug("Could not load queued patches", exc_info=True)

    try:
        from genie_space_optimizer.optimization.state import run_query
        fqn = f"{catalog}.{schema}.genie_opt_runs"
        df = run_query(
            spark,
            f"SELECT labeling_session_url FROM {fqn} "
            f"WHERE space_id = '{space_id}' AND labeling_session_url IS NOT NULL "
            f"ORDER BY started_at DESC LIMIT 1",
        )
        if not df.empty:
            labeling_url = df.iloc[0].get("labeling_session_url")
    except Exception:
        logger.debug("Could not load labeling session URL", exc_info=True)

    return PendingReviewsOut(
        flaggedQuestions=flagged_count,
        queuedPatches=queued_count,
        totalPending=flagged_count + queued_count,
        labelingSessionUrl=labeling_url,
        items=items,
    )
