"""
Optimization Harness — stage functions for the 5-task Databricks Job.

Each ``_run_*`` function is callable independently from a job notebook.
The ``optimize_genie_space()`` convenience function runs all 5 stages
in a single process for notebook/test use.

Architecture: ``preflight`` → ``baseline_eval`` → ``lever_loop`` →
``finalize`` → ``deploy``.  Inter-task data flows via
``dbutils.jobs.taskValues``. Detailed state goes to Delta.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    DEFAULT_LEVER_ORDER,
    DEFAULT_THRESHOLDS,
    INLINE_EVAL_DELAY,
    LEVER_NAMES,
    MAX_ITERATIONS,
    PROPAGATION_WAIT_SECONDS,
    REGRESSION_THRESHOLD,
    SLICE_GATE_TOLERANCE,
)
from genie_space_optimizer.optimization.applier import (
    _get_general_instructions,
    apply_patch_set,
    proposals_to_patches,
    rollback,
)
from genie_space_optimizer.optimization.evaluation import (
    all_thresholds_met,
    filter_benchmarks_by_scope,
    make_predict_fn,
    normalize_scores,
    register_instruction_version,
    run_evaluation,
)
from genie_space_optimizer.optimization.models import (
    create_genie_model_version,
    link_eval_scores_to_model,
    promote_best_model,
)
from genie_space_optimizer.optimization.optimizer import (
    cluster_failures,
    detect_regressions,
    generate_metadata_proposals,
)
from genie_space_optimizer.optimization.preflight import run_preflight
from genie_space_optimizer.optimization.repeatability import run_repeatability_test
from genie_space_optimizer.optimization.report import generate_report
from genie_space_optimizer.optimization.scorers import make_all_scorers
from genie_space_optimizer.optimization.state import (
    create_run,
    ensure_optimization_tables,
    load_latest_full_iteration,
    load_stages,
    mark_patches_rolled_back,
    update_run_status,
    write_iteration,
    write_patch,
    write_stage,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

FINALIZE_TIMEOUT_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS", "6600"),
)
FINALIZE_HEARTBEAT_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS", "30"),
)


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_sql_context(spark: SparkSession, catalog: str, schema: str) -> None:
    """Set Spark SQL catalog/schema context explicitly for SQL Connect stability."""
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")


# ── Result Dataclass ──────────────────────────────────────────────────


@dataclass
class OptimizationResult:
    """Outcome of an optimization run (used by convenience function)."""

    run_id: str
    space_id: str
    domain: str
    status: str  # CONVERGED | STALLED | MAX_ITERATIONS | FAILED
    best_iteration: int
    best_accuracy: float
    best_repeatability: float
    best_model_id: str | None
    convergence_reason: str | None
    total_iterations: int
    levers_attempted: list[int] = field(default_factory=list)
    levers_accepted: list[int] = field(default_factory=list)
    levers_rolled_back: list[int] = field(default_factory=list)
    final_scores: dict[str, float] = field(default_factory=dict)
    experiment_name: str = ""
    experiment_id: str = ""
    report_path: str | None = None
    error: str | None = None


# ── Error Handling ────────────────────────────────────────────────────


def _safe_stage(
    state_spark: Any,
    run_id: str,
    stage_name: str,
    fn: Any,
    state_catalog: str,
    state_schema: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Wrap a stage function — on exception write FAILED to Delta and re-raise."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("Stage %s FAILED for run %s", stage_name, run_id)
        try:
            write_stage(
                state_spark, run_id, stage_name, "FAILED",
                error_message=err_msg[:500],
                catalog=state_catalog, schema=state_schema,
            )
            update_run_status(
                state_spark, run_id, state_catalog, state_schema,
                status="FAILED",
                convergence_reason=f"error_in_{stage_name}",
            )
        except Exception:
            logger.exception("Failed to write FAILED state for %s", run_id)
        raise


# ── Stage 1: PREFLIGHT ───────────────────────────────────────────────


def _run_preflight(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    experiment_name: str | None = None,
) -> dict:
    """Stage 1: Fetch config, UC metadata, generate/load benchmarks, create experiment.

    Returns a dict of task values to pass downstream.
    """
    config, benchmarks, model_id, exp_name = _safe_stage(
        spark, run_id, "PREFLIGHT", run_preflight,
        catalog, schema,
        w, spark, run_id, space_id, catalog, schema, domain, experiment_name,
    )

    import mlflow
    exp = mlflow.get_experiment_by_name(exp_name)
    experiment_id = exp.experiment_id if exp else ""

    update_run_status(
        spark, run_id, catalog, schema,
        status="IN_PROGRESS",
        experiment_name=exp_name,
        experiment_id=experiment_id,
    )

    return {
        "benchmarks": benchmarks,
        "config": config,
        "model_id": model_id,
        "experiment_name": exp_name,
        "experiment_id": experiment_id,
    }


# ── Stage 2: BASELINE EVAL ──────────────────────────────────────────


def _run_baseline(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    benchmarks: list[dict],
    exp_name: str,
    model_id: str,
    catalog: str,
    schema: str,
    domain: str = "",
) -> dict:
    """Stage 2: Run full 8-judge evaluation, check thresholds.

    Returns a dict with scores, thresholds_met flag, and model_id.
    """
    write_stage(
        spark, run_id, "BASELINE_EVAL_STARTED", "STARTED",
        task_key="baseline_eval", catalog=catalog, schema=schema,
    )
    _ensure_sql_context(spark, catalog, schema)

    try:
        # Strict SQL/routine gating now lives inside run_evaluation so all eval
        # scopes (baseline, slice, p0, full) share one consistent quarantine path.
        baseline_benchmarks = benchmarks

        _ensure_sql_context(spark, catalog, schema)
        predict_fn = make_predict_fn(w, space_id, spark, catalog, schema)
        scorers = make_all_scorers(w, spark, catalog, schema)

        eval_result = _safe_stage(
            spark, run_id, "BASELINE_EVAL", run_evaluation,
            catalog, schema,
            space_id, exp_name, 0, baseline_benchmarks, domain, model_id, "full",
            predict_fn, scorers,
            spark=spark, catalog=catalog, gold_schema=schema, uc_schema=f"{catalog}.{schema}",
        )

        scores = eval_result.get("scores", {})
        thresholds_met = eval_result.get("thresholds_met", False)

        write_iteration(
            spark, run_id, 0, eval_result,
            catalog=catalog, schema=schema,
            eval_scope="full", model_id=model_id,
        )

        link_eval_scores_to_model(model_id, scores)

        update_run_status(
            spark, run_id, catalog, schema,
            best_iteration=0,
            best_accuracy=eval_result.get("overall_accuracy", 0.0),
            best_model_id=model_id,
        )

        write_stage(
            spark, run_id, "BASELINE_EVAL_STARTED", "COMPLETE",
            task_key="baseline_eval",
            detail={
                "overall_accuracy": eval_result.get("overall_accuracy", 0.0),
                "thresholds_met": thresholds_met,
                "invalid_benchmark_count": eval_result.get("invalid_benchmark_count", 0),
                "permission_blocked_count": eval_result.get("permission_blocked_count", 0),
                "unresolved_column_count": eval_result.get("unresolved_column_count", 0),
                "harness_retry_count": eval_result.get("harness_retry_count", 0),
            },
            catalog=catalog, schema=schema,
        )

        return {
            "scores": scores,
            "overall_accuracy": eval_result.get("overall_accuracy", 0.0),
            "thresholds_met": thresholds_met,
            "model_id": model_id,
            "eval_result": eval_result,
        }
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("BASELINE_EVAL FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "BASELINE_EVAL", "FAILED",
            task_key="baseline_eval",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        update_run_status(
            spark, run_id, catalog, schema,
            status="FAILED",
            convergence_reason="error_in_BASELINE_EVAL",
        )
        raise


# ── Stage 3: LEVER LOOP ─────────────────────────────────────────────


def _run_lever_loop(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    benchmarks: list[dict],
    exp_name: str,
    prev_scores: dict[str, float],
    prev_accuracy: float,
    prev_model_id: str,
    config: dict,
    catalog: str,
    schema: str,
    levers: list[int] | None = None,
    max_iterations: int = MAX_ITERATIONS,
    thresholds: dict[str, float] | None = None,
    apply_mode: str = APPLY_MODE,
) -> dict:
    """Stage 3: Iterate levers with convergence checking.

    Internal Python loop over levers. Supports resume on task retry.

    Returns dict with best scores, model_id, iteration_counter, levers lists.
    """
    levers = levers or DEFAULT_LEVER_ORDER
    thresholds = thresholds or DEFAULT_THRESHOLDS

    write_stage(
        spark, run_id, "LEVER_LOOP_STARTED", "STARTED",
        task_key="lever_loop", catalog=catalog, schema=schema,
    )
    _ensure_sql_context(spark, catalog, schema)

    resume_state = _resume_lever_loop(spark, run_id, catalog, schema)
    start_lever = resume_state.get("resume_from_lever", 0)
    iteration_counter = resume_state.get("iteration_counter", 0)
    if resume_state.get("prev_scores"):
        prev_scores = resume_state["prev_scores"]
    if resume_state.get("prev_model_id"):
        prev_model_id = resume_state["prev_model_id"]
    if resume_state.get("prev_accuracy"):
        prev_accuracy = resume_state["prev_accuracy"]

    best_scores = dict(prev_scores)
    best_accuracy = prev_accuracy
    best_model_id = prev_model_id
    best_iteration = iteration_counter

    levers_attempted: list[int] = []
    levers_accepted: list[int] = []
    levers_rolled_back: list[int] = []

    _ensure_sql_context(spark, catalog, schema)
    predict_fn = make_predict_fn(w, space_id, spark, catalog, schema)
    scorers = make_all_scorers(w, spark, catalog, schema)
    uc_schema = f"{catalog}.{schema}"
    metadata_snapshot = config.get("_parsed_space", config)

    for lever in levers:
        if lever <= start_lever:
            continue

        if all_thresholds_met(best_scores, thresholds):
            logger.info("Convergence: all thresholds met at lever %d", lever)
            break

        if iteration_counter >= max_iterations:
            logger.info("Max iterations (%d) reached", max_iterations)
            break

        iteration_counter += 1
        levers_attempted.append(lever)
        lever_name = LEVER_NAMES.get(lever, f"Lever {lever}")

        write_stage(
            spark, run_id, f"LEVER_{lever}_STARTED", "STARTED",
            task_key="lever_loop", lever=lever, iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )

        eval_result_for_clustering = {
            "rows": _get_failure_rows(spark, run_id, catalog, schema),
        }
        clusters = cluster_failures(eval_result_for_clustering, metadata_snapshot)

        proposals = generate_metadata_proposals(
            clusters, metadata_snapshot,
            target_lever=lever, apply_mode=apply_mode, w=w,
        )

        if not proposals:
            logger.info("No proposals for %s — skipping", lever_name)
            write_stage(
                spark, run_id, f"LEVER_{lever}_STARTED", "SKIPPED",
                task_key="lever_loop", lever=lever, iteration=iteration_counter,
                detail={"reason": "no_proposals"},
                catalog=catalog, schema=schema,
            )
            continue

        patches = proposals_to_patches(proposals)
        apply_log = apply_patch_set(
            w, space_id, patches, metadata_snapshot, apply_mode=apply_mode,
        )

        for idx, entry in enumerate(apply_log.get("applied", [])):
            write_patch(
                spark, run_id, iteration_counter, lever, idx,
                _build_patch_record(entry, lever, apply_mode),
                catalog, schema,
            )

        patched_objects = apply_log.get("patched_objects", [])

        logger.info("Waiting %ds for propagation", PROPAGATION_WAIT_SECONDS)
        time.sleep(PROPAGATION_WAIT_SECONDS)

        # ── Slice gate ────────────────────────────────────────────
        slice_benchmarks = filter_benchmarks_by_scope(
            benchmarks, "slice", patched_objects,
        )
        if slice_benchmarks:
            _ensure_sql_context(spark, catalog, schema)
            write_stage(
                spark, run_id, f"LEVER_{lever}_SLICE_EVAL", "STARTED",
                task_key="lever_loop", lever=lever, iteration=iteration_counter,
                catalog=catalog, schema=schema,
            )
            slice_result = run_evaluation(
                space_id, exp_name, iteration_counter, slice_benchmarks,
                domain, prev_model_id, "slice",
                predict_fn, scorers,
                spark=spark, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
                patched_objects=patched_objects,
            )
            slice_scores = slice_result.get("scores", {})
            slice_drops = detect_regressions(
                slice_scores, best_scores, threshold=SLICE_GATE_TOLERANCE,
            )
            if slice_drops:
                logger.warning(
                    "Slice gate FAILED for lever %d: %s", lever, slice_drops,
                )
                rollback(apply_log, w, space_id, metadata_snapshot)
                mark_patches_rolled_back(
                    spark, run_id, iteration_counter,
                    f"slice_gate_regression: {slice_drops[0]['judge']}",
                    catalog, schema,
                )
                levers_rolled_back.append(lever)
                write_stage(
                    spark, run_id, f"LEVER_{lever}_STARTED", "ROLLED_BACK",
                    task_key="lever_loop", lever=lever, iteration=iteration_counter,
                    detail={"reason": "slice_gate", "regressions": slice_drops},
                    catalog=catalog, schema=schema,
                )
                continue

        # ── P0 gate ───────────────────────────────────────────────
        p0_benchmarks = filter_benchmarks_by_scope(benchmarks, "p0")
        if p0_benchmarks:
            _ensure_sql_context(spark, catalog, schema)
            p0_result = run_evaluation(
                space_id, exp_name, iteration_counter, p0_benchmarks,
                domain, prev_model_id, "p0",
                predict_fn, scorers,
                spark=spark, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            )
            p0_failures = p0_result.get("failures", [])
            if p0_failures:
                logger.warning("P0 gate FAILED: %d P0 questions failing", len(p0_failures))
                rollback(apply_log, w, space_id, metadata_snapshot)
                mark_patches_rolled_back(
                    spark, run_id, iteration_counter,
                    f"p0_gate_failure: {len(p0_failures)} P0s failing",
                    catalog, schema,
                )
                levers_rolled_back.append(lever)
                write_stage(
                    spark, run_id, f"LEVER_{lever}_STARTED", "ROLLED_BACK",
                    task_key="lever_loop", lever=lever, iteration=iteration_counter,
                    detail={"reason": "p0_gate", "p0_failures": p0_failures},
                    catalog=catalog, schema=schema,
                )
                continue

        # ── Full evaluation ───────────────────────────────────────
        write_stage(
            spark, run_id, f"LEVER_{lever}_EVAL", "STARTED",
            task_key="lever_loop", lever=lever, iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )

        new_model_id = create_genie_model_version(
            w, space_id, config, iteration_counter, domain,
            experiment_name=exp_name,
            uc_schema=uc_schema,
            patch_set=patches,
            parent_model_id=prev_model_id,
        )

        _ensure_sql_context(spark, catalog, schema)
        full_result = run_evaluation(
            space_id, exp_name, iteration_counter, benchmarks,
            domain, new_model_id, "full",
            predict_fn, scorers,
            spark=spark, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
        )

        full_scores = full_result.get("scores", {})
        full_accuracy = full_result.get("overall_accuracy", 0.0)

        write_iteration(
            spark, run_id, iteration_counter, full_result,
            catalog=catalog, schema=schema,
            lever=lever, eval_scope="full", model_id=new_model_id,
        )

        regressions = detect_regressions(
            full_scores, best_scores, threshold=REGRESSION_THRESHOLD,
        )

        if regressions:
            logger.warning("Full eval regression for lever %d: %s", lever, regressions)
            rollback(apply_log, w, space_id, metadata_snapshot)
            mark_patches_rolled_back(
                spark, run_id, iteration_counter,
                f"full_eval_regression: {regressions[0]['judge']} dropped {regressions[0]['drop']:.1f}",
                catalog, schema,
            )
            levers_rolled_back.append(lever)
            write_stage(
                spark, run_id, f"LEVER_{lever}_STARTED", "ROLLED_BACK",
                task_key="lever_loop", lever=lever, iteration=iteration_counter,
                detail={"reason": "regression", "regressions": regressions},
                catalog=catalog, schema=schema,
            )
            continue

        # ── Accept lever ──────────────────────────────────────────
        levers_accepted.append(lever)
        best_scores = full_scores
        best_accuracy = full_accuracy
        best_model_id = new_model_id
        best_iteration = iteration_counter
        prev_scores = full_scores
        prev_model_id = new_model_id

        link_eval_scores_to_model(new_model_id, full_scores)
        update_run_status(
            spark, run_id, catalog, schema,
            best_iteration=best_iteration,
            best_accuracy=best_accuracy,
            best_model_id=best_model_id,
        )

        post_instructions = _get_general_instructions(
            apply_log.get("post_snapshot", metadata_snapshot)
        )
        if post_instructions:
            register_instruction_version(
                uc_schema=uc_schema,
                space_id=space_id,
                instruction_text=post_instructions,
                run_id=run_id,
                lever=lever,
                iteration=iteration_counter,
                accuracy=best_accuracy,
                domain=domain,
            )

        write_stage(
            spark, run_id, f"LEVER_{lever}_STARTED", "COMPLETE",
            task_key="lever_loop", lever=lever, iteration=iteration_counter,
            detail={
                "accuracy": full_accuracy,
                "accepted": True,
                "patches_applied": len(apply_log.get("applied", [])),
            },
            catalog=catalog, schema=schema,
        )

        write_stage(
            spark, run_id, f"LEVER_{lever}_EVAL", "COMPLETE",
            task_key="lever_loop", lever=lever, iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )

    write_stage(
        spark, run_id, "LEVER_LOOP_STARTED", "COMPLETE",
        task_key="lever_loop",
        detail={
            "levers_attempted": levers_attempted,
            "levers_accepted": levers_accepted,
            "levers_rolled_back": levers_rolled_back,
        },
        catalog=catalog, schema=schema,
    )

    return {
        "scores": best_scores,
        "accuracy": best_accuracy,
        "model_id": best_model_id,
        "iteration_counter": iteration_counter,
        "best_iteration": best_iteration,
        "levers_attempted": levers_attempted,
        "levers_accepted": levers_accepted,
        "levers_rolled_back": levers_rolled_back,
    }


# ── Stage 4: FINALIZE ───────────────────────────────────────────────


def _run_finalize(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    domain: str,
    exp_name: str,
    prev_scores: dict[str, float],
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
    run_repeatability: bool = True,
    benchmarks: list[dict] | None = None,
    thresholds: dict[str, float] | None = None,
    finalize_timeout_seconds: int = FINALIZE_TIMEOUT_SECONDS,
    heartbeat_interval_seconds: int = FINALIZE_HEARTBEAT_SECONDS,
) -> dict:
    """Stage 4: Repeatability test, promote model, generate report.

    Adds heartbeat events + a soft timeout so long-running finalization
    remains observable and fails with an explicit terminal reason.
    """
    thresholds = thresholds or DEFAULT_THRESHOLDS
    finalize_timeout_seconds = max(1, int(finalize_timeout_seconds))
    heartbeat_interval_seconds = max(5, int(heartbeat_interval_seconds))
    started_monotonic = time.monotonic()
    last_heartbeat = 0.0
    heartbeat_count = 0
    current_phase = "initializing"

    def _elapsed_seconds() -> float:
        return time.monotonic() - started_monotonic

    def _check_timeout(phase: str) -> None:
        elapsed = _elapsed_seconds()
        if elapsed > finalize_timeout_seconds:
            raise TimeoutError(
                f"Finalize exceeded timeout ({finalize_timeout_seconds}s) "
                f"during {phase} after {elapsed:.1f}s",
            )

    def _emit_heartbeat(
        phase: str,
        *,
        detail: dict[str, Any] | None = None,
        force: bool = False,
    ) -> None:
        nonlocal last_heartbeat, heartbeat_count, current_phase
        current_phase = phase
        now = time.monotonic()
        if not force and (now - last_heartbeat) < heartbeat_interval_seconds:
            return
        last_heartbeat = now
        heartbeat_count += 1

        heartbeat_detail: dict[str, Any] = {
            "phase": phase,
            "elapsed_seconds": round(_elapsed_seconds(), 1),
            "heartbeat_count": heartbeat_count,
            "timeout_seconds": finalize_timeout_seconds,
        }
        if detail:
            heartbeat_detail.update(detail)

        try:
            # Touch run.updated_at so stale-state reconciliation doesn't mark finalize as dead.
            update_run_status(spark, run_id, catalog, schema)
            write_stage(
                spark, run_id, "FINALIZE_HEARTBEAT", "STARTED",
                task_key="finalize",
                detail=heartbeat_detail,
                catalog=catalog, schema=schema,
            )
        except Exception:
            logger.warning(
                "Failed to persist finalize heartbeat for run %s",
                run_id,
                exc_info=True,
            )

    write_stage(
        spark, run_id, "FINALIZE_STARTED", "STARTED",
        task_key="finalize",
        detail={
            "timeout_seconds": finalize_timeout_seconds,
            "heartbeat_interval_seconds": heartbeat_interval_seconds,
        },
        catalog=catalog, schema=schema,
    )
    _emit_heartbeat("finalize_started", force=True)

    terminal_reason = ""
    repeatability_pct = 0.0
    try:
        _check_timeout("pre_repeatability")
        if run_repeatability and benchmarks:
            write_stage(
                spark, run_id, "REPEATABILITY_TEST", "STARTED",
                task_key="finalize", catalog=catalog, schema=schema,
            )
            _emit_heartbeat(
                "repeatability_test",
                force=True,
                detail={"benchmark_count": len(benchmarks)},
            )

            def _repeatability_progress(progress: dict[str, Any]) -> None:
                _check_timeout("repeatability_test")
                _emit_heartbeat("repeatability_test", detail=progress)

            try:
                rep_result = run_repeatability_test(
                    w,
                    space_id,
                    benchmarks,
                    spark,
                    heartbeat_cb=_repeatability_progress,
                )
                _check_timeout("post_repeatability")
                repeatability_pct = rep_result.get("average_repeatability_pct", 0.0)
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "COMPLETE",
                    task_key="finalize",
                    detail={
                        "average_pct": repeatability_pct,
                        "total_questions": rep_result.get("total_questions"),
                    },
                    catalog=catalog, schema=schema,
                )
            except TimeoutError as exc:
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "FAILED",
                    task_key="finalize",
                    detail={"terminal_reason": "finalize_timeout"},
                    error_message=str(exc)[:500],
                    catalog=catalog, schema=schema,
                )
                raise
            except Exception:
                logger.exception("Repeatability test failed")
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "FAILED",
                    task_key="finalize",
                    error_message="Repeatability test exception",
                    catalog=catalog, schema=schema,
                )
        else:
            _emit_heartbeat(
                "repeatability_skipped",
                force=True,
                detail={"reason": "disabled_or_no_benchmarks"},
            )

        _check_timeout("promote_best_model")
        _emit_heartbeat("promote_best_model", force=True)
        promoted_model = promote_best_model(spark, run_id, catalog, schema)

        _check_timeout("generate_report")
        _emit_heartbeat("generate_report", force=True)
        report_path = generate_report(spark, run_id, domain, catalog, schema)

        _check_timeout("resolve_terminal_status")
        converged = all_thresholds_met(prev_scores, thresholds)
        if converged:
            status = "CONVERGED"
            reason = "threshold_met"
        elif iteration_counter >= MAX_ITERATIONS:
            status = "MAX_ITERATIONS"
            reason = "max_iterations"
        else:
            status = "STALLED"
            reason = "no_further_improvement"

        terminal_reason = f"finalize_completed:{reason}"
        update_run_status(
            spark, run_id, catalog, schema,
            status=status,
            convergence_reason=reason,
            best_repeatability=repeatability_pct,
        )

        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "COMPLETE",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "status": status,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "COMPLETE",
            task_key="finalize",
            detail={
                "status": status,
                "report_path": report_path,
                "promoted_model": promoted_model,
                "repeatability_pct": repeatability_pct,
                "terminal_reason": terminal_reason,
                "heartbeat_count": heartbeat_count,
            },
            catalog=catalog, schema=schema,
        )

        return {
            "status": status,
            "convergence_reason": reason,
            "repeatability_pct": repeatability_pct,
            "report_path": report_path,
            "promoted_model": promoted_model,
            "terminal_reason": terminal_reason,
            "elapsed_seconds": round(_elapsed_seconds(), 1),
            "heartbeat_count": heartbeat_count,
        }

    except TimeoutError as exc:
        terminal_reason = "finalize_timeout"
        logger.exception("Finalize timeout for run %s", run_id)
        update_run_status(
            spark, run_id, catalog, schema,
            status="FAILED",
            convergence_reason=terminal_reason,
            best_repeatability=repeatability_pct,
        )
        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "FAILED",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "phase": current_phase,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            error_message=str(exc)[:500],
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "FAILED",
            task_key="finalize",
            detail={"terminal_reason": terminal_reason, "phase": current_phase},
            error_message=str(exc)[:500],
            catalog=catalog, schema=schema,
        )
        raise
    except Exception as exc:
        terminal_reason = "finalize_error"
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("Finalize failure for run %s", run_id)
        update_run_status(
            spark, run_id, catalog, schema,
            status="FAILED",
            convergence_reason=terminal_reason,
            best_repeatability=repeatability_pct,
        )
        write_stage(
            spark, run_id, "FINALIZE_TERMINAL", "FAILED",
            task_key="finalize",
            detail={
                "terminal_reason": terminal_reason,
                "phase": current_phase,
                "elapsed_seconds": round(_elapsed_seconds(), 1),
                "heartbeat_count": heartbeat_count,
            },
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "FINALIZE_STARTED", "FAILED",
            task_key="finalize",
            detail={"terminal_reason": terminal_reason, "phase": current_phase},
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        raise


# ── Stage 5: DEPLOY ─────────────────────────────────────────────────


def _run_deploy(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    deploy_target: str | None,
    space_id: str,
    exp_name: str,
    domain: str,
    prev_model_id: str,
    iteration_counter: int,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 5: Deploy via DABs, held-out evaluation (optional)."""
    if not deploy_target:
        write_stage(
            spark, run_id, "DEPLOY_SKIPPED", "SKIPPED",
            task_key="deploy",
            detail={"reason": "no_deploy_target"},
            catalog=catalog, schema=schema,
        )
        return {"status": "SKIPPED", "reason": "no_deploy_target"}

    write_stage(
        spark, run_id, "DEPLOY_STARTED", "STARTED",
        task_key="deploy", catalog=catalog, schema=schema,
    )

    try:
        logger.info("Deploy target: %s (placeholder — DABs integration pending)", deploy_target)

        write_stage(
            spark, run_id, "DEPLOY_STARTED", "COMPLETE",
            task_key="deploy",
            detail={"deploy_target": deploy_target},
            catalog=catalog, schema=schema,
        )
        return {"status": "DEPLOYED", "deploy_target": deploy_target}

    except Exception as exc:
        write_stage(
            spark, run_id, "DEPLOY_STARTED", "FAILED",
            task_key="deploy",
            error_message=str(exc)[:500],
            catalog=catalog, schema=schema,
        )
        return {"status": "FAILED", "error": str(exc)}


# ── Resume Helper ────────────────────────────────────────────────────


def _resume_lever_loop(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> dict:
    """Read Delta to find last completed lever for resume after task retry.

    Returns: resume_from_lever, iteration_counter, prev_scores, prev_model_id.
    """
    latest_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not latest_iter:
        return {"resume_from_lever": 0, "iteration_counter": 0}

    stages_df = load_stages(spark, run_id, catalog, schema)
    last_lever = 0
    if not stages_df.empty:
        lever_stages = stages_df[
            stages_df["stage"].str.startswith("LEVER_")
            & (stages_df["status"] == "COMPLETE")
        ]
        if not lever_stages.empty:
            lever_nums = lever_stages["lever"].dropna().astype(int)
            if not lever_nums.empty:
                last_lever = int(lever_nums.max())

    scores_json = latest_iter.get("scores_json", {})
    if isinstance(scores_json, str):
        try:
            scores_json = json.loads(scores_json)
        except (json.JSONDecodeError, TypeError):
            scores_json = {}

    return {
        "resume_from_lever": last_lever,
        "iteration_counter": int(latest_iter.get("iteration", 0)),
        "prev_scores": scores_json if isinstance(scores_json, dict) else {},
        "prev_model_id": latest_iter.get("model_id", ""),
        "prev_accuracy": float(latest_iter.get("overall_accuracy", 0.0)),
    }


# ── Evaluation via Job (for multi-task architecture) ─────────────────


def run_evaluation_via_job(
    w: WorkspaceClient,
    space_id: str,
    experiment_name: str,
    iteration: int,
    domain: str,
    model_id: str,
    eval_scope: str,
    **kwargs: Any,
) -> dict:
    """Submit evaluation as a Databricks Job run and poll for results.

    Uses ``w.jobs.submit_run()`` with Serverless compute. Benchmarks
    are loaded from the MLflow evaluation dataset.

    This is the job-based alternative to inline ``run_evaluation()``.
    """
    from genie_space_optimizer.common.config import JOB_MAX_WAIT, JOB_POLL_INTERVAL

    notebook_path = kwargs.get(
        "notebook_path", "/Workspace/genie_space_optimizer/jobs/run_evaluation_only",
    )

    task_params = {
        "space_id": space_id,
        "experiment_name": experiment_name,
        "iteration": str(iteration),
        "domain": domain,
        "model_id": model_id or "",
        "eval_scope": eval_scope,
    }

    try:
        run = w.jobs.submit(
            run_name=f"genie_eval_{space_id}_{iteration}",
            tasks=cast(Any, [
                {
                    "task_key": "evaluation",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": task_params,
                    },
                    "new_cluster": {"spark_version": "auto", "num_workers": 0},
                }
            ]),
        )
        run_id = run.run_id
        logger.info("Submitted evaluation job: run_id=%s", run_id)

        elapsed = 0
        while elapsed < JOB_MAX_WAIT:
            time.sleep(JOB_POLL_INTERVAL)
            elapsed += JOB_POLL_INTERVAL
            status = w.jobs.get_run(run_id)
            state = str(status.state.life_cycle_state) if status.state else "UNKNOWN"
            if state in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                break

        return {
            "job_run_id": str(run_id),
            "status": state,
        }

    except Exception as exc:
        logger.exception("Evaluation job submission failed")
        return {"status": "FAILED", "error": str(exc)}


# ── Convenience Function ─────────────────────────────────────────────


def optimize_genie_space(
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    *,
    run_id: str | None = None,
    apply_mode: str = APPLY_MODE,
    experiment_name: str | None = None,
    levers: list[int] | None = None,
    max_iterations: int = MAX_ITERATIONS,
    thresholds: dict[str, float] | None = None,
    deploy_target: str | None = None,
    run_repeat: bool = True,
    triggered_by: str | None = None,
) -> OptimizationResult:
    """Run all 5 stages in a single process.

    When *run_id* is supplied the caller has already created the Delta row
    (e.g. the backend ``start_optimization`` endpoint), so we skip
    ``create_run`` to avoid duplicating the row.
    """
    w = WorkspaceClient()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    _run_created_here = run_id is None
    if _run_created_here:
        run_id = str(uuid.uuid4())
    assert run_id is not None
    run_id_str = run_id

    ensure_optimization_tables(spark, catalog, schema)

    if _run_created_here:
        create_run(
            spark, run_id_str, space_id, domain, catalog, schema,
            max_iterations=max_iterations,
            levers=levers,
            apply_mode=apply_mode,
            deploy_target=deploy_target,
            triggered_by=triggered_by,
        )

    result = OptimizationResult(
        run_id=run_id_str,
        space_id=space_id,
        domain=domain,
        status="FAILED",
        best_iteration=0,
        best_accuracy=0.0,
        best_repeatability=0.0,
        best_model_id=None,
        convergence_reason=None,
        total_iterations=0,
    )

    try:
        # Stage 1: Preflight
        preflight_out = _run_preflight(
            w, spark, run_id_str, space_id, catalog, schema, domain, experiment_name,
        )
        config = cast(dict[str, Any], preflight_out["config"])
        benchmarks = cast(list[dict], preflight_out["benchmarks"])
        model_id = str(preflight_out["model_id"])
        exp_name = str(preflight_out["experiment_name"])
        result.experiment_name = exp_name
        result.experiment_id = str(preflight_out.get("experiment_id", ""))

        # Stage 2: Baseline
        baseline_out = _run_baseline(
            w, spark, run_id_str, space_id, benchmarks, exp_name,
            model_id, catalog, schema, domain,
        )
        prev_scores = cast(dict[str, float], baseline_out["scores"])
        prev_accuracy = float(baseline_out["overall_accuracy"])
        thresholds_met = bool(baseline_out["thresholds_met"])

        if thresholds_met:
            result.status = "CONVERGED"
            result.convergence_reason = "baseline_meets_thresholds"
            result.best_accuracy = prev_accuracy
            result.best_model_id = model_id
            result.final_scores = prev_scores
            update_run_status(
                spark, run_id_str, catalog, schema,
                status="CONVERGED",
                convergence_reason="baseline_meets_thresholds",
            )
        else:
            # Stage 3: Lever Loop
            loop_out = _run_lever_loop(
                w, spark, run_id_str, space_id, domain, benchmarks, exp_name,
                prev_scores, prev_accuracy, model_id, config,
                catalog, schema, levers, max_iterations, thresholds, apply_mode,
            )
            result.levers_attempted = cast(list[int], loop_out["levers_attempted"])
            result.levers_accepted = cast(list[int], loop_out["levers_accepted"])
            result.levers_rolled_back = cast(list[int], loop_out["levers_rolled_back"])
            result.total_iterations = int(loop_out["iteration_counter"])
            result.best_accuracy = float(loop_out["accuracy"])
            result.best_model_id = str(loop_out["model_id"])
            result.best_iteration = int(loop_out["best_iteration"])
            result.final_scores = cast(dict[str, float], loop_out["scores"])

            prev_scores = cast(dict[str, float], loop_out["scores"])
            prev_model_id = str(loop_out["model_id"])

            # Stage 4: Finalize
            finalize_out = _run_finalize(
                w, spark, run_id_str, space_id, domain, exp_name,
                prev_scores, prev_model_id, int(loop_out["iteration_counter"]),
                catalog, schema, run_repeat, benchmarks, thresholds,
            )
            result.status = str(finalize_out["status"])
            result.convergence_reason = str(finalize_out["convergence_reason"])
            result.best_repeatability = float(finalize_out["repeatability_pct"])
            result.report_path = str(finalize_out["report_path"])

        # Stage 5: Deploy
        _run_deploy(
            w, spark, run_id_str, deploy_target, space_id, exp_name,
            domain, result.best_model_id or "", result.total_iterations,
            catalog, schema,
        )

    except Exception as exc:
        result.status = "FAILED"
        result.error = traceback.format_exc()
        logger.exception("optimize_genie_space failed for run %s", run_id_str)

    return result


# ── Private Helpers ──────────────────────────────────────────────────


def _build_patch_record(entry: dict, lever: int, apply_mode: str) -> dict:
    """Build a patch record dict for Delta from an apply_log entry."""
    patch = entry.get("patch", {})
    action = entry.get("action", {})
    return {
        "patch_type": patch.get("type", action.get("action_type", "unknown")),
        "scope": apply_mode if lever <= 3 else "genie_config",
        "risk_level": action.get("risk_level", "medium"),
        "target_object": action.get("target", patch.get("target", "")),
        "patch": patch,
        "command": action.get("command"),
        "rollback": action.get("rollback_command"),
        "proposal_id": patch.get("source_proposal_id", ""),
    }


def _get_failure_rows(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Load the latest iteration's per-question rows for failure clustering."""
    latest = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not latest:
        return []
    rows_json = latest.get("rows_json")
    if isinstance(rows_json, str):
        try:
            return json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(rows_json, list):
        return rows_json
    return []
