"""
Optimization Harness — stage functions for the 5-task Databricks Job.

The canonical execution path is the **5-task DAG** launched via
``submit_optimization()`` in ``job_launcher.py``.  Each DAG notebook is a
thin wrapper that deserializes task values, calls a single harness function,
and publishes outputs.

Each ``_run_*`` / ``_prepare_*`` function encapsulates all business logic
for its stage so that both the DAG notebooks and the ``optimize_genie_space()``
convenience function (used for dev/test only) share identical code paths.

Architecture: ``preflight`` → ``baseline_eval`` → ``prepare_lever_loop`` +
``lever_loop`` → ``finalize`` → ``deploy``.  Inter-task data flows via
``dbutils.jobs.taskValues``.  Detailed state goes to Delta.
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

from collections import Counter

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    ARBITER_CORRECTION_TRIGGER,
    DEFAULT_LEVER_ORDER,
    DEFAULT_THRESHOLDS,
    ENABLE_PROMPT_MATCHING_AUTO_APPLY,
    INLINE_EVAL_DELAY,
    LEVER_NAMES,
    MAX_ITERATIONS,
    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS,
    PROPAGATION_WAIT_SECONDS,
    REGRESSION_THRESHOLD,
    SLICE_GATE_TOLERANCE,
)
from genie_space_optimizer.optimization.applier import (
    _get_general_instructions,
    apply_patch_set,
    auto_apply_prompt_matching,
    proposals_to_patches,
    rollback,
)
from genie_space_optimizer.optimization.evaluation import (
    all_thresholds_met,
    extract_reference_sqls,
    filter_benchmarks_by_scope,
    log_asi_feedback_on_traces,
    log_gate_feedback_on_traces,
    make_predict_fn,
    normalize_scores,
    register_instruction_version,
    run_evaluation,
    run_repeatability_evaluation,
)
from genie_space_optimizer.optimization.models import (
    create_genie_model_version,
    link_eval_scores_to_model,
    promote_best_model,
)
from genie_space_optimizer.optimization.optimizer import (
    _enrich_blank_descriptions,
    _generate_holistic_strategy,
    cluster_failures,
    detect_regressions,
    enrich_metadata_with_uc_types,
    generate_metadata_proposals,
    generate_proposals_from_strategy,
)
from genie_space_optimizer.optimization.preflight import run_preflight
from genie_space_optimizer.optimization.repeatability import run_repeatability_test
from genie_space_optimizer.optimization.report import generate_report
from genie_space_optimizer.optimization.scorers import make_all_scorers
from genie_space_optimizer.optimization.state import (
    create_run,
    ensure_optimization_tables,
    load_latest_full_iteration,
    load_run,
    load_stages,
    mark_patches_rolled_back,
    update_provenance_gate,
    update_provenance_proposals,
    update_run_status,
    write_asi_results,
    write_iteration,
    write_patch,
    write_provenance,
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


_W = 78

def _section(title: str, char: str = "=") -> str:
    pad = max(0, _W - len(title) - 4)
    return f"\n{char * 2} {title} {char * pad}"


def _kv(key: str, value: object, indent: int = 2) -> str:
    return f"{' ' * indent}{'| ' if indent == 0 else '|  '}{key + ':':<28s} {value}"


def _bar(char: str = "-") -> str:
    return char * _W


def _scorecard(scores: dict[str, float], prefix: str = "|  ") -> str:
    parts = [f"{j}={v:.1f}" for j, v in sorted(scores.items())]
    line = "  ".join(parts)
    return f"{prefix}Scores:  {line}"


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


# ── Stage 2.5: PROMPT MATCHING AUTO-CONFIG ──────────────────────────


def _run_prompt_matching_setup(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.5: Enable format assistance and entity matching as best practice.

    Runs between baseline eval and lever loop.  Deterministic (no LLM).
    Returns summary dict with counts of changes applied.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config

    write_stage(
        spark, run_id, "PROMPT_MATCHING_SETUP", "STARTED",
        task_key="prompt_matching_setup", catalog=catalog, schema=schema,
    )

    try:
        _parsed = config.get("_parsed_space", {})
        _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
        _tbl_count = len(_ds.get("tables", []))
        _mv_count = len(_ds.get("metric_views", []))
        print(
            f"\n[PROMPT MATCHING] Starting auto-config — "
            f"tables: {_tbl_count}, metric_views: {_mv_count}, "
            f"total data sources: {_tbl_count + _mv_count}"
        )

        apply_log = auto_apply_prompt_matching(w, space_id, config)

        applied = apply_log.get("applied", [])
        fa_count = apply_log.get("format_assistance_count", 0)
        em_count = apply_log.get("entity_matching_count", 0)

        for idx, entry in enumerate(applied):
            write_patch(
                spark, run_id, 0, 0, idx,
                {
                    "patch_type": entry.get("type", "unknown"),
                    "scope": "genie_config",
                    "risk_level": "low",
                    "target_object": f"{entry.get('table', '')}.{entry.get('column', '')}",
                    "patch": entry,
                    "command": None,
                    "rollback": None,
                    "proposal_id": "prompt_matching_auto_config",
                },
                catalog, schema,
            )

        if applied:
            refreshed = fetch_space_config(w, space_id)
            config["_parsed_space"] = refreshed.get("_parsed_space", refreshed)

        print(
            f"\n-- PROMPT MATCHING APPLIED " + "-" * 26 + "\n"
            f"  Total changes: {len(applied)}\n"
            f"    Format assistance (enable_format_assistance): {fa_count} columns\n"
            f"    Entity matching (enable_entity_matching): {em_count} columns\n"
            f"  Tables patched: {apply_log.get('patched_objects', [])}\n"
            f"  Genie Space API PATCH sent: {'YES' if applied else 'NO'}\n"
            f"  Config refreshed: {'YES' if applied else 'N/A'}\n"
            + "-" * 52
        )

        write_stage(
            spark, run_id, "PROMPT_MATCHING_SETUP", "COMPLETE",
            task_key="prompt_matching_setup",
            detail={
                "format_assistance_enabled": fa_count,
                "entity_matching_enabled": em_count,
                "total_changes": len(applied),
                "patched_objects": apply_log.get("patched_objects", []),
            },
            catalog=catalog, schema=schema,
        )

        return {
            "format_assistance_count": fa_count,
            "entity_matching_count": em_count,
            "total_changes": len(applied),
        }

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("PROMPT_MATCHING_SETUP FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "PROMPT_MATCHING_SETUP", "FAILED",
            task_key="prompt_matching_setup",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return {"format_assistance_count": 0, "entity_matching_count": 0, "total_changes": 0}


# ── Stage 2.75: PROACTIVE DESCRIPTION ENRICHMENT ───────────────────


def _run_description_enrichment(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    config: dict,
    metadata_snapshot: dict,
    catalog: str,
    schema: str,
) -> dict:
    """Stage 2.75: Generate structured descriptions for blank columns.

    Runs after UC type enrichment and before the strategist.  Targets only
    columns that have NO description in both the Genie Space and Unity Catalog.
    Applies patches via update_sections with lever=0 (pre-optimization).

    Returns summary dict with ``total_eligible``, ``total_enriched``, and
    ``total_skipped``.
    """
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        patch_space_config,
    )
    from genie_space_optimizer.optimization.structured_metadata import (
        entity_type_for_column,
        update_sections,
    )

    write_stage(
        spark, run_id, "DESCRIPTION_ENRICHMENT", "STARTED",
        task_key="description_enrichment", catalog=catalog, schema=schema,
    )

    result = {"total_eligible": 0, "total_enriched": 0, "total_skipped": 0}

    try:
        patches = _enrich_blank_descriptions(metadata_snapshot, w)
        result["total_eligible"] = len(patches)

        if not patches:
            print(
                f"\n-- DESCRIPTION ENRICHMENT " + "-" * 27 + "\n"
                f"  Eligible columns: 0\n"
                f"  No columns need proactive descriptions.\n"
                + "-" * 52
            )
            write_stage(
                spark, run_id, "DESCRIPTION_ENRICHMENT", "COMPLETE",
                task_key="description_enrichment",
                detail=result, catalog=catalog, schema=schema,
            )
            return result

        ds = metadata_snapshot.get("data_sources", {})
        if not isinstance(ds, dict):
            ds = {}
        tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
        mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
        all_objects = list(tables) + list(mvs)
        tbl_lookup: dict[str, dict] = {}
        for tbl in all_objects:
            if isinstance(tbl, dict):
                ident = tbl.get("identifier", "") or tbl.get("name", "")
                tbl_lookup[ident] = tbl

        enriched = 0
        skipped = 0

        for patch in patches:
            tbl_id = patch["table"]
            col_name = patch["column"]
            sections = patch.get("structured_sections", {})
            etype = patch.get("column_entity_type", "")

            tbl = tbl_lookup.get(tbl_id)
            if not tbl:
                logger.warning("Description enrichment: table %s not found — skipping", tbl_id)
                skipped += 1
                continue

            cols = tbl.get("column_configs", tbl.get("columns", []))
            cc = None
            for c in cols:
                if isinstance(c, dict) and (c.get("column_name", c.get("name", "")) == col_name):
                    cc = c
                    break

            if cc is None:
                logger.warning(
                    "Description enrichment: column %s.%s not found — skipping", tbl_id, col_name,
                )
                skipped += 1
                continue

            if not etype:
                data_type = cc.get("data_type", "")
                etype = entity_type_for_column(col_name, data_type)

            try:
                new_desc = update_sections(
                    cc.get("description"),
                    sections,
                    lever=0,
                    entity_type=etype,
                )
                cc["description"] = new_desc
                enriched += 1
            except Exception:
                logger.warning(
                    "Description enrichment: failed to apply sections for %s.%s",
                    tbl_id, col_name, exc_info=True,
                )
                skipped += 1

        result["total_enriched"] = enriched
        result["total_skipped"] = skipped

        if enriched > 0:
            parsed = config.get("_parsed_space", config)
            patch_space_config(w, space_id, parsed)

            for idx, patch in enumerate(patches[:enriched]):
                write_patch(
                    spark, run_id, 0, 0, idx,
                    {
                        "patch_type": "proactive_description_enrichment",
                        "scope": "genie_config",
                        "risk_level": "low",
                        "target_object": f"{patch['table']}.{patch['column']}",
                        "patch": patch,
                        "command": None,
                        "rollback": None,
                        "proposal_id": "description_enrichment",
                    },
                    catalog, schema,
                )

        print(
            f"\n-- DESCRIPTION ENRICHMENT " + "-" * 27 + "\n"
            f"  Eligible columns: {len(patches)}\n"
            f"  Enriched: {enriched}\n"
            f"  Skipped: {skipped}\n"
            f"  Genie Space API PATCH sent: {'YES' if enriched > 0 else 'NO'}\n"
            + "-" * 52
        )

        write_stage(
            spark, run_id, "DESCRIPTION_ENRICHMENT", "COMPLETE",
            task_key="description_enrichment",
            detail=result, catalog=catalog, schema=schema,
        )

        return result

    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("DESCRIPTION_ENRICHMENT FAILED for run %s", run_id)
        write_stage(
            spark, run_id, "DESCRIPTION_ENRICHMENT", "FAILED",
            task_key="description_enrichment",
            error_message=err_msg[:500],
            catalog=catalog, schema=schema,
        )
        return result


# ── Stage 2.5b: PREPARE LEVER LOOP ──────────────────────────────────


def _prepare_lever_loop(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
) -> dict:
    """Load Genie Space config, enrich UC metadata, run Stage 2.5 prompt matching.

    Consolidates all config preparation needed before the lever loop into a
    single function used by both the DAG notebook and the convenience wrapper.

    Steps:
      1. Load config from Delta snapshot (API fetch fallback).
      2. Fetch UC column metadata via REST API (for prompt matching + type
         enrichment in the lever loop).
      3. Print detailed inventory diagnostic (tables, cols, FA/VD stats).
      4. Run Stage 2.5 prompt matching auto-config (if enabled).
      5. If changes applied: entity-matching-aware propagation wait + diagnostic.
      6. Post-wait config refresh from API.
      7. If no changes or prompt matching disabled: skip wait entirely.

    Non-fatal: exceptions in UC fetch or prompt matching are logged and
    swallowed so the lever loop can still proceed.

    Returns the fully prepared config dict with ``_uc_columns`` populated.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.common.uc_metadata import (
        extract_genie_space_table_refs,
        get_columns_for_tables_rest,
    )

    # ── 1. Load config from Delta snapshot (API fetch fallback) ──────
    run_data = load_run(spark, run_id, catalog, schema) or {}
    snapshot = run_data.get("config_snapshot", {})
    config: dict
    if isinstance(snapshot, dict) and snapshot:
        config = snapshot
        logger.info("Lever loop: using config snapshot from run row for %s", run_id)
    else:
        logger.warning(
            "No config snapshot found in run row for %s — fetching from API.",
            run_id,
        )
        config = fetch_space_config(w, space_id)
        logger.info("Lever loop: fetched config for space %s", space_id)

    logger.info(
        "Lever loop config loaded: keys=%s",
        sorted(list(config.keys()))[:20] if isinstance(config, dict) else [],
    )

    # ── 2. Fetch UC column metadata via REST API ─────────────────────
    table_refs: list = []
    try:
        table_refs = extract_genie_space_table_refs(config)
        uc_columns = get_columns_for_tables_rest(w, table_refs) if table_refs else []
        config["_uc_columns"] = uc_columns
        logger.info(
            "Lever loop: fetched %d UC columns across %d tables",
            len(uc_columns), len(table_refs),
        )
    except Exception as exc:
        logger.warning(
            "UC column metadata fetch failed for %s (non-fatal): %s",
            run_id, exc,
        )
        uc_columns = config.get("_uc_columns", [])

    # ── 3. Print detailed inventory diagnostic ───────────────────────
    _parsed = config.get("_parsed_space", {})
    _ds = _parsed.get("data_sources", {}) if isinstance(_parsed, dict) else {}
    _tables = _ds.get("tables", []) + _ds.get("metric_views", [])
    _total_cols = sum(len(t.get("column_configs", [])) for t in _tables)
    _visible_cols = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if not c.get("hidden")
    )
    _hidden_cols = _total_cols - _visible_cols
    _string_cols = sum(
        1 for c in uc_columns
        if str(c.get("data_type", "")).upper() == "STRING"
    )
    _fa_existing = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if c.get("enable_format_assistance")
    )
    _vd_existing = sum(
        1 for t in _tables for c in t.get("column_configs", [])
        if c.get("enable_entity_matching")
    )
    print(
        f"\n{'=' * 62}\n"
        f"  PREPARE LEVER LOOP — GENIE SPACE INVENTORY\n"
        f"{'=' * 62}\n"
        f"\n-- GENIE SPACE INVENTORY " + "-" * 27 + "\n"
        f"  Tables: {len(_tables)}"
        f" ({', '.join(t.get('name', t.get('identifier', '?')) for t in _tables[:10])})\n"
        f"  Total columns: {_total_cols}"
        f" (visible: {_visible_cols}, hidden: {_hidden_cols})\n"
        f"  UC column metadata: {len(uc_columns)} columns"
        f" fetched across {len(table_refs)} tables\n"
        f"  STRING columns eligible for entity matching: {_string_cols}\n"
        f"  Columns already with format assistance: {_fa_existing}\n"
        f"  Columns already with value dictionary: {_vd_existing} of 120 max slots\n"
        + "-" * 52
    )

    # ── 4–7. Stage 2.5 prompt matching + propagation wait ────────────
    if ENABLE_PROMPT_MATCHING_AUTO_APPLY:
        try:
            pm_result = _run_prompt_matching_setup(
                w, spark, run_id, space_id, config, catalog, schema,
            )
            logger.info(
                "Prompt matching complete: FA=%d, EM=%d, total=%d",
                pm_result.get("format_assistance_count", 0),
                pm_result.get("entity_matching_count", 0),
                pm_result.get("total_changes", 0),
            )

            if pm_result.get("total_changes", 0) > 0:
                has_entity_matching = pm_result.get("entity_matching_count", 0) > 0
                wait_time = (
                    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS if has_entity_matching
                    else PROPAGATION_WAIT_SECONDS
                )
                print(
                    f"\n-- PROPAGATION WAIT " + "-" * 32 + "\n"
                    f"  Changes applied: {pm_result.get('total_changes', 0)}\n"
                    f"  Entity matching changes:"
                    f" {pm_result.get('entity_matching_count', 0)}\n"
                    f"  Wait time: {wait_time}s"
                    + (
                        " (extended for value dictionary rebuild)"
                        if has_entity_matching else ""
                    )
                    + "\n" + "-" * 52
                )
                time.sleep(wait_time)
                config = fetch_space_config(w, space_id)
                config["_uc_columns"] = uc_columns
                logger.info("Config refreshed after prompt matching propagation wait")
            else:
                logger.info("Prompt matching: no changes applied, skipping wait")
        except Exception as exc:
            logger.warning(
                "Stage 2.5 prompt matching failed (non-fatal, continuing): %s: %s",
                type(exc).__name__, exc,
            )

    return config


# ── Stage 3: LEVER LOOP ─────────────────────────────────────────────


def _extract_arbiter_actions_from_baseline(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Extract genie_correct arbiter actions from baseline iteration rows."""
    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    if not baseline_iter:
        return []

    rows_json = baseline_iter.get("rows_json")
    if isinstance(rows_json, str):
        try:
            rows_json = json.loads(rows_json)
        except (json.JSONDecodeError, TypeError):
            return []
    if not isinstance(rows_json, list):
        return []

    actions: list[dict] = []
    for row in rows_json:
        av = str(
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
            or "skipped"
        ).lower()
        if av != "genie_correct":
            continue
        genie_sql = (
            row.get("outputs/response")
            or (row.get("outputs") or {}).get("response", "")
        )
        question = (
            row.get("inputs/question")
            or (row.get("inputs") or {}).get("question", "")
        )
        if genie_sql and question:
            actions.append({
                "question": str(question),
                "new_expected_sql": str(genie_sql),
                "verdict": "genie_correct",
            })
    return actions


def _analyze_and_distribute(
    spark: Any,
    run_id: str,
    catalog: str,
    schema: str,
    metadata_snapshot: dict,
    iteration_counter: int,
    lever_label: int,
    *,
    verbose: bool = True,
) -> dict:
    """Analyze failures once, cluster, and distribute clusters to levers.

    Returns a dict with:
      - ``lever_assignments``: ``{lever_int: [clusters]}``
      - ``all_clusters``: all hard-failure clusters (flat list)
      - ``soft_signal_clusters``: soft-signal clusters
      - ``summary``: printable summary lines
      - ``asi_rows``: ASI rows for Delta
      - ``prov_rows``: provenance rows for Delta
    """
    from genie_space_optimizer.optimization.optimizer import (
        _map_to_lever,
        cluster_failures,
    )

    failure_rows = _get_failure_rows(spark, run_id, catalog, schema)

    _NON_ACTIONABLE_VERDICTS = {"genie_correct", "both_correct"}
    arbiter_counts: dict[str, int] = {}
    arbiter_excluded: list[str] = []
    soft_signal_qids: list[str] = []
    filtered_failure_rows: list[dict] = []
    soft_signal_rows: list[dict] = []
    for row in failure_rows:
        av = str(
            row.get("arbiter/value")
            or row.get("feedback/arbiter/value")
            or (row.get("arbiter") if isinstance(row.get("arbiter"), str) else "")
            or "skipped"
        ).lower()
        arbiter_counts[av] = arbiter_counts.get(av, 0) + 1
        if av in _NON_ACTIONABLE_VERDICTS:
            _rq = row.get("request") or {}
            if isinstance(_rq, str):
                try:
                    _rq = json.loads(_rq)
                except (json.JSONDecodeError, TypeError):
                    _rq = {}
            _rqk = _rq.get("kwargs", {}) if isinstance(_rq, dict) else {}
            qid = str(
                row.get("inputs/question_id")
                or (row.get("inputs") or {}).get("question_id", "")
                or row.get("question_id")
                or _rqk.get("question_id")
                or (_rq.get("question_id") if isinstance(_rq, dict) else None)
                or "?"
            )
            if _has_individual_judge_failure(row):
                soft_signal_rows.append(row)
                soft_signal_qids.append(qid)
            else:
                arbiter_excluded.append(qid)
        else:
            filtered_failure_rows.append(row)

    # ── Print failure analysis summary ─────────────────────────────
    _arbiter_summary = "  ".join(f"{k}={v}" for k, v in sorted(arbiter_counts.items()))
    _fa_lines = [
        _section("Failure Analysis", "-"),
        _kv("Total rows loaded", len(failure_rows)),
        _kv("Arbiter verdicts", _arbiter_summary),
    ]
    if arbiter_excluded:
        _fa_lines.append(_kv("Excluded (fully correct)", f"{len(arbiter_excluded)} question(s)"))
    if soft_signal_rows:
        _fa_lines.append(_kv("Soft signals (correct but judges failed)", f"{len(soft_signal_rows)} question(s)"))
        _fa_lines.append(_kv("  Soft signal question IDs", ", ".join(soft_signal_qids[:10])))
        for _ss_row, _ss_qid in zip(soft_signal_rows[:10], soft_signal_qids[:10]):
            _failed_judges = _get_failed_judges(_ss_row)
            _fa_lines.append(f"  |    {_ss_qid}: failed judges = {', '.join(_failed_judges) if _failed_judges else '(none detected)'}")
    _fa_lines.append(_kv("Hard failure rows for clustering", len(filtered_failure_rows)))
    _fa_lines.append(_bar("-"))
    print("\n".join(_fa_lines))

    # ── Cluster hard failures ──────────────────────────────────────
    eval_result_for_clustering = {"rows": filtered_failure_rows}
    clusters = cluster_failures(
        eval_result_for_clustering, metadata_snapshot,
        spark=spark, run_id=run_id, catalog=catalog, schema=schema,
    )

    # ── Cluster soft signals ───────────────────────────────────────
    soft_clusters: list[dict] = []
    if soft_signal_rows:
        soft_eval = {"rows": soft_signal_rows}
        soft_clusters = cluster_failures(
            soft_eval, metadata_snapshot,
            spark=spark, run_id=run_id, catalog=catalog, schema=schema,
            verbose=False,
        )
        for sc in soft_clusters:
            sc["signal_type"] = "soft"
        _soft_qids_total = sum(len(sc.get("question_ids", [])) for sc in soft_clusters)
        _soft_lines = [
            _section("Soft Signal Clusters (correct-but-suboptimal)", "-"),
            _kv("Soft signal rows", len(soft_signal_rows)),
            _kv("Soft clusters formed", len(soft_clusters)),
            _kv("Soft cluster questions", _soft_qids_total),
            "|",
        ]
        for si, sc in enumerate(soft_clusters, 1):
            sc_judge = sc.get("affected_judge", "?")
            sc_cause = sc.get("root_cause", "?")
            sc_asi = sc.get("asi_failure_type", "n/a")
            sc_qids = sc.get("question_ids", [])
            sc_blame = sc.get("asi_blame_set", sc.get("blame_set", []))
            blame_str = ", ".join(sc_blame) if isinstance(sc_blame, list) and sc_blame else str(sc_blame) if sc_blame else "(none)"
            _soft_lines.append(f"|  Soft cluster {si} / {len(soft_clusters)}")
            _soft_lines.append(f"|    {'Judge:':<24s} {sc_judge}")
            _soft_lines.append(f"|    {'Root cause:':<24s} {sc_cause}")
            _soft_lines.append(f"|    {'ASI failure type:':<24s} {sc_asi}")
            _soft_lines.append(f"|    {'Blame:':<24s} {blame_str}")
            _soft_lines.append(f"|    Questions ({len(sc_qids)}):")
            for qid in sc_qids:
                _soft_lines.append(f"|      {qid}")
            _soft_lines.append("|")
        _soft_lines.append(_bar("-"))
        print("\n".join(_soft_lines))

    # ── Map clusters to levers ─────────────────────────────────────
    lever_assignments: dict[int, list[dict]] = {}
    cluster_lines = [_section(f"Failure Clusters ({len(clusters)} total)", "-"), "|"]
    _root_cause_counter: Counter[str] = Counter()
    _lever_counter: Counter[int] = Counter()
    _all_cluster_qids: set[str] = set()
    _clusters_with_asi = 0
    _clusters_with_blame = 0
    for ci, c in enumerate(clusters, 1):
        mapped = _map_to_lever(
            c["root_cause"],
            asi_failure_type=c.get("asi_failure_type"),
            blame_set=c.get("asi_blame_set"),
            judge=c.get("affected_judge"),
        )
        c["_mapped_lever"] = mapped
        lever_assignments.setdefault(mapped, []).append(c)
        blame = c.get("asi_blame_set", c.get("blame_set", []))
        qids = c["question_ids"]
        asi_ft = c.get("asi_failure_type", "n/a")
        cluster_lines.append(f"|  Cluster {ci} / {len(clusters)}")
        cluster_lines.append(f"|    {'Judge:':<24s} {c['affected_judge']}")
        cluster_lines.append(f"|    {'Root cause:':<24s} {c['root_cause']}")
        cluster_lines.append(f"|    {'ASI failure type:':<24s} {asi_ft}")
        cluster_lines.append(f"|    {'Mapped lever:':<24s} {mapped}")
        blame_str = ", ".join(blame) if isinstance(blame, list) and blame else str(blame) if blame else "(none)"
        cluster_lines.append(f"|    {'Blame:':<24s} {blame_str}")
        cluster_lines.append(f"|    Questions ({len(qids)}):")
        for qid in qids:
            cluster_lines.append(f"|      {qid}")
        cluster_lines.append("|")
        _root_cause_counter[c["root_cause"]] += 1
        _lever_counter[mapped] += 1
        _all_cluster_qids.update(qids)
        if asi_ft and asi_ft != "n/a":
            _clusters_with_asi += 1
        if blame and blame != []:
            _clusters_with_blame += 1

    _lever_summary = ", ".join(f"lever {k} = {v}" for k, v in sorted(_lever_counter.items()))
    _top_causes = ", ".join(f"{k} ({v})" for k, v in _root_cause_counter.most_common(5))
    cluster_lines.append("|  --- Summary ---")
    cluster_lines.append(f"|    {'Clusters by lever:':<24s} {_lever_summary}")
    cluster_lines.append(f"|    {'Unique questions:':<24s} {len(_all_cluster_qids)}")
    cluster_lines.append(f"|    {'Top root causes:':<24s} {_top_causes}")
    cluster_lines.append(f"|    {'Clusters with ASI:':<24s} {_clusters_with_asi} of {len(clusters)}")
    cluster_lines.append(f"|    {'Clusters with blame:':<24s} {_clusters_with_blame} of {len(clusters)}")
    cluster_lines.append(_bar("-"))
    print("\n".join(cluster_lines))

    # ── ASI / provenance rows for Delta ────────────────────────────
    all_clusters_for_asi = clusters + soft_clusters
    _asi_rows: list[dict] = []
    _prov_rows: list[dict] = []
    for c in all_clusters_for_asi:
        sig_type = c.get("signal_type", "hard")
        for qt in c.get("question_traces", []):
            qid = qt.get("question_id", "")
            for jt in qt.get("failed_judges", []):
                _asi_rows.append({
                    "question_id": qid,
                    "judge": jt.get("judge", ""),
                    "value": "no",
                    "failure_type": jt.get("asi_failure_type_raw"),
                    "blame_set": jt.get("blame_set"),
                    "counterfactual_fix": jt.get("counterfactual_fix"),
                    "wrong_clause": jt.get("wrong_clause"),
                })
                _prov_rows.append({
                    "question_id": qid,
                    "signal_type": sig_type,
                    "judge": jt.get("judge", ""),
                    "judge_verdict": jt.get("verdict", "FAIL"),
                    "asi_failure_type_raw": jt.get("asi_failure_type_raw"),
                    "resolved_root_cause": jt.get("resolved_root_cause", "other"),
                    "resolution_method": jt.get("resolution_method", "unknown"),
                    "blame_set": jt.get("blame_set"),
                    "counterfactual_fix": jt.get("counterfactual_fix"),
                    "wrong_clause": jt.get("wrong_clause"),
                    "rationale_snippet": jt.get("rationale_snippet"),
                    "cluster_id": c.get("cluster_id", ""),
                })

    # ── Pipeline lineage summary ───────────────────────────────────
    _lineage_lines = ["\n== PIPELINE LINEAGE ==========================================================", "|"]
    for c in clusters:
        mapped = c.get("_mapped_lever", _map_to_lever(
            c["root_cause"],
            asi_failure_type=c.get("asi_failure_type"),
            blame_set=c.get("asi_blame_set"),
            judge=c.get("affected_judge"),
        ))
        for qt in c.get("question_traces", []):
            qid = qt.get("question_id", "")
            judges_info = ", ".join(
                f"{jt['judge']} ({jt.get('resolved_root_cause', '?')})"
                for jt in qt.get("failed_judges", [])
            )
            blame = c.get("asi_blame_set") or "(none)"
            cfix_list = c.get("asi_counterfactual_fixes", [])
            cfix = str(cfix_list[0])[:120] if cfix_list else "(none)"
            _lineage_lines.append(f"|  Q: {qid}")
            _lineage_lines.append(f"|    Failed judges:         {judges_info}")
            _lineage_lines.append(f"|    Dominant root cause:   {c['root_cause']}")
            _lineage_lines.append(f"|    Blame:                 {blame}")
            _lineage_lines.append(f"|    Counterfactual:        \"{cfix}\"")
            _lineage_lines.append(f"|    -> Cluster {c['cluster_id']} -> Lever {mapped} ({LEVER_NAMES.get(mapped, '?')})")
            _lineage_lines.append("|")
    _lineage_lines.append("=" * 78)
    print("\n".join(_lineage_lines))

    return {
        "lever_assignments": lever_assignments,
        "all_clusters": clusters,
        "soft_signal_clusters": soft_clusters,
        "asi_rows": _asi_rows,
        "prov_rows": _prov_rows,
        "lever_counter": dict(_lever_counter),
    }


def _run_gate_checks(
    *,
    spark: Any,
    w: WorkspaceClient,
    run_id: str,
    space_id: str,
    exp_name: str,
    domain: str,
    iteration_counter: int,
    ag_id: str,
    benchmarks: list[dict],
    proposals: list[dict],
    patches: list[dict],
    apply_log: dict,
    clusters: list[dict],
    metadata_snapshot: dict,
    predict_fn: Any,
    scorers: list,
    prev_model_id: str,
    best_scores: dict[str, float],
    best_accuracy: float,
    catalog: str,
    schema: str,
    reference_sqls: dict[str, str],
    noise_floor: float,
) -> dict:
    """Run slice → P0 → full eval gate sequence for an action group.

    Returns a dict with:
      - ``passed``: bool
      - ``full_scores``: dict (only if full eval ran)
      - ``full_accuracy``: float (only if full eval ran)
      - ``new_model_id``: str (only if full eval ran)
      - ``full_result``: dict (only if full eval ran)
      - ``rollback_reason``: str (only if failed)
    """
    import mlflow

    uc_schema = f"{catalog}.{schema}"

    has_dict_changes = any(
        (entry.get("patch", {}) or {}).get("enable_entity_matching")
        or (entry.get("action", {}) or {}).get("type") in (
            "enable_value_dictionary", "enable_entity_matching",
        )
        for entry in apply_log.get("applied", [])
    )
    wait_time = (
        PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS if has_dict_changes
        else PROPAGATION_WAIT_SECONDS
    )
    _wait_note = " (extended for value dictionary rebuild)" if has_dict_changes else ""
    patched_objects = apply_log.get("patched_objects", [])
    print(
        _section("Propagation Wait", "-") + "\n"
        + _kv("AG", f"{ag_id}: {len(apply_log.get('applied', []))} patches applied") + "\n"
        + _kv("Patched objects", ", ".join(str(o) for o in patched_objects) if patched_objects else "(none)") + "\n"
        + _kv("Wait time", f"{wait_time}s{_wait_note}") + "\n"
        + _bar("-")
    )
    time.sleep(wait_time)

    # ── Slice gate ────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    affected_qids: set[str] = set()
    for p in proposals:
        cid = p.get("cluster_id", "")
        for c in clusters:
            if c.get("cluster_id") == cid:
                affected_qids.update(c.get("question_ids", []))
    slice_benchmarks = filter_benchmarks_by_scope(
        benchmarks, "slice", patched_objects,
        affected_question_ids=affected_qids,
    )
    if slice_benchmarks:
        _ensure_sql_context(spark, catalog, schema)
        write_stage(
            spark, run_id, f"AG_{ag_id}_SLICE_EVAL", "STARTED",
            task_key="lever_loop", iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )
        slice_result = run_evaluation(
            space_id, exp_name, iteration_counter, slice_benchmarks,
            domain, prev_model_id, "slice",
            predict_fn, scorers,
            spark=spark, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            patched_objects=patched_objects,
            reference_sqls=reference_sqls if reference_sqls else None,
        )
        slice_scores = slice_result.get("scores", {})
        effective_slice_tol = max(SLICE_GATE_TOLERANCE, noise_floor + 2.0)
        slice_drops = detect_regressions(
            slice_scores, best_scores, threshold=effective_slice_tol,
        )

        if slice_drops:
            _score_changes = ", ".join(
                f"{d['judge']} {best_scores.get(d['judge'], 0):.1f}->{slice_scores.get(d['judge'], 0):.1f} ({d['drop']:+.1f})"
                for d in slice_drops
            )
            print(
                _section(f"SLICE GATE [{ag_id}]: FAIL", "-") + "\n"
                + _kv("Regressions", _score_changes) + "\n"
                + _kv("Action", "ROLLBACK") + "\n"
                + _bar("-")
            )
            try:
                update_provenance_gate(
                    spark, run_id, iteration_counter, 0,
                    "slice", "rollback",
                    {"regressions": [{"judge": d["judge"], "drop": d["drop"]} for d in slice_drops]},
                    catalog, schema,
                )
            except Exception:
                logger.debug("Failed to update provenance gate", exc_info=True)
            try:
                log_gate_feedback_on_traces(
                    slice_result, "slice", "rollback",
                    regressions=slice_drops, lever=0, iteration=iteration_counter,
                )
            except Exception:
                logger.debug("Failed to log gate feedback", exc_info=True)
            return {"passed": False, "rollback_reason": f"slice_gate: {slice_drops[0]['judge']}"}
        else:
            _sc = ", ".join(
                f"{j} {best_scores.get(j, 0):.1f}->{slice_scores.get(j, 0):.1f}"
                for j in sorted(slice_scores)
            )
            print(
                _section(f"SLICE GATE [{ag_id}]: PASS", "-") + "\n"
                + _kv("Score changes", _sc) + "\n"
                + _bar("-")
            )

    # ── P0 gate ───────────────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    p0_benchmarks = filter_benchmarks_by_scope(benchmarks, "p0")
    if p0_benchmarks:
        _ensure_sql_context(spark, catalog, schema)
        p0_result = run_evaluation(
            space_id, exp_name, iteration_counter, p0_benchmarks,
            domain, prev_model_id, "p0",
            predict_fn, scorers,
            spark=spark, catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
            reference_sqls=reference_sqls if reference_sqls else None,
        )
        p0_failures = p0_result.get("failures", [])
        if p0_failures:
            print(
                _section(f"P0 GATE [{ag_id}]: FAIL", "-") + "\n"
                + _kv("P0 questions failing", len(p0_failures)) + "\n"
                + _kv("Action", "ROLLBACK") + "\n"
                + _bar("-")
            )
            return {"passed": False, "rollback_reason": f"p0_gate: {len(p0_failures)} failures"}
        else:
            print(
                _section(f"P0 GATE [{ag_id}]: PASS", "-") + "\n"
                + _kv("P0 benchmarks", len(p0_benchmarks)) + "\n"
                + _bar("-")
            )

    # ── Full evaluation ───────────────────────────────────────────────
    try:
        mlflow.end_run()
    except Exception:
        pass
    write_stage(
        spark, run_id, f"AG_{ag_id}_FULL_EVAL", "STARTED",
        task_key="lever_loop", iteration=iteration_counter,
        catalog=catalog, schema=schema,
    )

    new_model_id = create_genie_model_version(
        w, space_id, metadata_snapshot, iteration_counter, domain,
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
        reference_sqls=reference_sqls if reference_sqls else None,
    )

    full_scores = full_result.get("scores", {})
    full_accuracy = full_result.get("overall_accuracy", 0.0)

    write_iteration(
        spark, run_id, iteration_counter, full_result,
        catalog=catalog, schema=schema,
        lever=0, eval_scope="full", model_id=new_model_id,
    )

    effective_regression_tol = max(REGRESSION_THRESHOLD, noise_floor)
    regressions = detect_regressions(
        full_scores, best_scores, threshold=effective_regression_tol,
    )

    accuracy_drop = best_accuracy - full_accuracy
    accuracy_threshold = max(effective_regression_tol / 2, noise_floor)
    if accuracy_drop > accuracy_threshold:
        regressions.append({
            "judge": "overall_accuracy",
            "previous": best_accuracy,
            "current": full_accuracy,
            "drop": accuracy_drop,
        })

    if regressions:
        _reg_details = ", ".join(
            f"{r['judge']} {best_scores.get(r['judge'], 0):.1f}->{full_scores.get(r['judge'], 0):.1f} ({r['drop']:+.1f})"
            for r in regressions
        )
        print(
            _section(f"FULL EVAL [{ag_id}]: FAIL (REGRESSION)", "-") + "\n"
            + _kv("Accuracy", f"{best_accuracy:.1f}% -> {full_accuracy:.1f}%") + "\n"
            + _kv("Regressions", _reg_details) + "\n"
            + _kv("Action", "ROLLBACK") + "\n"
            + _bar("-")
        )
        try:
            update_provenance_gate(
                spark, run_id, iteration_counter, 0,
                "full", "rollback",
                {"regressions": [{"judge": r["judge"], "drop": r["drop"]} for r in regressions]},
                catalog, schema,
            )
        except Exception:
            logger.debug("Failed to update provenance gate (full rollback)", exc_info=True)
        try:
            log_gate_feedback_on_traces(
                full_result, "full", "rollback",
                regressions=regressions, lever=0, iteration=iteration_counter,
            )
        except Exception:
            logger.debug("Failed to log full eval gate feedback", exc_info=True)
        return {"passed": False, "rollback_reason": f"full_eval: {regressions[0]['judge']}"}

    # ── PASSED ────────────────────────────────────────────────────────
    _score_delta = ", ".join(
        f"{j} {best_scores.get(j, 0):.1f}->{full_scores.get(j, 0):.1f}"
        for j in sorted(full_scores)
    )
    print(
        _section(f"FULL EVAL [{ag_id}]: PASS -- ACCEPTED", "=") + "\n"
        + _kv("Accuracy", f"{best_accuracy:.1f}% -> {full_accuracy:.1f}% ({full_accuracy - best_accuracy:+.1f}%)") + "\n"
        + _kv("Score changes", _score_delta) + "\n"
        + _bar("=")
    )
    try:
        update_provenance_gate(
            spark, run_id, iteration_counter, 0,
            "full", "pass", None, catalog, schema,
        )
    except Exception:
        logger.debug("Failed to update provenance gate (full pass)", exc_info=True)
    try:
        log_gate_feedback_on_traces(
            full_result, "full", "pass",
            lever=0, iteration=iteration_counter,
        )
    except Exception:
        logger.debug("Failed to log full eval gate feedback", exc_info=True)

    return {
        "passed": True,
        "full_scores": full_scores,
        "full_accuracy": full_accuracy,
        "new_model_id": new_model_id,
        "full_result": full_result,
    }


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
    triggered_by: str = "",
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
    lever_changes: list[dict] = []
    all_failure_trace_ids: list[str] = []
    all_regression_trace_ids: list[str] = []

    _ensure_sql_context(spark, catalog, schema)
    from genie_space_optimizer.optimization.evaluation import build_metric_view_measures
    _mv_measures = build_metric_view_measures(config)
    predict_fn = make_predict_fn(
        w, space_id, spark, catalog, schema,
        metric_view_measures=_mv_measures,
        optimization_run_id=run_id,
        triggered_by=triggered_by,
    )
    scorers = make_all_scorers(w, spark, catalog, schema)
    uc_schema = f"{catalog}.{schema}"
    metadata_snapshot = config.get("_parsed_space", config)

    uc_columns = config.get("_uc_columns", [])
    if uc_columns:
        enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

    # Stage 2.75: Proactive description enrichment for blank columns
    enrichment_result = _run_description_enrichment(
        w, spark, run_id, space_id, config, metadata_snapshot, catalog, schema,
    )
    if enrichment_result.get("total_enriched", 0) > 0:
        from genie_space_optimizer.common.genie_client import fetch_space_config
        config = fetch_space_config(w, space_id)
        config["_uc_columns"] = uc_columns
        metadata_snapshot = config.get("_parsed_space", config)
        if uc_columns:
            enrich_metadata_with_uc_types(metadata_snapshot, uc_columns)

    baseline_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
    reference_sqls: dict[str, str] = {}
    if baseline_iter:
        rows_json = baseline_iter.get("rows_json")
        if isinstance(rows_json, list):
            reference_sqls = extract_reference_sqls({"rows": rows_json})
        elif isinstance(rows_json, str):
            try:
                reference_sqls = extract_reference_sqls(
                    {"rows": json.loads(rows_json)}
                )
            except (json.JSONDecodeError, TypeError):
                pass
    logger.info(
        "Lever loop: %d reference SQLs from baseline for repeatability scoring",
        len(reference_sqls),
    )

    _arbiter_actions = _extract_arbiter_actions_from_baseline(
        spark, run_id, catalog, schema,
    )
    _genie_correct_count = sum(
        1 for a in _arbiter_actions if a.get("verdict") == "genie_correct"
    )
    if _genie_correct_count >= ARBITER_CORRECTION_TRIGGER and _arbiter_actions:
        from genie_space_optimizer.optimization.benchmarks import apply_benchmark_corrections

        print(
            f"\n-- ARBITER BENCHMARK CORRECTIONS " + "-" * 19 + "\n"
            f"  genie_correct count: {_genie_correct_count} "
            f"(threshold: {ARBITER_CORRECTION_TRIGGER})\n"
            f"  Corrections to apply: {len(_arbiter_actions)}"
        )
        for _ac in _arbiter_actions:
            print(
                f"    - \"{_ac['question'][:60]}\" -> {_ac['new_expected_sql'][:80]}"
            )
        print("-" * 52)

        correction_result = apply_benchmark_corrections(
            _arbiter_actions, spark, uc_schema, domain,
        )
        print(
            f"  Applied: {correction_result['applied']}, "
            f"Skipped: {correction_result['skipped']}"
        )
        if correction_result["errors"]:
            print(f"  Errors: {correction_result['errors'][:3]}")
        print("-" * 52)
    elif _genie_correct_count > 0:
        print(
            f"\n-- ARBITER: {_genie_correct_count} genie_correct verdicts "
            f"(below threshold {ARBITER_CORRECTION_TRIGGER}, no corrections applied)"
        )

    # ── Analyze failures once before the lever loop ─────────────────
    _analysis = _analyze_and_distribute(
        spark, run_id, catalog, schema, metadata_snapshot,
        iteration_counter, lever_label=0,
    )
    lever_assignments = _analysis["lever_assignments"]
    clusters = _analysis["all_clusters"]
    soft_signal_clusters = _analysis["soft_signal_clusters"]

    try:
        write_asi_results(spark, run_id, iteration_counter, _analysis["asi_rows"], catalog, schema, mlflow_run_id="")
    except Exception:
        logger.debug("Failed to write ASI results", exc_info=True)
    try:
        write_provenance(spark, run_id, iteration_counter, 0, _analysis["prov_rows"], catalog, schema)
    except Exception:
        logger.debug("Failed to write provenance rows", exc_info=True)

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 1: Holistic Strategist
    # ═══════════════════════════════════════════════════════════════════
    print(_section("PHASE 1: HOLISTIC STRATEGIST", "="))
    strategy = _generate_holistic_strategy(
        clusters=clusters,
        soft_signal_clusters=soft_signal_clusters,
        metadata_snapshot=metadata_snapshot,
        w=w,
    )
    action_groups = strategy.get("action_groups", [])
    action_groups.sort(key=lambda a: a.get("priority", 999))

    if not action_groups:
        print(
            _section("Strategy produced 0 action groups — nothing to do", "-") + "\n"
            + _bar("-")
        )
        logger.info("Strategist returned 0 action groups — lever loop ending early")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE 2 + 3: Execute Action Groups with Gating
    # ═══════════════════════════════════════════════════════════════════
    ags_attempted: list[str] = []
    ags_accepted: list[str] = []
    ags_rolled_back: list[str] = []
    noise_floor = 100.0 / max(len(benchmarks), 1)
    global_rewrite_applied = False

    for ag in action_groups:
        ag_id = ag.get("id", f"AG{len(ags_attempted) + 1}")

        if all_thresholds_met(best_scores, thresholds):
            logger.info("Convergence: all thresholds met before AG %s", ag_id)
            break
        if iteration_counter >= max_iterations:
            logger.info("Max iterations (%d) reached before AG %s", max_iterations, ag_id)
            break

        iteration_counter += 1
        ags_attempted.append(ag_id)
        lever_keys = sorted(ag.get("lever_directives", {}).keys())

        print(
            _section(f"ACTION GROUP {ag_id} — Iteration {iteration_counter}") + "\n"
            + _kv("Root cause", ag.get("root_cause_summary", "?")[:120]) + "\n"
            + _kv("Levers", ", ".join(lever_keys)) + "\n"
            + _kv("Affected questions", len(ag.get("affected_questions", []))) + "\n"
            + _kv("Best accuracy", f"{best_accuracy:.1f}%") + "\n"
            + _scorecard(best_scores) + "\n"
            + _bar("=")
        )

        write_stage(
            spark, run_id, f"AG_{ag_id}_STARTED", "STARTED",
            task_key="lever_loop", iteration=iteration_counter,
            catalog=catalog, schema=schema,
        )

        # ── Generate proposals for ALL levers in this action group ──
        all_proposals: list[dict] = []
        for lever_key in lever_keys:
            lever_int = int(lever_key)
            levers_attempted.append(lever_int)
            lever_proposals = generate_proposals_from_strategy(
                strategy=strategy,
                action_group=ag,
                metadata_snapshot=metadata_snapshot,
                target_lever=lever_int,
                apply_mode=apply_mode,
                w=w,
                spark=spark,
                catalog=catalog,
                gold_schema=schema,
            )
            all_proposals.extend(lever_proposals)

        # Lever 5: apply global instruction rewrite only once (on first AG with L5)
        if "5" in lever_keys and not global_rewrite_applied:
            global_rewrite_applied = True
        elif "5" not in lever_keys and not global_rewrite_applied:
            gi = strategy.get("global_instruction_rewrite", "")
            if gi and ag == action_groups[-1]:
                l5_proposals = generate_proposals_from_strategy(
                    strategy=strategy,
                    action_group={**ag, "lever_directives": {**ag.get("lever_directives", {}), "5": {}}},
                    metadata_snapshot=metadata_snapshot,
                    target_lever=5,
                    apply_mode=apply_mode,
                    w=w,
                    spark=spark,
                    catalog=catalog,
                    gold_schema=schema,
                )
                all_proposals.extend(l5_proposals)
                global_rewrite_applied = True

        # ── Log proposals ────────────────────────────────────────────
        _n_valid = 0
        _n_failed = 0
        proposal_lines = [_section(f"[{ag_id}] Proposals ({len(all_proposals)} total)", "-"), "|"]
        for pi, p in enumerate(all_proposals, 1):
            cluster_id = p.get("cluster_id", "?")
            ptype = p.get("type", p.get("patch_type", "?"))
            target = p.get("target", p.get("target_object", "")) or "(none)"
            rationale = str(p.get("rationale", ""))
            proposed_value = str(p.get("proposed_value", ""))
            table = p.get("table", "")
            column = p.get("column", "")
            is_failed = "not valid JSON" in rationale or "non-JSON" in rationale.lower()
            status = "FAILED (non-JSON)" if is_failed else "OK"
            if is_failed:
                _n_failed += 1
            else:
                _n_valid += 1

            proposal_lines.append(f"|  Proposal {pi} / {len(all_proposals)}  [{cluster_id}]")
            proposal_lines.append(f"|    {'Type:':<24s} {ptype}")
            proposal_lines.append(f"|    {'Lever:':<24s} {p.get('lever', '?')}")
            if table:
                proposal_lines.append(f"|    {'Table:':<24s} {table}")
            if column:
                proposal_lines.append(f"|    {'Column:':<24s} {column}")
            proposal_lines.append(f"|    {'Rationale:':<24s} {rationale[:200]}")
            _p_col_sect = p.get("column_sections")
            _p_tbl_sect = p.get("table_sections")
            if isinstance(_p_col_sect, dict) and _p_col_sect:
                proposal_lines.append(f"|    Sections proposed:")
                for _sk, _sv in _p_col_sect.items():
                    _sv_str = str(_sv).replace("\n", " ")
                    proposal_lines.append(f"|      {_sk}: \"{_sv_str[:100]}\"")
            elif isinstance(_p_tbl_sect, dict) and _p_tbl_sect:
                proposal_lines.append(f"|    Table sections proposed:")
                for _sk, _sv in _p_tbl_sect.items():
                    _sv_str = str(_sv).replace("\n", " ")
                    proposal_lines.append(f"|      {_sk}: \"{_sv_str[:100]}\"")
            elif proposed_value:
                _val_preview = proposed_value.replace("\n", "\\n")
                proposal_lines.append(f"|    {'Value (preview):':<24s} {_val_preview[:300]}")
            proposal_lines.append(f"|    {'Status:':<24s} {status}")
            proposal_lines.append("|")

        proposal_lines.append("|  --- Summary ---")
        proposal_lines.append(f"|    {'Valid proposals:':<24s} {_n_valid} of {len(all_proposals)}")
        if _n_failed:
            proposal_lines.append(f"|    {'Failed (non-JSON):':<24s} {_n_failed}")
        proposal_lines.append(f"|    Proceeding with {_n_valid} patch(es)")
        proposal_lines.append(_bar("-"))
        print("\n".join(proposal_lines))

        # ── Provenance log ───────────────────────────────────────────
        _prov_patch_lines = ["\n-- Patch Provenance " + "-" * 58]
        for pi, p in enumerate(all_proposals, 1):
            prov = p.get("provenance", {})
            if not prov:
                continue
            cid = prov.get("cluster_id", "?")
            rc = prov.get("root_cause", "?")
            lv = prov.get("lever", "?")
            ln = prov.get("lever_name", "?")
            pt = prov.get("patch_type", "?")
            _prov_patch_lines.append(f"|  P{pi:03d} [{cid}] lever={lv} ({ln}) type={pt} root_cause={rc}")
        _prov_patch_lines.append("-" * 78)
        print("\n".join(_prov_patch_lines))

        _prop_mappings = [
            {"cluster_id": p.get("cluster_id"), "proposal_id": p.get("proposal_id"), "patch_type": p.get("patch_type")}
            for p in all_proposals if p.get("cluster_id")
        ]
        try:
            update_provenance_proposals(spark, run_id, iteration_counter, _prop_mappings, catalog, schema)
        except Exception:
            logger.debug("Failed to update provenance proposals", exc_info=True)

        if not all_proposals:
            print(_section(f"[{ag_id}] No proposals — SKIPPING action group", "-"))
            write_stage(
                spark, run_id, f"AG_{ag_id}_STARTED", "SKIPPED",
                task_key="lever_loop", iteration=iteration_counter,
                detail={"reason": "no_proposals"},
                catalog=catalog, schema=schema,
            )
            continue

        # ── Apply coordinated patch set ──────────────────────────────
        patches = proposals_to_patches(all_proposals)
        apply_log = apply_patch_set(
            w, space_id, patches, metadata_snapshot, apply_mode=apply_mode,
        )

        for idx, entry in enumerate(apply_log.get("applied", [])):
            write_patch(
                spark, run_id, iteration_counter, 0, idx,
                _build_patch_record(entry, 0, apply_mode),
                catalog, schema,
            )

        if not apply_log.get("patch_deployed", False) and apply_log.get("applied"):
            _pe = apply_log.get("patch_error", "unknown")
            print(
                _section(f"[{ag_id}] PATCH DEPLOY FAILED", "!") + "\n"
                + _kv("Error", str(_pe)[:300]) + "\n"
                + _bar("!")
            )
            write_stage(
                spark, run_id, f"AG_{ag_id}_PATCH_FAILED", "ERROR",
                task_key="lever_loop", iteration=iteration_counter,
                error_message=str(_pe)[:500],
                catalog=catalog, schema=schema,
            )
            continue

        # ── Applied Patches Detail ───────────────────────────────────
        _applied = apply_log.get("applied", [])
        if _applied:
            _ap_lines = [_section(f"[{ag_id}] Applied Patches ({len(_applied)})", "-")]
            for ai, aentry in enumerate(_applied, 1):
                _ap = aentry.get("patch", {})
                _aa = aentry.get("action", {})
                _ap_type = _ap.get("type", _aa.get("action_type", "?"))
                _ap_table = _ap.get("table", "")
                _ap_column = _ap.get("column", "")
                _ap_new_text = _ap.get("new_text", "")
                _ap_join_spec = _ap.get("join_spec")
                _ap_example_q = _ap.get("example_question", "")
                _ap_example_sql = _ap.get("example_sql", "")
                _ap_lines.append(f"|  Patch {ai}: {_ap_type}")
                if _ap_table:
                    _ap_lines.append(f"|    Table: {_ap_table}")
                if _ap_column:
                    _ap_lines.append(f"|    Column: {_ap_column}")
                _ap_struct = _ap.get("structured_sections") or {}
                if _ap_struct and isinstance(_ap_struct, dict):
                    for _sk, _sv in _ap_struct.items():
                        _ap_lines.append(f"|    **{_sk}:** {str(_sv).replace(chr(10), ' ')[:120]}")
                elif _ap_new_text:
                    _ap_lines.append(f"|    Value: {_ap_new_text.replace(chr(10), ' ')[:200]}")
                _tbl_struct = _ap.get("table_sections") if not _ap_struct else None
                if _tbl_struct and isinstance(_tbl_struct, dict):
                    for _sk, _sv in _tbl_struct.items():
                        _ap_lines.append(f"|    **{_sk}:** {str(_sv).replace(chr(10), ' ')[:120]}")
                if _ap_example_q:
                    _ap_lines.append(f"|    Example Q: {_ap_example_q[:120]}")
                if _ap_example_sql:
                    _ap_lines.append(f"|    Example SQL: {_ap_example_sql[:120]}")
                if _ap_join_spec:
                    _ap_lines.append(f"|    Join: {json.dumps(_ap_join_spec)[:200]}")
            _ap_lines.append(_bar("-"))
            print("\n".join(_ap_lines))

        # ── Gate checks (slice → P0 → full eval) ────────────────────
        gate_result = _run_gate_checks(
            spark=spark,
            w=w,
            run_id=run_id,
            space_id=space_id,
            exp_name=exp_name,
            domain=domain,
            iteration_counter=iteration_counter,
            ag_id=ag_id,
            benchmarks=benchmarks,
            proposals=all_proposals,
            patches=patches,
            apply_log=apply_log,
            clusters=clusters,
            metadata_snapshot=metadata_snapshot,
            predict_fn=predict_fn,
            scorers=scorers,
            prev_model_id=prev_model_id,
            best_scores=best_scores,
            best_accuracy=best_accuracy,
            catalog=catalog,
            schema=schema,
            reference_sqls=reference_sqls,
            noise_floor=noise_floor,
        )

        if not gate_result.get("passed"):
            reason = gate_result.get("rollback_reason", "unknown")
            rollback(apply_log, w, space_id, metadata_snapshot)
            mark_patches_rolled_back(
                spark, run_id, iteration_counter, reason, catalog, schema,
            )
            ags_rolled_back.append(ag_id)
            for lk in lever_keys:
                levers_rolled_back.append(int(lk))
            write_stage(
                spark, run_id, f"AG_{ag_id}_STARTED", "ROLLED_BACK",
                task_key="lever_loop", iteration=iteration_counter,
                detail={"reason": reason},
                catalog=catalog, schema=schema,
            )
            continue

        # ── Accept action group ──────────────────────────────────────
        ags_accepted.append(ag_id)
        for lk in lever_keys:
            levers_accepted.append(int(lk))

        full_scores = gate_result["full_scores"]
        full_accuracy = gate_result["full_accuracy"]
        new_model_id = gate_result["new_model_id"]
        full_result = gate_result["full_result"]

        _full_trace_map = full_result.get("trace_map", {})
        _full_failures = set(full_result.get("failure_question_ids", []))
        for qid, tid in _full_trace_map.items():
            if qid in _full_failures:
                all_failure_trace_ids.append(tid)

        lever_changes.append({
            "lever": ag_id,
            "lever_name": f"AG {ag_id}: {ag.get('root_cause_summary', '')[:60]}",
            "patches": [
                {"change": p.get("change_description", ""), "patch_type": p.get("patch_type", "")}
                for p in all_proposals
            ],
            "accuracy_delta": full_accuracy - best_accuracy,
        })
        best_scores = full_scores
        best_accuracy = full_accuracy
        best_model_id = new_model_id
        best_iteration = iteration_counter
        prev_scores = full_scores
        prev_model_id = new_model_id

        new_refs = extract_reference_sqls(full_result)
        if new_refs:
            reference_sqls.update(new_refs)

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
                uc_schema=f"{catalog}.{schema}",
                space_id=space_id,
                instruction_text=post_instructions,
                run_id=run_id,
                lever=0,
                iteration=iteration_counter,
                accuracy=best_accuracy,
                domain=domain,
            )

        write_stage(
            spark, run_id, f"AG_{ag_id}_STARTED", "COMPLETE",
            task_key="lever_loop", iteration=iteration_counter,
            detail={
                "accuracy": full_accuracy,
                "accepted": True,
                "patches_applied": len(apply_log.get("applied", [])),
                "levers": lever_keys,
            },
            catalog=catalog, schema=schema,
        )

        metadata_snapshot = apply_log.get("post_snapshot", metadata_snapshot)

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

    session_info: dict = {}
    if all_failure_trace_ids or all_regression_trace_ids:
        try:
            from genie_space_optimizer.optimization.labeling import create_review_session
            session_info = create_review_session(
                run_id=run_id,
                domain=domain,
                experiment_name=exp_name,
                uc_schema=f"{catalog}.{schema}",
                failure_trace_ids=list(dict.fromkeys(all_failure_trace_ids)),
                regression_trace_ids=list(dict.fromkeys(all_regression_trace_ids)),
            )
            _sname = session_info.get("session_name", "")
            _srun = session_info.get("session_run_id", "")
            if _sname:
                print(
                    f"\n[MLflow Review] Labeling session created for human review:\n"
                    f"  Name: {_sname}\n"
                    f"  Traces: {session_info.get('trace_count', 0)}\n"
                )
            if _sname or _srun:
                update_run_status(
                    spark, run_id, catalog, schema,
                    labeling_session_name=_sname,
                    labeling_session_run_id=_srun,
                )
        except Exception:
            logger.debug("Failed to create labeling session", exc_info=True)

    return {
        "scores": best_scores,
        "accuracy": best_accuracy,
        "model_id": best_model_id,
        "iteration_counter": iteration_counter,
        "best_iteration": best_iteration,
        "levers_attempted": levers_attempted,
        "levers_accepted": levers_accepted,
        "levers_rolled_back": levers_rolled_back,
        "labeling_session": session_info,
        "_debug_ref_sqls_count": len(reference_sqls),
        "_debug_failure_rows_loaded": len(_get_failure_rows(spark, run_id, catalog, schema)),
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
                detail={"benchmark_count": len(benchmarks), "runs": 2},
            )

            uc_schema = f"{catalog}.{schema}"
            latest_iter = load_latest_full_iteration(spark, run_id, catalog, schema)
            reference_sqls: dict[str, str] = {}
            if latest_iter:
                rows_json = latest_iter.get("rows_json")
                if isinstance(rows_json, list):
                    reference_sqls = extract_reference_sqls({"rows": rows_json})
                elif isinstance(rows_json, str):
                    try:
                        reference_sqls = extract_reference_sqls(
                            {"rows": json.loads(rows_json)}
                        )
                    except (json.JSONDecodeError, TypeError):
                        pass
            logger.info(
                "Repeatability: %d reference SQLs from latest iteration",
                len(reference_sqls),
            )

            _ensure_sql_context(spark, catalog, schema)
            predict_fn = make_predict_fn(w, space_id, spark, catalog, schema)

            rep_pcts: list[float] = []
            try:
                for rep_run_idx in range(1, 3):
                    _check_timeout(f"repeatability_run_{rep_run_idx}")
                    _emit_heartbeat(
                        f"repeatability_run_{rep_run_idx}",
                        force=True,
                        detail={"run": rep_run_idx, "of": 2},
                    )
                    rep_result = run_repeatability_evaluation(
                        space_id=space_id,
                        experiment_name=exp_name,
                        iteration=iteration_counter,
                        benchmarks=benchmarks,
                        domain=domain,
                        reference_sqls=reference_sqls,
                        predict_fn=predict_fn,
                        spark=spark,
                        catalog=catalog,
                        gold_schema=schema,
                        uc_schema=uc_schema,
                        model_id=prev_model_id,
                        run_label=f"final_{rep_run_idx}",
                    )
                    rep_pcts.append(rep_result.get("repeatability_pct", 0.0))
                    logger.info(
                        "Repeatability run %d/2: %.1f%%",
                        rep_run_idx,
                        rep_pcts[-1],
                    )

                _check_timeout("post_repeatability")
                repeatability_pct = (
                    sum(rep_pcts) / len(rep_pcts) if rep_pcts else 0.0
                )
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "COMPLETE",
                    task_key="finalize",
                    detail={
                        "average_pct": repeatability_pct,
                        "per_run_pcts": rep_pcts,
                        "total_questions": len(benchmarks),
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
                logger.exception("Repeatability evaluation failed")
                repeatability_pct = (
                    sum(rep_pcts) / len(rep_pcts) if rep_pcts else 0.0
                )
                write_stage(
                    spark, run_id, "REPEATABILITY_TEST", "FAILED",
                    task_key="finalize",
                    error_message="Repeatability evaluation exception",
                    detail={"partial_pcts": rep_pcts},
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

        # Publish benchmarks to the Genie Space's native benchmarks section
        # so they appear in the Genie UI and can be used for UI-based eval runs.
        benchmark_publish_count = 0
        _check_timeout("publish_benchmarks")
        _emit_heartbeat("publish_benchmarks", force=True)
        if benchmarks:
            try:
                from genie_space_optimizer.common.genie_client import (
                    publish_benchmarks_to_genie_space,
                )

                benchmark_publish_count = publish_benchmarks_to_genie_space(
                    w, space_id, benchmarks,
                )
                write_stage(
                    spark, run_id, "BENCHMARK_PUBLISH", "COMPLETE",
                    task_key="finalize",
                    detail={"published_count": benchmark_publish_count},
                    catalog=catalog, schema=schema,
                )
            except Exception:
                logger.warning(
                    "Failed to publish benchmarks to Genie space %s — "
                    "optimization results are still valid",
                    space_id,
                    exc_info=True,
                )
                write_stage(
                    spark, run_id, "BENCHMARK_PUBLISH", "FAILED",
                    task_key="finalize",
                    error_message="Benchmark publish failed (non-fatal)",
                    catalog=catalog, schema=schema,
                )
        else:
            write_stage(
                spark, run_id, "BENCHMARK_PUBLISH", "SKIPPED",
                task_key="finalize",
                detail={"reason": "no_benchmarks_available"},
                catalog=catalog, schema=schema,
            )

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
            "benchmark_publish_count": benchmark_publish_count,
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
            # Stage 2.5 + config prep (unified path for DAG and convenience)
            config = _prepare_lever_loop(
                w, spark, run_id_str, space_id, catalog, schema,
            )

            # Stage 3: Lever Loop
            loop_out = _run_lever_loop(
                w, spark, run_id_str, space_id, domain, benchmarks, exp_name,
                prev_scores, prev_accuracy, model_id, config,
                catalog, schema, levers, max_iterations, thresholds, apply_mode,
                triggered_by=triggered_by or "",
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


def _get_failed_judges(row: dict) -> list[str]:
    """Return list of individual scorer judge names that failed (value contains 'no')."""
    _NON_JUDGE_SUFFIXES = ("/rationale", "/source", "/metadata", "/error")
    failed: list[str] = []
    for col, val in row.items():
        is_judge = False
        if col.startswith("feedback/") and col.endswith("/value"):
            is_judge = True
        elif col.startswith("feedback/") and not any(col.endswith(s) for s in _NON_JUDGE_SUFFIXES):
            if "/" not in col.removeprefix("feedback/"):
                is_judge = True
        elif col.endswith("/value") and not col.startswith("feedback/"):
            is_judge = True
        if is_judge and "no" in str(val).lower():
            judge_name = col.replace("feedback/", "").replace("/value", "")
            failed.append(judge_name)
    return failed


def _has_individual_judge_failure(row: dict) -> bool:
    """Return True if at least one individual scorer judge failed (value contains 'no').

    Used to detect rows where the arbiter said 'both_correct' or 'genie_correct'
    but individual judges flagged suboptimal patterns worth learning from.
    """
    return len(_get_failed_judges(row)) > 0


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
