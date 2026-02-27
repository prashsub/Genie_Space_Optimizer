"""
Preflight logic — extracted from the harness to keep orchestration lean.

Fetches Genie Space config, UC metadata, loads or generates benchmarks,
validates SQL, registers judge prompts, and creates the initial MLflow
LoggedModel (iteration 0).
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

import mlflow

from genie_space_optimizer.common.config import (
    EXPERIMENT_PATH_TEMPLATE,
    TARGET_BENCHMARK_COUNT,
)
from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.common.uc_metadata import (
    extract_genie_space_table_refs,
    get_columns,
    get_columns_for_tables,
    get_columns_for_tables_rest,
    get_routines,
    get_routines_for_schemas,
    get_routines_for_schemas_rest,
    get_tags,
    get_tags_for_tables,
)
from genie_space_optimizer.optimization.benchmarks import validate_benchmarks
from genie_space_optimizer.optimization.applier import _get_general_instructions
from genie_space_optimizer.optimization.evaluation import (
    create_evaluation_dataset,
    extract_genie_space_benchmarks,
    generate_benchmarks,
    load_benchmarks_from_dataset,
    register_instruction_version,
    register_judge_prompts,
)
from genie_space_optimizer.optimization.models import create_genie_model_version
from genie_space_optimizer.optimization.state import load_run, write_stage

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _collect_or_empty(fetch_fn: Any, label: str) -> tuple[list[dict], str | None]:
    """Best-effort metadata fetch; continue if catalog permissions are limited.

    Returns ``(rows, error_message)``.  *error_message* is ``None`` on
    success and a human-readable string when the query fails.
    """
    try:
        df = fetch_fn()
        rows = [r.asDict() for r in df.collect()]
        if not rows:
            logger.info(
                "UC metadata query for %s returned 0 rows (query succeeded, no data matched)", label,
            )
        return rows, None
    except Exception as exc:
        logger.warning(
            "Skipping %s metadata: %s: %s", label, type(exc).__name__, exc,
        )
        return [], f"{type(exc).__name__}: {exc}"


def _resolve_experiment_path(
    *,
    run_data: dict,
    domain: str,
    ws: WorkspaceClient,
) -> str:
    """Pick a stable MLflow experiment path for this run.

    With OBO-first execution the job runs as the triggering user, so we
    always prefer ``/Users/<user_email>/genie-optimization/<domain>``.
    ``/Shared/`` is a last-resort fallback when identity cannot be resolved.
    """
    triggered_by = str(run_data.get("triggered_by") or "").strip()
    if "@" in triggered_by:
        return EXPERIMENT_PATH_TEMPLATE.format(user_email=triggered_by, domain=domain)

    try:
        current_user = str(ws.current_user.me().user_name or "").strip()
    except Exception:
        current_user = ""

    if "@" in current_user:
        return EXPERIMENT_PATH_TEMPLATE.format(user_email=current_user, domain=domain)

    return f"/Shared/genie-optimization/{domain}"


def _ensure_experiment_parent_dir(ws: WorkspaceClient, experiment_path: str) -> None:
    """Ensure the workspace parent directory exists before creating experiment."""
    if not experiment_path.startswith("/"):
        return
    parent = str(PurePosixPath(experiment_path).parent)
    if not parent or parent == "/":
        return
    try:
        ws.workspace.mkdirs(parent)
    except Exception as exc:
        logger.warning("Could not ensure experiment parent directory %s: %s", parent, exc)


def _has_non_email_user_home(path: str) -> bool:
    """Detect /Users/<principal>/... paths where principal is not an email."""
    parts = PurePosixPath(path).parts
    return len(parts) > 2 and parts[0] == "/" and parts[1] == "Users" and "@" not in parts[2]


def run_preflight(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    experiment_name: str | None = None,
) -> tuple[dict, list[dict], str, str]:
    """Execute the full preflight sequence (Stage 1).

    Steps:
      1. Fetch Genie Space config via API
      2. Fetch UC metadata (columns, tags, routines)
      3. Resolve or create MLflow experiment
      4. Load or generate benchmarks
      5. Validate benchmark SQL via EXPLAIN
      6. Sync benchmarks to MLflow evaluation dataset
      7. Register judge prompts (idempotent)
      8. Create LoggedModel iteration 0
      9. Write PREFLIGHT_STARTED → PREFLIGHT_COMPLETE to Delta

    Returns:
        (config, benchmarks, model_id, experiment_name)
    """
    write_stage(
        spark, run_id, "PREFLIGHT_STARTED", "STARTED",
        task_key="preflight", catalog=catalog, schema=schema,
    )

    run_data = load_run(spark, run_id, catalog, schema) or {}
    snapshot = run_data.get("config_snapshot", {})
    config: dict
    if isinstance(snapshot, dict) and snapshot:
        config = snapshot
        logger.info("Using config snapshot from run row for %s", run_id)
    else:
        config = fetch_space_config(w, space_id)
        logger.info("Fetched config for space %s", space_id)

    # OBO-first: jobs run as the triggering user, so Spark can query
    # information_schema for user catalogs directly.  Prefetched metadata
    # from the backend OBO call is kept as a cache optimisation only.
    prefetched = snapshot.get("_prefetched_uc_metadata", {}) if isinstance(snapshot, dict) else {}

    genie_table_refs = extract_genie_space_table_refs(config)
    if genie_table_refs:
        logger.info(
            "Genie space references %d data assets across schemas: %s",
            len(genie_table_refs),
            sorted({f"{c}.{s}" for c, s, _ in genie_table_refs if c and s}),
        )

    _pf = prefetched if isinstance(prefetched, dict) else {}
    _actual_source: dict[str, str] = {}

    def _usable_prefetch(key: str) -> list | None:
        """Return prefetched list only when non-empty; empty lists are treated as cache misses."""
        val = _pf.get(key)
        if isinstance(val, list) and val:
            _actual_source[key] = "prefetched"
            return val
        if isinstance(val, list):
            logger.info("Prefetched %s is empty, falling back", key)
        return None

    _collection_errors: dict[str, str] = {}

    def _rest_collect(fetch_fn: Any, label: str, source_key: str) -> list[dict] | None:
        """Try REST API first; return None on failure so caller can fall back."""
        try:
            print(f"[PREFLIGHT] Attempting REST API for {label}...", flush=True)
            rows = fetch_fn()
            if rows:
                _actual_source[source_key] = "rest_api"
                print(f"[PREFLIGHT] ✓ REST API returned {len(rows)} {label}", flush=True)
                return rows
            print(f"[PREFLIGHT] REST API returned 0 {label}, falling back to Spark", flush=True)
        except Exception as exc:
            print(f"[PREFLIGHT] REST API failed for {label}: {type(exc).__name__}: {exc}", flush=True)
        return None

    def _spark_collect(fetch_fn: Any, label: str, source_key: str) -> list[dict]:
        _actual_source[source_key] = "spark"
        rows, err = _collect_or_empty(fetch_fn, label)
        if err:
            _collection_errors[source_key] = err
        return rows

    # Columns & routines: prefetch → REST API → Spark SQL
    # Tags: prefetch → Spark SQL (no REST API for table_tags)
    if genie_table_refs:
        uc_columns_dicts = (
            _usable_prefetch("uc_columns")
            or _rest_collect(
                lambda: get_columns_for_tables_rest(w, genie_table_refs),
                "columns (genie tables)", "uc_columns",
            )
            or _spark_collect(
                lambda: get_columns_for_tables(spark, genie_table_refs),
                "columns (genie tables)", "uc_columns",
            )
        )
        uc_tags_dicts = (
            _usable_prefetch("uc_tags")
            or _spark_collect(
                lambda: get_tags_for_tables(spark, genie_table_refs),
                "tags (genie tables)", "uc_tags",
            )
        )
        uc_routines_dicts = (
            _usable_prefetch("uc_routines")
            or _rest_collect(
                lambda: get_routines_for_schemas_rest(w, genie_table_refs),
                "routines (genie schemas)", "uc_routines",
            )
            or _spark_collect(
                lambda: get_routines_for_schemas(spark, genie_table_refs),
                "routines (genie schemas)", "uc_routines",
            )
        )
    else:
        uc_columns_dicts = (
            _usable_prefetch("uc_columns")
            or _spark_collect(lambda: get_columns(spark, catalog, schema), "columns", "uc_columns")
        )
        uc_tags_dicts = (
            _usable_prefetch("uc_tags")
            or _spark_collect(lambda: get_tags(spark, catalog, schema), "tags", "uc_tags")
        )
        uc_routines_dicts = (
            _usable_prefetch("uc_routines")
            or _spark_collect(lambda: get_routines(spark, catalog, schema), "routines", "uc_routines")
        )

    uc_columns_dicts = uc_columns_dicts if isinstance(uc_columns_dicts, list) else []
    uc_tags_dicts = uc_tags_dicts if isinstance(uc_tags_dicts, list) else []
    uc_routines_dicts = uc_routines_dicts if isinstance(uc_routines_dicts, list) else []

    logger.info(
        "UC metadata: %d columns, %d tags, %d routines",
        len(uc_columns_dicts), len(uc_tags_dicts), len(uc_routines_dicts),
    )
    column_samples: list[str] = []
    for col in uc_columns_dicts[:12]:
        if not isinstance(col, dict):
            continue
        table_name = str(col.get("table_name") or col.get("table") or "").strip()
        col_name = str(col.get("column_name") or col.get("column") or "").strip()
        if table_name and col_name:
            column_samples.append(f"{table_name}.{col_name}")
        elif col_name:
            column_samples.append(col_name)

    tag_samples: list[str] = []
    for tag in uc_tags_dicts[:8]:
        if not isinstance(tag, dict):
            continue
        table_name = str(tag.get("table_name") or "").strip()
        col_name = str(tag.get("column_name") or "").strip()
        tag_name = str(tag.get("tag_name") or tag.get("name") or "").strip()
        tag_value = str(tag.get("tag_value") or tag.get("value") or "").strip()
        target = ".".join(part for part in [table_name, col_name] if part)
        if tag_name:
            tag_samples.append(
                f"{target}: {tag_name}={tag_value}" if target else f"{tag_name}={tag_value}"
            )

    routine_samples: list[str] = []
    for routine in uc_routines_dicts[:8]:
        if not isinstance(routine, dict):
            continue
        routine_name = str(routine.get("routine_name") or routine.get("name") or "").strip()
        if routine_name:
            routine_samples.append(routine_name)

    config["_uc_columns"] = uc_columns_dicts

    referenced_schemas = sorted(
        {f"{c}.{s}" for c, s, _ in genie_table_refs if c and s}
    ) if genie_table_refs else []
    metadata_source = {
        "columns": _actual_source.get("uc_columns", "unknown"),
        "tags": _actual_source.get("uc_tags", "unknown"),
        "routines": _actual_source.get("uc_routines", "unknown"),
    }
    stage_detail: dict[str, Any] = {
        "columns_collected": len(uc_columns_dicts),
        "tags_collected": len(uc_tags_dicts),
        "routines_collected": len(uc_routines_dicts),
        "column_samples": [s for s in column_samples if s],
        "tag_samples": [s for s in tag_samples if s],
        "routine_samples": [s for s in routine_samples if s],
        "table_ref_count": len(genie_table_refs),
        "referenced_schema_count": len(referenced_schemas),
        "referenced_schemas": referenced_schemas[:12],
        "collection_scope": "genie_assets" if genie_table_refs else "catalog_schema_fallback",
        "metadata_source": metadata_source,
    }
    if _collection_errors:
        stage_detail["collection_errors"] = {
            k: v[:500] for k, v in _collection_errors.items()
        }
    write_stage(
        spark, run_id, "PREFLIGHT_METADATA_COLLECTION", "COMPLETE",
        task_key="preflight", catalog=catalog, schema=schema,
        detail=stage_detail,
    )

    uc_schema = f"{catalog}.{schema}"
    if experiment_name is None:
        experiment_name = _resolve_experiment_path(run_data=run_data, domain=domain, ws=w)
    elif _has_non_email_user_home(experiment_name):
        # Migrate away from service-principal style home paths like /Users/<uuid>/...
        experiment_name = _resolve_experiment_path(run_data=run_data, domain=domain, ws=w)
    _ensure_experiment_parent_dir(w, experiment_name)
    mlflow.set_experiment(experiment_name)
    exp = mlflow.get_experiment_by_name(experiment_name)
    experiment_id = exp.experiment_id if exp else ""
    logger.info("Experiment: %s (id=%s)", experiment_name, experiment_id)

    initial_instructions = _get_general_instructions(config.get("_parsed_space", config))
    if initial_instructions:
        register_instruction_version(
            uc_schema=uc_schema,
            space_id=space_id,
            instruction_text=initial_instructions,
            run_id=run_id,
            lever=0,
            iteration=0,
            accuracy=0.0,
            domain=domain,
        )

    benchmarks = _load_or_generate_benchmarks(
        w, spark, config, uc_columns_dicts, uc_tags_dicts, uc_routines_dicts,
        domain, catalog, schema, uc_schema, run_id,
    )

    MIN_VALID_BENCHMARKS = 5

    validation_results = validate_benchmarks(
        benchmarks, spark, catalog=catalog, gold_schema=schema,
    )
    pre_count = len(benchmarks)
    filtered_benchmarks: list[dict] = []
    invalid_errors: list[str] = []
    rejected_details: list[str] = []
    for benchmark, validation in zip(benchmarks, validation_results):
        if validation.get("valid"):
            benchmark["validation_status"] = "valid"
            benchmark["validation_reason_code"] = benchmark.get("validation_reason_code", "ok")
            benchmark["validation_error"] = None
            filtered_benchmarks.append(benchmark)
        else:
            err = str(validation.get("error") or "").strip()
            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = benchmark.get(
                "validation_reason_code",
                "sql_compile_error",
            )
            benchmark["validation_error"] = err or benchmark.get("validation_error")
            if err:
                invalid_errors.append(err[:200])
            bid = benchmark.get("id", benchmark.get("question_id", "?"))
            bq = benchmark.get("question", "?")[:80]
            logger.warning(
                "BENCHMARK REJECTED: id=%s question='%s' error=%s",
                bid, bq, err[:200],
            )
            rejected_details.append(f"    - {bid}: \"{bq}\" — {err[:120]}")
    benchmarks = filtered_benchmarks
    if len(benchmarks) < pre_count:
        logger.warning(
            "Discarded %d/%d benchmarks that failed validation (sample_errors=%s)",
            pre_count - len(benchmarks),
            pre_count,
            invalid_errors[:5],
        )
        print(
            f"\n-- BENCHMARK VALIDATION " + "-" * 28 + "\n"
            f"  Total benchmarks: {pre_count}\n"
            f"  Valid after validation: {len(benchmarks)}\n"
            f"  Rejected: {pre_count - len(benchmarks)}\n"
            f"  Rejected details:\n"
            + "\n".join(rejected_details[:20]) + "\n"
            + "-" * 52
        )

    if len(benchmarks) < MIN_VALID_BENCHMARKS:
        logger.warning(
            "Only %d valid benchmarks after filtering (min %d). "
            "Re-generating from scratch using Genie space assets.",
            len(benchmarks), MIN_VALID_BENCHMARKS,
        )
        genie_benchmarks_regen = extract_genie_space_benchmarks(
            config, spark, catalog=catalog, schema=schema,
        )
        write_stage(
            spark, run_id, "BENCHMARK_REGENERATION", "STARTED",
            task_key="preflight", catalog=catalog, schema=schema,
            detail={"reason": "too_few_valid_benchmarks", "valid_count": len(benchmarks), "discarded_errors": invalid_errors[:5]},
        )
        benchmarks = generate_benchmarks(
            w, config, uc_columns_dicts, uc_tags_dicts, uc_routines_dicts,
            domain, catalog, schema, spark,
            target_count=TARGET_BENCHMARK_COUNT,
            genie_space_benchmarks=genie_benchmarks_regen,
        )
        write_stage(
            spark, run_id, "BENCHMARK_REGENERATION", "COMPLETE",
            task_key="preflight",
            detail={"regenerated_count": len(benchmarks)},
            catalog=catalog, schema=schema,
        )

    if not benchmarks:
        raise RuntimeError(
            f"All {pre_count} benchmarks failed validation even after regeneration. "
            f"Sample errors: {invalid_errors[:5]}. "
            "Check that the Genie space's referenced tables actually exist."
        )

    create_evaluation_dataset(
        spark, benchmarks, uc_schema, domain,
        space_id=space_id, catalog=catalog, gold_schema=schema,
    )

    prompt_registrations = register_judge_prompts(uc_schema, domain, experiment_name)

    model_id = create_genie_model_version(
        w, space_id, config, iteration=0, domain=domain,
        experiment_name=experiment_name,
        uc_schema=uc_schema,
        uc_columns=uc_columns_dicts,
        uc_tags=uc_tags_dicts,
        uc_routines=uc_routines_dicts,
    )

    _instr_items = (
        config.get("_parsed_space", config)
        .get("instructions", {})
        .get("text_instructions", [])
    )
    _instr_count = sum(
        len(ti.get("content", [])) if isinstance(ti.get("content"), list) else (1 if ti.get("content") else 0)
        for ti in _instr_items
    )
    write_stage(
        spark, run_id, "PREFLIGHT_STARTED", "COMPLETE",
        task_key="preflight",
        detail={
            "table_count": len(genie_table_refs) if genie_table_refs else 0,
            "instruction_count": _instr_count,
            "benchmark_count": len(benchmarks),
            "experiment_name": experiment_name,
            "model_id": model_id,
            "prompt_count": len(prompt_registrations),
        },
        catalog=catalog, schema=schema,
    )

    return config, benchmarks, model_id, experiment_name


def _load_or_generate_benchmarks(
    w: WorkspaceClient,
    spark: SparkSession,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    uc_schema: str,
    run_id: str,
) -> list[dict]:
    """Load existing benchmarks or generate new ones from Genie space + LLM.

    Strategy:
      1. Extract curated benchmarks from the Genie Space config (example_question_sqls,
         sample_questions). These are always included as the authoritative ground truth.
      2. Try loading previously persisted benchmarks from UC dataset.
         If enough exist AND they already include the curated ones, reuse them.
      3. Otherwise, generate synthetic benchmarks via LLM to augment the curated set.
    """
    genie_benchmarks = extract_genie_space_benchmarks(
        config, spark, catalog=catalog, schema=schema,
    )
    curated_with_sql = sum(1 for b in genie_benchmarks if b.get("expected_sql"))
    curated_question_only = sum(1 for b in genie_benchmarks if not b.get("expected_sql"))
    write_stage(
        spark, run_id, "GENIE_BENCHMARK_EXTRACTION", "COMPLETE",
        task_key="preflight", catalog=catalog, schema=schema,
        detail={
            "genie_space_benchmarks": len(genie_benchmarks),
            "with_sql": curated_with_sql,
            "question_only": curated_question_only,
        },
    )

    print(
        f"\n-- BENCHMARK LOADING " + "-" * 31 + "\n"
        f"  Target count: {TARGET_BENCHMARK_COUNT}\n"
        f"  Curated from Genie Space: {len(genie_benchmarks)} "
        f"({curated_with_sql} with SQL, {curated_question_only} question-only)"
    )

    existing = load_benchmarks_from_dataset(spark, uc_schema, domain)
    if existing and len(existing) >= 5:
        validation_results = validate_benchmarks(
            existing, spark, catalog=catalog, gold_schema=schema,
        )
        valid_existing = [
            b for b, v in zip(existing, validation_results)
            if v.get("valid")
        ]
        invalid_existing = [
            (b, v) for b, v in zip(existing, validation_results)
            if not v.get("valid")
        ]
        invalid_count = len(invalid_existing)

        _rejected_lines: list[str] = []
        for b, v in invalid_existing:
            bid = b.get("id", b.get("question_id", "?"))
            bq = b.get("question", "?")[:80]
            berr = str(v.get("error", ""))[:120]
            _rejected_lines.append(f"    - {bid}: \"{bq}\" — {berr}")
            logger.warning(
                "BENCHMARK REJECTED (re-validation): id=%s question='%s' error=%s",
                bid, bq, berr,
            )

        print(
            f"  Existing in UC table: {len(existing)}\n"
            f"  Valid after re-validation: {len(valid_existing)} ({invalid_count} rejected)"
        )
        if _rejected_lines:
            print("  Rejected reasons:\n" + "\n".join(_rejected_lines[:10]))

        if len(valid_existing) >= 5:
            curated_questions = {b.get("question", "").lower().strip() for b in genie_benchmarks}
            existing_questions = {b.get("question", "").lower().strip() for b in valid_existing}
            missing_curated = curated_questions - existing_questions

            print(f"  Missing curated: {len(missing_curated)}")

            if not missing_curated:
                if len(valid_existing) >= TARGET_BENCHMARK_COUNT:
                    print(
                        f"  Decision: REUSE ({len(valid_existing)} valid, target {TARGET_BENCHMARK_COUNT} met)\n"
                        + "-" * 52
                    )
                    logger.info(
                        "Loaded %d valid existing benchmarks from UC dataset (all %d curated included)",
                        len(valid_existing), len(genie_benchmarks),
                    )
                    for benchmark in valid_existing:
                        benchmark.setdefault("provenance", "synthetic")
                        benchmark.setdefault("validation_status", "valid")
                        benchmark.setdefault("validation_reason_code", "ok")
                        benchmark.setdefault("validation_error", None)
                        benchmark.setdefault("correction_source", "")
                    return valid_existing
                else:
                    gap = TARGET_BENCHMARK_COUNT - len(valid_existing)
                    print(
                        f"  Decision: TOP-UP ({len(valid_existing)} valid, "
                        f"need {gap} more to reach target {TARGET_BENCHMARK_COUNT})\n"
                        + "-" * 52
                    )
                    logger.warning(
                        "Only %d valid benchmarks (target %d). Generating %d more to top up.",
                        len(valid_existing), TARGET_BENCHMARK_COUNT, gap,
                    )
                    for benchmark in valid_existing:
                        benchmark.setdefault("provenance", "synthetic")
                        benchmark.setdefault("validation_status", "valid")
                        benchmark.setdefault("validation_reason_code", "ok")
                        benchmark.setdefault("validation_error", None)
                        benchmark.setdefault("correction_source", "")

                    write_stage(
                        spark, run_id, "BENCHMARK_GENERATION", "STARTED",
                        task_key="preflight", catalog=catalog, schema=schema,
                        detail={"reason": "top_up", "existing_valid": len(valid_existing)},
                    )
                    new_benchmarks = generate_benchmarks(
                        w, config, uc_columns, uc_tags, uc_routines,
                        domain, catalog, schema, spark,
                        target_count=TARGET_BENCHMARK_COUNT,
                        genie_space_benchmarks=genie_benchmarks,
                        existing_benchmarks=valid_existing,
                    )
                    write_stage(
                        spark, run_id, "BENCHMARK_GENERATION", "COMPLETE",
                        task_key="preflight",
                        detail={
                            "total_count": len(new_benchmarks),
                            "top_up_reason": f"{len(valid_existing)}<{TARGET_BENCHMARK_COUNT}",
                        },
                        catalog=catalog, schema=schema,
                    )
                    return new_benchmarks

            print(
                f"  Decision: RE-GENERATE (missing {len(missing_curated)} curated questions)\n"
                + "-" * 52
            )
            logger.info(
                "UC dataset has %d valid benchmarks but missing %d curated Genie space questions. "
                "Re-generating to include them.",
                len(valid_existing), len(missing_curated),
            )
        else:
            print(
                f"  Decision: RE-GENERATE (only {len(valid_existing)} valid, need >=5)\n"
                + "-" * 52
            )
            logger.info(
                "Only %d valid benchmarks remain after re-validation (need >=5). "
                "Re-generating from scratch.",
                len(valid_existing),
            )
    else:
        print(
            f"  Existing in UC table: {len(existing) if existing else 0}\n"
            f"  Decision: GENERATE (no sufficient existing benchmarks)\n"
            + "-" * 52
        )

    logger.info(
        "Generating benchmarks: %d curated from Genie space + synthetic to reach %d",
        len(genie_benchmarks), TARGET_BENCHMARK_COUNT,
    )
    write_stage(
        spark, run_id, "BENCHMARK_GENERATION", "STARTED",
        task_key="preflight", catalog=catalog, schema=schema,
    )

    benchmarks = generate_benchmarks(
        w, config, uc_columns, uc_tags, uc_routines,
        domain, catalog, schema, spark,
        target_count=TARGET_BENCHMARK_COUNT,
        genie_space_benchmarks=genie_benchmarks,
    )

    write_stage(
        spark, run_id, "BENCHMARK_GENERATION", "COMPLETE",
        task_key="preflight",
        detail={
            "total_count": len(benchmarks),
            "curated_count": sum(1 for b in benchmarks if b.get("provenance") == "curated"),
            "synthetic_count": sum(1 for b in benchmarks if b.get("provenance") == "synthetic"),
            "auto_corrected_count": sum(1 for b in benchmarks if b.get("provenance") == "auto_corrected"),
            "valid_count": sum(1 for b in benchmarks if b.get("validation_status") == "valid"),
        },
        catalog=catalog, schema=schema,
    )
    return benchmarks
