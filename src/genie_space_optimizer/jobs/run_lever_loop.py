# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Lever Loop — Training Document
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Task 3 is the **optimization engine** of the Genie Space Optimizer. It iteratively improves a Genie Space by applying targeted metadata patches across **5 levers**, evaluating each change through a **3-gate pattern**, and rolling back any patch that causes regression.
# MAGIC
# MAGIC ## Lever Loop Overview
# MAGIC
# MAGIC The lever loop:
# MAGIC 1. **Reads** baseline scores and thresholds_met from Task 2 (baseline_eval)
# MAGIC 2. **Skips** entirely if baseline already meets all quality thresholds (baseline gate)
# MAGIC 3. **Iterates** through levers 1–5 in order, up to `max_iterations`
# MAGIC 4. For each lever: clusters failures → generates proposals → applies patches → runs 3-gate evaluation → accepts or rolls back
# MAGIC 5. **Publishes** final scores, model_id, iteration counts, and lever outcomes to downstream tasks
# MAGIC
# MAGIC ## The 5 Levers
# MAGIC
# MAGIC | Lever | Name | What It Optimizes | Example Patches | Risk Level |
# MAGIC |-------|------|-------------------|-----------------|------------|
# MAGIC | 1 | **Tables & Columns** | Column descriptions, visibility, aliases, synonyms | `update_column_description`, `hide_column`, `add_column_synonym` | Low–Medium |
# MAGIC | 2 | **Metric Views** | MV measures, dimensions, YAML definitions | `update_mv_measure`, `add_mv_dimension`, `update_mv_yaml` | Medium–High |
# MAGIC | 3 | **Table-Valued Functions** | TVF SQL, parameters, signatures | `update_tvf_sql`, `add_tvf_parameter`, `add_tvf` | Medium–High |
# MAGIC | 4 | **Join Specifications** | Table relationships, join columns (reactive + column-name discovery) | `add_join_spec`, `update_join_spec`, `remove_join_spec` | Medium |
# MAGIC | 5 | **Genie Space Instructions** | Routing rules, disambiguation, default behaviors, example SQL queries | `add_example_sql`, `update_example_sql`, `add_instruction`, `update_instruction` | Low–Medium |
# MAGIC
# MAGIC **Lever 5 priority hierarchy:** SQL expressions > example SQL > text instructions. Most Lever 5 failures are routed to `add_example_sql` (preferred), with text instructions used only as a last resort when example SQL cannot address the need.
# MAGIC
# MAGIC Format assistance and entity matching are applied automatically between baseline and the lever loop (Stage 2.5).
# MAGIC Levers 1–3 are governed by `apply_mode` (genie_config, uc_artifact, or both). Levers 4–5 always write to Genie Space config.
# MAGIC
# MAGIC ## Convergence Logic
# MAGIC
# MAGIC The loop exits when:
# MAGIC - **All thresholds met** — every judge (e.g. syntax_validity, schema_accuracy) meets its target
# MAGIC - **Max iterations reached** — `iteration_counter >= max_iterations`
# MAGIC - **All levers exhausted** — no more levers to try
# MAGIC
# MAGIC ## Place in the DAG
# MAGIC
# MAGIC - **Upstream:** Task 1 (preflight), Task 2 (baseline_eval)
# MAGIC - **Downstream:** Task 4 (finalize)
# MAGIC - **Run condition:** Typically `run_if: !thresholds_met` — lever loop runs only when baseline does *not* meet thresholds. This notebook also has an internal baseline gate as a safety check.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture: 3-Gate Pattern and Rollback
# MAGIC
# MAGIC For each lever, after patches are applied, the harness runs **three evaluation gates** in sequence:
# MAGIC
# MAGIC ### 1. Slice Gate
# MAGIC - **Scope:** Only benchmarks that touch the **patched objects** (tables, MVs, TVFs)
# MAGIC - **Purpose:** Quick sanity check — did our changes break the things we touched?
# MAGIC - **Pass condition:** No regression vs. best scores (within noise-floor-adjusted `SLICE_GATE_TOLERANCE`)
# MAGIC - **On failure:** Rollback patches, mark lever as rolled back, continue to next lever
# MAGIC
# MAGIC ### 2. P0 Gate
# MAGIC - **Scope:** Priority P0 benchmarks only (top 3 most critical questions)
# MAGIC - **Purpose:** Never regress on the most important questions
# MAGIC - **Pass condition:** Zero P0 failures
# MAGIC - **On failure:** Rollback patches, mark lever as rolled back, continue to next lever
# MAGIC
# MAGIC ### 3. Full Evaluation
# MAGIC - **Scope:** All benchmarks
# MAGIC - **Purpose:** Final acceptance — does the patch improve or at least maintain overall quality?
# MAGIC - **Pass condition:** No regression vs. best scores (within noise-floor-adjusted `REGRESSION_THRESHOLD`)
# MAGIC - **On failure:** Rollback patches, mark lever as rolled back, continue to next lever
# MAGIC - **On success:** Accept lever, update best_scores/best_model_id, write iteration to Delta, register instruction version snapshot, update reference SQLs
# MAGIC
# MAGIC ### Rollback Mechanism
# MAGIC
# MAGIC When any gate fails, `rollback(apply_log, w, space_id, metadata_snapshot)` reverses all applied patches. The space returns to its pre-lever state. Patches are marked as rolled back in Delta for audit.
# MAGIC
# MAGIC ### Arbiter Benchmark Corrections (Pre-Loop)
# MAGIC
# MAGIC Before iterating levers, the harness runs `_extract_arbiter_actions_from_baseline()` to find baseline evaluation rows where the arbiter verdict was `genie_correct` — meaning Genie's SQL was actually correct and the benchmark's expected SQL was wrong. When the number of `genie_correct` verdicts meets or exceeds `ARBITER_CORRECTION_TRIGGER` (default 3), the harness calls `apply_benchmark_corrections()` to rewrite those benchmarks' `expected_sql` to match Genie's correct SQL. This prevents the optimizer from chasing false failures caused by stale or incorrect gold SQL.
# MAGIC
# MAGIC ### Arbiter Verdict Filtering in Failure Clustering
# MAGIC
# MAGIC During each lever iteration, the harness loads failure rows from the latest evaluation and filters out any rows with an arbiter verdict of `genie_correct` before passing them to `cluster_failures()`. This ensures the optimizer only proposes metadata fixes for questions where Genie is genuinely wrong, not questions where the arbiter determined Genie was already correct.
# MAGIC
# MAGIC ### Reference SQL Tracking
# MAGIC
# MAGIC The harness extracts `reference_sqls` (a mapping of question to SQL) from the baseline iteration. These reference SQLs are passed to every evaluation call (slice, P0, full) for cross-iteration SQL consistency tracking. When a lever is accepted, the reference SQLs are updated with the new evaluation results. This enables repeatability scoring to detect when Genie's SQL output changes between iterations.
# MAGIC
# MAGIC ### Noise Floor Adjustment for Gate Tolerances
# MAGIC
# MAGIC To prevent false regression signals on small benchmark sets, the harness computes a noise floor: `noise_floor = 100.0 / max(len(benchmarks), 1)`. This represents the score impact of a single question flip. Gate tolerances are adjusted:
# MAGIC - **Slice gate:** `effective_tolerance = max(SLICE_GATE_TOLERANCE, noise_floor + 2.0)`
# MAGIC - **Full eval:** `effective_threshold = max(REGRESSION_THRESHOLD, noise_floor)`
# MAGIC
# MAGIC For example, with 10 benchmarks the noise floor is 10.0%, so the full eval threshold becomes `max(10.0, 10.0) = 10.0%` instead of the default 10.0%, while the slice gate tolerance becomes `max(15.0, 12.0) = 15.0%`.
# MAGIC
# MAGIC ### SQL Context (USE CATALOG / USE SCHEMA)
# MAGIC
# MAGIC Before each evaluation (slice, P0, full), the harness calls `_ensure_sql_context(spark, catalog, schema)` which runs:
# MAGIC
# MAGIC ```sql
# MAGIC USE CATALOG `{catalog}`;
# MAGIC USE SCHEMA `{schema}`;
# MAGIC ```
# MAGIC
# MAGIC This ensures Spark SQL and MLflow evaluation resolve table references (e.g. in benchmark `expected_sql`) against the correct catalog/schema. Without this, you may see errors like "target schema not in current catalog" when the default session catalog differs from the Genie Space's Unity Catalog location.
# MAGIC
# MAGIC ### What `thresholds_met` Means
# MAGIC
# MAGIC `thresholds_met` is a boolean from Task 2 (baseline_eval). It is `True` when `all_thresholds_met(scores, DEFAULT_THRESHOLDS)` returns `True` — i.e., every quality dimension (syntax_validity, schema_accuracy, logical_accuracy, etc.) meets its target threshold. When `thresholds_met` is True, the lever loop is skipped because no optimization is needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline Gate: Skip If Thresholds Already Met
# MAGIC
# MAGIC Even if this task runs (e.g. due to job configuration), we check `thresholds_met` at the start. If the baseline already meets all quality thresholds, we:
# MAGIC - Publish baseline scores, accuracy, model_id as the final result
# MAGIC - Set `iteration_counter=0`, `skipped=True`
# MAGIC - Exit early with `dbutils.notebook.exit("SKIPPED: baseline meets thresholds")`
# MAGIC
# MAGIC This avoids unnecessary lever iterations when the space is already good enough.

# COMMAND ----------

import json
import time
import traceback
from datetime import datetime, timezone
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.common.config import (
    ENABLE_PROMPT_MATCHING_AUTO_APPLY,
    PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS,
    PROPAGATION_WAIT_SECONDS,
)
from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.common.uc_metadata import (
    extract_genie_space_table_refs,
    get_columns_for_tables_rest,
)
from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import (
    _run_lever_loop,
    _run_prompt_matching_setup,
)

dbutils = cast(Any, globals().get("dbutils"))


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _banner(title: str) -> None:
    print("\n" + "=" * 120)
    print(f"[{_ts()}] [TASK-3 LEVER_LOOP] {title}")
    print("=" * 120)


def _log(event: str, **payload) -> None:
    print(f"[{_ts()}] [TASK-3 LEVER_LOOP] {event}")
    if payload:
        print(json.dumps(payload, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Upstream Task Values
# MAGIC
# MAGIC Task 3 reads from two upstream tasks:
# MAGIC
# MAGIC **From preflight:**
# MAGIC - `run_id`, `space_id`, `domain`, `catalog`, `schema` — run and UC context
# MAGIC - `experiment_name` — MLflow experiment for evaluations
# MAGIC - `max_iterations` — cap on lever loop iterations
# MAGIC - `levers` — list of lever indices to try (default `[1,2,3,4,5]`)
# MAGIC - `apply_mode` — where levers 1–3 write: `genie_config`, `uc_artifact`, or `both`
# MAGIC
# MAGIC **From baseline_eval:**
# MAGIC - `scores` — per-judge scores (JSON)
# MAGIC - `overall_accuracy` — aggregate accuracy
# MAGIC - `thresholds_met` — True if baseline already meets all quality thresholds
# MAGIC - `model_id` — Genie model version ID for iteration 0

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

# Read task values from upstream
run_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="run_id")
space_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="space_id")
domain = dbutils.jobs.taskValues.get(taskKey="preflight", key="domain")
catalog = dbutils.jobs.taskValues.get(taskKey="preflight", key="catalog")
schema = dbutils.jobs.taskValues.get(taskKey="preflight", key="schema")
exp_name = dbutils.jobs.taskValues.get(taskKey="preflight", key="experiment_name")
max_iterations = int(dbutils.jobs.taskValues.get(taskKey="preflight", key="max_iterations"))
levers = json.loads(dbutils.jobs.taskValues.get(taskKey="preflight", key="levers"))
apply_mode = dbutils.jobs.taskValues.get(taskKey="preflight", key="apply_mode")

scores_json = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="scores")
prev_scores = json.loads(scores_json)
prev_accuracy = float(dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="overall_accuracy"))
thresholds_met_raw = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="thresholds_met")
thresholds_met = str(thresholds_met_raw).lower() in ("true", "1")
prev_model_id = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="model_id")

_banner("Resolved Upstream Task Values")
_log(
    "Inputs",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    experiment_name=exp_name,
    max_iterations=max_iterations,
    levers=levers,
    apply_mode=apply_mode,
    baseline_accuracy=prev_accuracy,
    baseline_thresholds_met=thresholds_met,
    baseline_model_id=prev_model_id,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline Gate Check
# MAGIC
# MAGIC If `thresholds_met` is True, we skip the lever loop and publish baseline values as the final result. Downstream tasks (finalize, deploy) receive the same structure whether we skipped or ran the loop.

# COMMAND ----------

if thresholds_met:
    _banner("Baseline Gate: SKIP Lever Loop")
    _log("Skip reason", reason="baseline_meets_thresholds", baseline_accuracy=prev_accuracy)
    dbutils.jobs.taskValues.set(key="scores", value=scores_json)
    dbutils.jobs.taskValues.set(key="accuracy", value=prev_accuracy)
    dbutils.jobs.taskValues.set(key="model_id", value=prev_model_id)
    dbutils.jobs.taskValues.set(key="iteration_counter", value=0)
    dbutils.jobs.taskValues.set(key="skipped", value=True)
    dbutils.notebook.exit("SKIPPED: baseline meets thresholds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Benchmarks and Config
# MAGIC
# MAGIC - **Benchmarks:** Loaded from the MLflow evaluation dataset `{catalog}.{schema}.genie_benchmarks_{domain}`. These are the gold questions used for slice, P0, and full evaluation.
# MAGIC - **Config:** Fetched via `fetch_space_config()` — the Genie Space configuration including tables, instructions, join_specs, column_configs, etc. The harness uses this to apply patches and roll back.

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
config = fetch_space_config(w, space_id)
_banner("Loaded Runtime Inputs")
_log(
    "Dataset/config loaded",
    uc_schema=uc_schema,
    benchmark_count=len(benchmarks),
    config_keys=sorted(list(config.keys()))[:20] if isinstance(config, dict) else [],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2.5: Prompt Matching Auto-Config
# MAGIC
# MAGIC Before the lever loop, apply format assistance and entity matching as a best-practice hygiene step.
# MAGIC This enables `enable_format_assistance` on all visible columns and `enable_entity_matching` on prioritized
# MAGIC STRING columns (up to the 120-column cap), skipping metric view measure columns.
# MAGIC
# MAGIC After applying changes, a propagation wait allows the Genie Space to rebuild its index:
# MAGIC - **With entity matching changes:** `PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS` (default 90s) — extended wait for value dictionary rebuild
# MAGIC - **Without entity matching changes:** `PROPAGATION_WAIT_SECONDS` (default 30s)
# MAGIC
# MAGIC The same two-tier propagation wait applies per-lever in the lever loop when patches include value dictionary changes.

# COMMAND ----------

if ENABLE_PROMPT_MATCHING_AUTO_APPLY:
    _banner("Stage 2.5: Prompt Matching Auto-Config")
    try:
        table_refs = extract_genie_space_table_refs(config)
        uc_columns = get_columns_for_tables_rest(w, table_refs) if table_refs else []
        config["_uc_columns"] = uc_columns

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
            f"  STAGE 2.5: Prompt Matching Auto-Config\n"
            f"{'=' * 62}\n"
            f"\n-- GENIE SPACE INVENTORY " + "-" * 27 + "\n"
            f"  Tables: {len(_tables)} ({', '.join(t.get('name', t.get('identifier', '?')) for t in _tables[:10])})\n"
            f"  Total columns: {_total_cols} (visible: {_visible_cols}, hidden: {_hidden_cols})\n"
            f"  UC column metadata: {len(uc_columns)} columns fetched across {len(table_refs)} tables\n"
            f"  STRING columns eligible for entity matching: {_string_cols}\n"
            f"  Columns already with format assistance: {_fa_existing}\n"
            f"  Columns already with value dictionary: {_vd_existing} of 120 max slots\n"
            + "-" * 52
        )

        pm_result = _run_prompt_matching_setup(w, spark, run_id, space_id, config, catalog, schema)
        _log(
            "Prompt matching complete",
            format_assistance=pm_result.get("format_assistance_count", 0),
            entity_matching=pm_result.get("entity_matching_count", 0),
            total_changes=pm_result.get("total_changes", 0),
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
                f"  Entity matching changes: {pm_result.get('entity_matching_count', 0)}\n"
                f"  Wait time: {wait_time}s"
                + (" (extended for value dictionary rebuild)" if has_entity_matching else "")
                + "\n" + "-" * 52
            )
            time.sleep(wait_time)
            config = fetch_space_config(w, space_id)
            _log("Config refreshed after prompt matching")
    except Exception as exc:
        _banner("Prompt Matching FAILED (non-fatal, continuing to lever loop)")
        _log("Prompt matching error", error_type=type(exc).__name__, error_message=str(exc))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running the Lever Loop
# MAGIC
# MAGIC `_run_lever_loop()` orchestrates the full optimization:
# MAGIC - Extracts arbiter corrections from baseline and applies benchmark rewrites when threshold is met
# MAGIC - Filters `genie_correct` arbiter verdicts from failure rows before clustering
# MAGIC - Tracks reference SQLs from baseline for cross-iteration consistency scoring
# MAGIC - Sets SQL context (`USE CATALOG` / `USE SCHEMA`) before evaluations
# MAGIC - Iterates levers, applies patches, runs 3-gate evaluation (slice → P0 → full) with noise-floor-adjusted tolerances
# MAGIC - Rolls back on any gate failure
# MAGIC - On lever acceptance: updates best scores, registers instruction version snapshot, refreshes reference SQLs
# MAGIC - Supports resume from Delta if the task retries mid-loop
# MAGIC
# MAGIC Returns: `scores`, `accuracy`, `model_id`, `iteration_counter`, `best_iteration`, `levers_attempted`, `levers_accepted`, `levers_rolled_back`.

# COMMAND ----------

try:
    _banner("Running _run_lever_loop")
    loop_out = _run_lever_loop(
        w, spark, run_id, space_id, domain, benchmarks, exp_name,
        prev_scores, prev_accuracy, prev_model_id, config,
        catalog, schema, levers, max_iterations,
        apply_mode=apply_mode,
    )
    _log(
        "Lever loop finished",
        accuracy=loop_out["accuracy"],
        iteration_counter=loop_out["iteration_counter"],
        best_iteration=loop_out["best_iteration"],
        levers_attempted=loop_out["levers_attempted"],
        levers_accepted=loop_out["levers_accepted"],
        levers_rolled_back=loop_out["levers_rolled_back"],
    )
except Exception as exc:
    _banner("Lever Loop FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publishing Task Values
# MAGIC
# MAGIC Task 4 (finalize) and Task 5 (deploy) consume these values. The keys must match what downstream notebooks expect.

# COMMAND ----------

_banner("Publishing Task Values")
dbutils.jobs.taskValues.set(key="scores", value=json.dumps(loop_out["scores"]))
dbutils.jobs.taskValues.set(key="accuracy", value=loop_out["accuracy"])
dbutils.jobs.taskValues.set(key="model_id", value=loop_out["model_id"])
dbutils.jobs.taskValues.set(key="iteration_counter", value=loop_out["iteration_counter"])
dbutils.jobs.taskValues.set(key="best_iteration", value=loop_out["best_iteration"])
dbutils.jobs.taskValues.set(key="skipped", value=False)

debug_info = {
    k: v for k, v in loop_out.items()
    if k.startswith("_debug_") or k in ("levers_attempted", "levers_accepted", "levers_rolled_back", "iteration_counter")
}
dbutils.jobs.taskValues.set(key="debug_info", value=json.dumps(debug_info, default=str))

_log(
    "Task values published",
    accuracy=loop_out["accuracy"],
    model_id=loop_out["model_id"],
    iteration_counter=loop_out["iteration_counter"],
    debug_info=debug_info,
)
_banner("Task 3 Completed")
dbutils.notebook.exit(json.dumps(debug_info, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Task 3 (Lever Loop) is the core optimization stage. It:
# MAGIC - Skips when baseline already meets thresholds
# MAGIC - Applies prompt matching (format assistance + entity matching) as a best-practice hygiene step (Stage 2.5), with extended propagation wait for entity matching changes
# MAGIC - Applies arbiter benchmark corrections when `genie_correct` verdicts exceed the trigger threshold
# MAGIC - Filters `genie_correct` arbiter verdicts from failure rows before clustering to avoid chasing false failures
# MAGIC - Tracks reference SQLs from baseline for cross-iteration consistency scoring
# MAGIC - Iterates through 5 levers (Tables & Columns → Metric Views → TVFs → Join Specs → Instructions)
# MAGIC - Lever 5 prioritizes example SQL over text instructions (SQL expressions > example SQL > text)
# MAGIC - Applies patches, evaluates via slice → P0 → full gates with noise-floor-adjusted tolerances, rolls back on regression
# MAGIC - On lever acceptance: registers instruction version snapshot and updates reference SQLs
# MAGIC - Sets `USE CATALOG` / `USE SCHEMA` before each evaluation to avoid catalog/schema resolution errors
# MAGIC - Publishes scores, model_id, iteration counts, and lever outcomes for finalize and deploy
