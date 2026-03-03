# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Lever Loop — Training Guide
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 3 of 5 — Lever Loop (Optimization Engine) |
# MAGIC | **Harness functions** | `_prepare_lever_loop()` + `_run_lever_loop()` in `optimization/harness.py` |
# MAGIC | **Reads from** | `preflight` (run context, levers, max_iterations) + `baseline_eval` (scores, thresholds_met, model_id) |
# MAGIC | **Publishes to** | `finalize` (scores, accuracy, model_id, iteration_counter, debug_info) |
# MAGIC | **Typical duration** | 15–90 min (depends on lever count and benchmark size) |
# MAGIC | **Log label** | `[TASK-3 LEVER_LOOP]` |
# MAGIC
# MAGIC ## 🎯 Purpose
# MAGIC
# MAGIC Task 3 is the **optimization engine** of the Genie Space Optimizer. It iteratively improves a Genie Space by applying targeted metadata patches across **5 levers**, evaluating each change through a **3-gate pattern**, and rolling back any patch that causes regression.
# MAGIC
# MAGIC ## 🏗️ DAG Position
# MAGIC
# MAGIC | Step | Task | Status | Reads From | Publishes To |
# MAGIC |:----:|------|:------:|------------|--------------|
# MAGIC | 1 | preflight | Done | widgets | all tasks |
# MAGIC | 2 | baseline_eval | Done | preflight | lever_loop |
# MAGIC | 3 | **lever_loop** | **⬅️ THIS TASK** | preflight + baseline | finalize |
# MAGIC | 4 | finalize | Next | lever_loop | deploy |
# MAGIC | 5 | deploy | Pending | preflight + finalize | *(terminal)* |
# MAGIC
# MAGIC > **📝 Note:** This task runs only when baseline does *not* meet thresholds (`run_if: !thresholds_met`). An internal baseline gate provides a safety check even if the job configuration allows the task to run.
# MAGIC
# MAGIC ## The 5 Levers
# MAGIC
# MAGIC | Lever | Name | What It Optimizes | Example Patches | Risk |
# MAGIC |:-----:|------|-------------------|-----------------|:----:|
# MAGIC | 1 | **Tables & Columns** | Column descriptions, visibility, aliases, synonyms | `update_column_description`, `hide_column`, `add_column_synonym` | 🔵 Low–Med |
# MAGIC | 2 | **Metric Views** | MV measures, dimensions, YAML definitions | `update_mv_measure`, `add_mv_dimension`, `update_mv_yaml` | 🟠 Med–High |
# MAGIC | 3 | **Table-Valued Functions** | TVF SQL, parameters, signatures | `update_tvf_sql`, `add_tvf_parameter`, `add_tvf` | 🟠 Med–High |
# MAGIC | 4 | **Join Specifications** | Table relationships, join columns (reactive + column-name discovery) | `add_join_spec`, `update_join_spec`, `remove_join_spec` | 🔵 Medium |
# MAGIC | 5 | **Genie Space Instructions** | Routing rules, disambiguation, default behaviors, example SQL queries | `add_example_sql`, `update_example_sql`, `add_instruction`, `update_instruction` | 🔵 Low–Med |
# MAGIC
# MAGIC > **📝 Note:** Lever 5 priority hierarchy: SQL expressions > example SQL > text instructions. Most Lever 5 failures are routed to `add_example_sql` (preferred), with text instructions used only as a last resort.
# MAGIC
# MAGIC > **📝 Note:** Format assistance and entity matching are applied automatically between baseline and the lever loop (Stage 2.5). Levers 1–3 are governed by `apply_mode` (genie_config, uc_artifact, or both). Levers 4–5 always write to Genie Space config.
# MAGIC
# MAGIC ## Convergence Logic
# MAGIC
# MAGIC The loop exits when:
# MAGIC - **All thresholds met** — every judge (e.g. syntax_validity, schema_accuracy) meets its target
# MAGIC - **Max iterations reached** — `iteration_counter >= max_iterations`
# MAGIC - **All levers exhausted** — no more levers to try

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Architecture: 3-Gate Pattern and Rollback
# MAGIC
# MAGIC For each lever, after patches are applied, the harness runs **three evaluation gates** in sequence:
# MAGIC
# MAGIC | Gate | Scope | Pass Condition | On Failure |
# MAGIC |:----:|-------|---------------|------------|
# MAGIC | **1. Slice** | Only benchmarks touching **patched objects** | No regression vs. best scores (noise-floor-adjusted `SLICE_GATE_TOLERANCE`) | Rollback → next lever |
# MAGIC | **2. P0** | Top 3 most critical questions | Zero P0 failures | Rollback → next lever |
# MAGIC | **3. Full** | All benchmarks | No regression vs. best scores (noise-floor-adjusted `REGRESSION_THRESHOLD`) | Rollback → next lever |
# MAGIC
# MAGIC **On full-gate success:** Accept lever → update best_scores/best_model_id → write iteration to Delta → register instruction version snapshot → update reference SQLs.
# MAGIC
# MAGIC ### Rollback Mechanism
# MAGIC
# MAGIC When any gate fails, `rollback(apply_log, w, space_id, metadata_snapshot)` reverses all applied patches. The space returns to its pre-lever state. Patches are marked as rolled back in Delta for audit.
# MAGIC
# MAGIC ### Arbiter Benchmark Corrections (Pre-Loop)
# MAGIC
# MAGIC > **📝 Note:** Before iterating levers, the harness runs `_extract_arbiter_actions_from_baseline()` to find rows where the arbiter verdict was `genie_correct` — meaning Genie's SQL was actually correct and the benchmark's expected SQL was wrong. When `genie_correct` verdicts meet or exceed `ARBITER_CORRECTION_TRIGGER` (default 3), `apply_benchmark_corrections()` rewrites those benchmarks' `expected_sql`. This prevents chasing false failures caused by stale gold SQL.
# MAGIC
# MAGIC ### Arbiter Verdict Filtering in Failure Clustering
# MAGIC
# MAGIC During each lever iteration, the harness filters out rows with arbiter verdict `genie_correct` before passing them to `cluster_failures()`. The optimizer only proposes fixes for questions where Genie is genuinely wrong.
# MAGIC
# MAGIC ### Reference SQL Tracking
# MAGIC
# MAGIC The harness extracts `reference_sqls` (question → SQL mapping) from the baseline iteration. These are passed to every evaluation call for cross-iteration SQL consistency tracking. When a lever is accepted, reference SQLs are updated with new results.
# MAGIC
# MAGIC ### Noise Floor Adjustment for Gate Tolerances
# MAGIC
# MAGIC > **💡 Tip:** To prevent false regression signals on small benchmark sets, the harness computes: `noise_floor = 100.0 / max(len(benchmarks), 1)`. Gate tolerances are adjusted:
# MAGIC > - **Slice gate:** `effective_tolerance = max(SLICE_GATE_TOLERANCE, noise_floor + 2.0)`
# MAGIC > - **Full eval:** `effective_threshold = max(REGRESSION_THRESHOLD, noise_floor)`
# MAGIC >
# MAGIC > Example: 10 benchmarks → noise floor 10.0% → full eval threshold = `max(10.0, 10.0) = 10.0%`, slice gate = `max(15.0, 12.0) = 15.0%`.
# MAGIC
# MAGIC ### SQL Context (USE CATALOG / USE SCHEMA)
# MAGIC
# MAGIC Before each evaluation (slice, P0, full), the harness calls `_ensure_sql_context(spark, catalog, schema)`:
# MAGIC
# MAGIC ```sql
# MAGIC USE CATALOG `{catalog}`;
# MAGIC USE SCHEMA `{schema}`;
# MAGIC ```
# MAGIC
# MAGIC > **⚠️ Warning:** Without this, you may see "target schema not in current catalog" errors when the default session catalog differs from the Genie Space's UC location.
# MAGIC
# MAGIC ### What `thresholds_met` Means
# MAGIC
# MAGIC `thresholds_met` is a boolean from Task 2 (baseline_eval). It is `True` when `all_thresholds_met(scores, DEFAULT_THRESHOLDS)` returns `True` — i.e., every quality dimension meets its target threshold. When `True`, the lever loop is skipped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚪 Baseline Gate: Skip If Thresholds Already Met
# MAGIC
# MAGIC > **📝 Note:** Even if this task runs (e.g. due to job configuration), we check `thresholds_met` at the start. If the baseline already meets all quality thresholds, we publish baseline values as-is and exit early — no lever iterations needed.
# MAGIC
# MAGIC On skip:
# MAGIC - Publish baseline scores, accuracy, model_id as the final result
# MAGIC - Set `iteration_counter=0`, `skipped=True`
# MAGIC - Exit with `dbutils.notebook.exit("SKIPPED: baseline meets thresholds")`

# COMMAND ----------

import json
import traceback
from functools import partial
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import (
    _prepare_lever_loop,
    _run_lever_loop,
)

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-3 LEVER_LOOP"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Reading Upstream Task Values
# MAGIC
# MAGIC Task 3 reads from two upstream tasks:
# MAGIC
# MAGIC | Source | Keys | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | `preflight` | `run_id`, `space_id`, `domain`, `catalog`, `schema` | Run and UC context |
# MAGIC | `preflight` | `experiment_name` | MLflow experiment for evaluations |
# MAGIC | `preflight` | `max_iterations` | Cap on lever loop iterations |
# MAGIC | `preflight` | `levers` | Lever indices to try (default `[1,2,3,4,5]`) |
# MAGIC | `preflight` | `apply_mode` | Where levers 1–3 write: `genie_config`, `uc_artifact`, or `both` |
# MAGIC | `baseline_eval` | `scores` | Per-judge scores (JSON) |
# MAGIC | `baseline_eval` | `overall_accuracy` | Aggregate accuracy |
# MAGIC | `baseline_eval` | `thresholds_met` | `True` if baseline already meets all quality thresholds |
# MAGIC | `baseline_eval` | `model_id` | Genie model version ID for iteration 0 |

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

triggered_by = dbutils.jobs.taskValues.get(taskKey="preflight", key="triggered_by", default="")

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
    triggered_by=triggered_by,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚪 Baseline Gate Check
# MAGIC
# MAGIC > **📝 Note:** If `thresholds_met` is `True`, we skip the lever loop and publish baseline values as the final result. Downstream tasks (finalize, deploy) receive the same structure whether we skipped or ran the loop.

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
# MAGIC ## 🔄 Loading Benchmarks and Preparing Config
# MAGIC
# MAGIC - **Benchmarks:** Loaded from `{catalog}.{schema}.genie_benchmarks_{domain}`.
# MAGIC - **Config:** `_prepare_lever_loop()` handles config loading from Delta snapshot (API fetch fallback), UC column metadata enrichment, Stage 2.5 prompt matching auto-config (format assistance + entity matching), entity-matching-aware propagation wait, and post-wait config refresh.
# MAGIC
# MAGIC > **📝 Note:** All business logic lives in the harness — this notebook is a thin wrapper.

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
_banner("Loaded Benchmarks")
_log("Benchmark dataset", uc_schema=uc_schema, domain=domain, benchmark_count=len(benchmarks))
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Prepare Config (Stage 2.5 included)
# MAGIC
# MAGIC `_prepare_lever_loop()` is the single unified function that:
# MAGIC
# MAGIC | Step | Action | Fallback |
# MAGIC |:----:|--------|----------|
# MAGIC | 1 | Load config from Delta snapshot | API fetch fallback |
# MAGIC | 2 | Fetch UC column metadata via REST API | Non-fatal: continues if fails |
# MAGIC | 3 | Run Stage 2.5 prompt matching auto-config (format assistance + entity matching) | Non-fatal: continues if fails |
# MAGIC | 4 | Apply entity-matching-aware propagation wait (extended for value dictionary rebuild) | — |
# MAGIC | 5 | Refresh config from API after wait | — |
# MAGIC
# MAGIC > **📝 Note:** Steps 2–3 are non-fatal: if UC metadata or prompt matching fails, the lever loop proceeds anyway.

# COMMAND ----------

_banner("Preparing Lever Loop Config (Stage 2.5)")
config = _prepare_lever_loop(w, spark, run_id, space_id, catalog, schema)
_log(
    "Config prepared",
    config_keys=sorted(list(config.keys()))[:20] if isinstance(config, dict) else [],
    uc_columns_count=len(config.get("_uc_columns", [])),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Running the Lever Loop
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
        triggered_by=triggered_by,
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
# MAGIC ## 📤 Publishing Task Values
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
# MAGIC ## ✅ What Success Looks Like
# MAGIC
# MAGIC When this task completes successfully (levers accepted), you will see output similar to:
# MAGIC
# MAGIC ```
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-3 LEVER_LOOP] Running _run_lever_loop
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 10:30:00 UTC] [TASK-3 LEVER_LOOP] Lever 1 (Tables & Columns): 4 patches applied
# MAGIC [2026-02-28 10:32:15 UTC] [TASK-3 LEVER_LOOP] Slice gate: PASSED (delta +2.5%)
# MAGIC [2026-02-28 10:33:00 UTC] [TASK-3 LEVER_LOOP] P0 gate: PASSED (0 failures)
# MAGIC [2026-02-28 10:38:00 UTC] [TASK-3 LEVER_LOOP] Full eval: PASSED (accuracy 78.2% → 81.0%)
# MAGIC [2026-02-28 10:38:01 UTC] [TASK-3 LEVER_LOOP] Lever 1: ACCEPTED
# MAGIC [2026-02-28 10:45:00 UTC] [TASK-3 LEVER_LOOP] Lever 2 (Metric Views): 2 patches applied
# MAGIC [2026-02-28 10:47:00 UTC] [TASK-3 LEVER_LOOP] Slice gate: FAILED (regression -3.5%)
# MAGIC [2026-02-28 10:47:01 UTC] [TASK-3 LEVER_LOOP] Lever 2: ROLLED BACK
# MAGIC ...
# MAGIC [2026-02-28 11:15:00 UTC] [TASK-3 LEVER_LOOP] Lever loop finished
# MAGIC   {"accuracy": 85.0, "iteration_counter": 3, "levers_accepted": 2, "levers_rolled_back": 3}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-3 LEVER_LOOP] Task 3 Completed
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC ```
# MAGIC
# MAGIC When the baseline gate triggers a skip:
# MAGIC
# MAGIC ```
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-3 LEVER_LOOP] Baseline Gate: SKIP Lever Loop
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 10:20:00 UTC] [TASK-3 LEVER_LOOP] Skip reason
# MAGIC   {"reason": "baseline_meets_thresholds", "baseline_accuracy": 95.2}
# MAGIC ```
# MAGIC
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC Task 3 (Lever Loop) is a **thin wrapper** around two harness functions:
# MAGIC
# MAGIC 1. **`_prepare_lever_loop()`** — loads config from Delta snapshot, enriches UC column metadata,
# MAGIC    runs Stage 2.5 prompt matching (format assistance + entity matching) with entity-matching-aware
# MAGIC    propagation wait, and refreshes config from the API.
# MAGIC 2. **`_run_lever_loop()`** — the core optimization engine that iterates through 5 levers,
# MAGIC    applies patches via 3-gate evaluation (slice → P0 → full), and rolls back on regression.
# MAGIC
# MAGIC All business logic lives in the harness (`optimization/harness.py`), shared identically by
# MAGIC the DAG notebooks and the `optimize_genie_space()` convenience function.
