# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Lever Loop — Training Document
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Task 3 is the **optimization engine** of the Genie Space Optimizer. It iteratively improves a Genie Space by applying targeted metadata patches across **6 levers**, evaluating each change through a **3-gate pattern**, and rolling back any patch that causes regression.
# MAGIC
# MAGIC ## Lever Loop Overview
# MAGIC
# MAGIC The lever loop:
# MAGIC 1. **Reads** baseline scores and thresholds_met from Task 2 (baseline_eval)
# MAGIC 2. **Skips** entirely if baseline already meets all quality thresholds (baseline gate)
# MAGIC 3. **Iterates** through levers 1–6 in order, up to `max_iterations`
# MAGIC 4. For each lever: clusters failures → generates proposals → applies patches → runs 3-gate evaluation → accepts or rolls back
# MAGIC 5. **Publishes** final scores, model_id, iteration counts, and lever outcomes to downstream tasks
# MAGIC
# MAGIC ## The 6 Levers
# MAGIC
# MAGIC | Lever | Name | What It Optimizes | Example Patches | Risk Level |
# MAGIC |-------|------|-------------------|-----------------|------------|
# MAGIC | 1 | **Tables & Columns** | Column descriptions, visibility, aliases | `update_column_description`, `hide_column`, `add_column_description` | Low–Medium |
# MAGIC | 2 | **Metric Views** | MV measures, dimensions, YAML definitions | `update_mv_measure`, `add_mv_dimension`, `update_mv_yaml` | Medium–High |
# MAGIC | 3 | **Table-Valued Functions** | TVF SQL, parameters, signatures | `update_tvf_sql`, `add_tvf_parameter`, `add_tvf` | Medium–High |
# MAGIC | 4 | **Join Specifications** | Table relationships, join columns | `add_join_spec`, `update_join_spec`, `remove_join_spec` | Medium |
# MAGIC | 5 | **Column Discovery** | Example values, value dictionaries, synonyms | `enable_example_values`, `enable_value_dictionary`, `add_column_synonym` | Low |
# MAGIC | 6 | **Genie Space Instructions** | Routing rules, disambiguation, default behaviors | `add_instruction`, `update_instruction` | Low–Medium |
# MAGIC
# MAGIC Levers 1–3 are governed by `apply_mode` (genie_config, uc_artifact, or both). Levers 4–6 always write to Genie Space config.
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
# MAGIC - **Pass condition:** No regression vs. best scores (within `SLICE_GATE_TOLERANCE`)
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
# MAGIC - **Pass condition:** No regression vs. best scores (within `REGRESSION_THRESHOLD`)
# MAGIC - **On failure:** Rollback patches, mark lever as rolled back, continue to next lever
# MAGIC - **On success:** Accept lever, update best_scores/best_model_id, write iteration to Delta
# MAGIC
# MAGIC ### Rollback Mechanism
# MAGIC
# MAGIC When any gate fails, `rollback(apply_log, w, space_id, metadata_snapshot)` reverses all applied patches. The space returns to its pre-lever state. Patches are marked as rolled back in Delta for audit.
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
import traceback
from datetime import datetime, timezone
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import _run_lever_loop

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
# MAGIC - `levers` — list of lever indices to try (default `[1,2,3,4,5,6]`)
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
thresholds_met = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="thresholds_met")
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
# MAGIC ## Running the Lever Loop
# MAGIC
# MAGIC `_run_lever_loop()` orchestrates the full optimization:
# MAGIC - Sets SQL context (`USE CATALOG` / `USE SCHEMA`) before evaluations
# MAGIC - Iterates levers, applies patches, runs 3-gate evaluation
# MAGIC - Rolls back on any gate failure
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

_log(
    "Task values published",
    accuracy=loop_out["accuracy"],
    model_id=loop_out["model_id"],
    iteration_counter=loop_out["iteration_counter"],
)
_banner("Task 3 Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Task 3 (Lever Loop) is the core optimization stage. It:
# MAGIC - Skips when baseline already meets thresholds
# MAGIC - Iterates through 6 levers (Tables & Columns → Metric Views → TVFs → Join Specs → Column Discovery → Instructions)
# MAGIC - Applies patches, evaluates via slice → P0 → full gates, rolls back on regression
# MAGIC - Sets `USE CATALOG` / `USE SCHEMA` before each evaluation to avoid catalog/schema resolution errors
# MAGIC - Publishes scores, model_id, iteration counts, and lever outcomes for finalize and deploy
