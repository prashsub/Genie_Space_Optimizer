# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1: Preflight — Training Guide
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | 1 of 5 — Preflight |
# MAGIC | **Harness function** | `_run_preflight()` → `run_preflight()` in `optimization/preflight.py` |
# MAGIC | **Reads from** | Job widgets (set by app backend) |
# MAGIC | **Publishes to** | All downstream tasks (baseline, lever_loop, finalize, deploy) |
# MAGIC | **Typical duration** | 2–10 min |
# MAGIC | **Log label** | `[TASK-1 PREFLIGHT]` |
# MAGIC
# MAGIC ## 🎯 Purpose
# MAGIC
# MAGIC **Preflight** is the first stage of the Genie Space Optimizer's 5-task DAG. It runs *before* any optimization iterations and prepares everything downstream tasks need:
# MAGIC
# MAGIC | Responsibility | Why It Matters |
# MAGIC |----------------|----------------|
# MAGIC | **Validate orchestration parameters** | Ensures `run_id`, `space_id`, `catalog`, `schema`, etc. are present and sane before any work begins |
# MAGIC | **Ensure Delta state tables exist** | Creates 7 Delta tables (`genie_opt_runs`, `genie_opt_stages`, `genie_opt_iterations`, `genie_opt_patches`, `genie_eval_asi_results`, `genie_opt_data_access_grants`, `genie_opt_provenance`) if missing; runs column migrations on existing tables |
# MAGIC | **Fetch Genie Space config** | Baseline configuration is needed for lever proposals and rollback |
# MAGIC | **Collect UC metadata** | Columns, tags, routines inform benchmark generation and model context |
# MAGIC | **Load or generate benchmarks** | Evaluation queries that drive accuracy scoring; LLM generates if none exist |
# MAGIC | **Register judge prompts & create iteration 0 model** | MLflow experiment setup and initial LoggedModel for baseline comparison |
# MAGIC
# MAGIC ## ⚠️ What Happens If Preflight Fails?
# MAGIC
# MAGIC > **⚠️ Warning:** If preflight fails, **the entire DAG stops.** Tasks 2–5 (baseline, lever_loop, finalize, deploy) never run.
# MAGIC
# MAGIC - **Run status** is written as `FAILED` in `genie_opt_runs` (via `_safe_stage` in the harness).
# MAGIC - **Error details** are logged and re-raised; the Databricks Job run fails.
# MAGIC
# MAGIC > **💡 Tip:** Check job run logs, `genie_opt_stages` for the last stage/status, and ensure Genie API access, UC permissions, and MLflow experiment paths are valid.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Architecture: DAG Position and Data Flow
# MAGIC
# MAGIC ### DAG Position
# MAGIC
# MAGIC | Step | Task | Status | Reads From | Publishes To |
# MAGIC |:----:|------|:------:|------------|--------------|
# MAGIC | 1 | **preflight** | **⬅️ THIS TASK** | widgets | all tasks |
# MAGIC | 2 | baseline_eval | Next | preflight | lever_loop |
# MAGIC | 3 | lever_loop | Pending | preflight + baseline | finalize |
# MAGIC | 4 | finalize | Pending | lever_loop | deploy |
# MAGIC | 5 | deploy | Pending | preflight + finalize | *(terminal)* |
# MAGIC
# MAGIC ### Task Value Flow
# MAGIC
# MAGIC ```
# MAGIC   ┌─────────────┐
# MAGIC   │  preflight  │  ← Task 1 (this notebook)
# MAGIC   └──────┬──────┘
# MAGIC          │ taskValues: run_id, space_id, catalog, schema, domain, experiment_name,
# MAGIC          │             experiment_id, model_id, benchmark_count, max_iterations,
# MAGIC          │             levers, apply_mode, deploy_target, triggered_by
# MAGIC          ▼
# MAGIC   ┌─────────────┐
# MAGIC   │  baseline   │  Task 2: Run full evaluation on iteration 0
# MAGIC   └──────┬──────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC   ┌─────────────┐
# MAGIC   │ lever_loop  │  Task 3: Iterate levers, propose patches, evaluate
# MAGIC   └──────┬──────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC   ┌─────────────┐
# MAGIC   │  finalize   │  Task 4: Promote best model, generate report
# MAGIC   └──────┬──────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC   ┌─────────────┐
# MAGIC   │   deploy    │  Task 5: Deploy to DABs target (optional)
# MAGIC   └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### How Parameters Flow
# MAGIC
# MAGIC 1. **App backend** (FastAPI) receives a run request and creates a Databricks Job run with **widget values**.
# MAGIC 2. **This notebook** reads widgets via `dbutils.widgets.get(...)` and passes them to `_run_preflight`.
# MAGIC 3. **Task values** (`dbutils.jobs.taskValues.set`) publish outputs to downstream tasks. Each task reads via `dbutils.jobs.taskValues.get(taskKey="preflight", key="run_id")`, etc.
# MAGIC 4. **Delta tables** store durable state (runs, stages, iterations, patches) for resume, reporting, and UI display.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Imports and Helper Functions
# MAGIC
# MAGIC | Import | Purpose |
# MAGIC |--------|---------|
# MAGIC | `json` | Serialize `levers` and other payloads for logging and task values |
# MAGIC | `traceback` | Format full stack traces on failure for debugging |
# MAGIC | `datetime`, `timezone` | UTC timestamps for `_ts()` and consistent logging |
# MAGIC | `WorkspaceClient` | Databricks SDK — Genie API, MLflow, workspace operations |
# MAGIC | `SparkSession` | Delta table creation and SQL execution |
# MAGIC | `_run_preflight` | Harness stage function: config fetch, UC metadata, benchmarks, model creation |
# MAGIC | `ensure_optimization_tables` | Creates the 7 Delta tables if they don't exist; runs column migrations on existing tables |
# MAGIC
# MAGIC ### Helper Functions
# MAGIC
# MAGIC | Function | What It Does |
# MAGIC |----------|---------------|
# MAGIC | `_ts()` | Returns current UTC timestamp string for log prefixes |
# MAGIC | `_banner(title)` | Prints a 120-char separator and `[TASK-1 PREFLIGHT] {title}` for visual section breaks |
# MAGIC | `_log(event, **payload)` | Logs `event` with optional JSON payload; uses `default=str` for non-JSON-serializable values |

# COMMAND ----------

import json
import traceback
from functools import partial
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.common.config import MAX_ITERATIONS
from genie_space_optimizer.jobs._helpers import _banner as _banner_base
from genie_space_optimizer.jobs._helpers import _log as _log_base
from genie_space_optimizer.optimization.harness import _run_preflight
from genie_space_optimizer.optimization.preflight import (
    preflight_collect_uc_metadata,
    preflight_fetch_config,
    preflight_generate_benchmarks,
    preflight_load_human_feedback,
    preflight_setup_experiment,
    preflight_validate_benchmarks,
)
from genie_space_optimizer.optimization.state import ensure_optimization_tables, update_run_status

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-1 PREFLIGHT"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Widget Parameters
# MAGIC
# MAGIC Databricks Jobs pass parameters as **widgets**. Each widget has a default; the app backend overrides them when triggering a run.
# MAGIC
# MAGIC | Widget | Type | Default | Description |
# MAGIC |--------|------|--------|-------------|
# MAGIC | `run_id` | text | `""` | UUID for this optimization run (required) |
# MAGIC | `space_id` | text | `""` | Genie Space ID being optimized |
# MAGIC | `catalog` | text | `""` | Unity Catalog name for state tables |
# MAGIC | `schema` | text | `""` | UC schema for state tables and gold data |
# MAGIC | `domain` | text | `""` | Domain name (e.g. `revenue_property`) for experiment path and prompts |
# MAGIC | `experiment_name` | text | `""` | Optional MLflow experiment path; auto-resolved if empty |
# MAGIC | `max_iterations` | text | `str(MAX_ITERATIONS)` | Max lever iterations before stopping |
# MAGIC | `levers` | text | `"[1,2,3,4,5]"` | JSON array of lever numbers to try |
# MAGIC | `apply_mode` | text | `"genie_config"` | Where patches apply: `genie_config` \| `uc_artifact` \| `both` |
# MAGIC | `deploy_target` | text | `""` | DABs target for post-optimization deploy (optional) |
# MAGIC | `triggered_by` | text | `""` | Identity or context of who/what triggered the run (e.g. user email, app backend) |
# MAGIC
# MAGIC > **⚠️ Warning:** Empty `run_id`, `space_id`, `catalog`, or `schema` will cause downstream failures. The harness does not validate here; failures surface during `_run_preflight`.

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("experiment_name", "")
dbutils.widgets.text("max_iterations", str(MAX_ITERATIONS))
dbutils.widgets.text("levers", "[1,2,3,4,5]")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("deploy_target", "")
dbutils.widgets.text("triggered_by", "")

run_id = dbutils.widgets.get("run_id")
space_id = dbutils.widgets.get("space_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain")
experiment_name = dbutils.widgets.get("experiment_name") or None
max_iterations = int(dbutils.widgets.get("max_iterations") or str(MAX_ITERATIONS))
levers = json.loads(dbutils.widgets.get("levers") or "[1,2,3,4,5]")
apply_mode = dbutils.widgets.get("apply_mode") or "genie_config"
deploy_target = dbutils.widgets.get("deploy_target") or None
triggered_by = dbutils.widgets.get("triggered_by") or ""

_banner("Resolved Widget Inputs")
_log(
    "Parameters",
    run_id=run_id,
    space_id=space_id,
    catalog=catalog,
    schema=schema,
    domain=domain,
    experiment_name=experiment_name,
    max_iterations=max_iterations,
    levers=levers,
    apply_mode=apply_mode,
    deploy_target=deploy_target,
    triggered_by=triggered_by,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 WorkspaceClient and SparkSession Initialization
# MAGIC
# MAGIC - **`WorkspaceClient()`** — Uses the job's execution identity (service principal or user). Needed for Genie API (`fetch_space_config`), MLflow, and workspace operations.
# MAGIC - **`SparkSession.builder.getOrCreate()`** — Reuses the cluster's Spark context. Required for Delta DDL and SQL.
# MAGIC
# MAGIC > **💡 Tip:** Missing `DATABRICKS_HOST` / `DATABRICKS_TOKEN` (or OAuth) causes `WorkspaceClient` to fail. Spark is typically already configured by the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💾 Delta State Tables
# MAGIC
# MAGIC `ensure_optimization_tables(spark, catalog, schema)` creates these 7 tables with `CREATE TABLE IF NOT EXISTS`:
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `genie_opt_runs` | One row per run: status, config snapshot, best iteration, convergence reason, labeling session URL |
# MAGIC | `genie_opt_stages` | Stage transitions (PREFLIGHT_STARTED, BASELINE_EVAL_STARTED, etc.) with timestamps |
# MAGIC | `genie_opt_iterations` | Per-iteration scores, evaluation results, and adaptive reflection entries |
# MAGIC | `genie_opt_patches` | Applied patches per iteration/lever; rollback tracking and provenance chain |
# MAGIC | `genie_eval_asi_results` | ASI (Automated Structured Investigation) judge feedback |
# MAGIC | `genie_opt_data_access_grants` | Tracks data access grants applied during optimization |
# MAGIC | `genie_opt_provenance` | End-to-end provenance linking patches to judge verdicts and gate outcomes |
# MAGIC
# MAGIC > **📝 Note:** Idempotent — safe to call on every run. For existing tables, `_migrate_add_columns()` adds any new columns (e.g. `reflection_json`, `labeling_session_url`) that were introduced in newer versions, making upgrades seamless. Missing catalog/schema permissions will raise.

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

from genie_space_optimizer.common.genie_client import (
    configure_connection_pool,
    configure_mlflow_connection_pool,
)
from genie_space_optimizer.common.config import CONNECTION_POOL_SIZE
configure_connection_pool(w, CONNECTION_POOL_SIZE)
configure_mlflow_connection_pool(CONNECTION_POOL_SIZE)

_banner("Ensuring Delta State Tables")
ensure_optimization_tables(spark, catalog, schema)
_log("State tables verified", catalog=catalog, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1a: Genie Space Configuration Import
# MAGIC
# MAGIC Load the Genie Space configuration from the run snapshot (captured at trigger time
# MAGIC by the app backend) or, as a fallback, fetch it via the Genie API. The configuration
# MAGIC contains table references, metric views, TVFs, instructions, and column descriptions
# MAGIC that form the foundation for benchmark generation and optimization.

# COMMAND ----------

try:
    _banner("Step 1a — Config Fetch")
    ctx_config = preflight_fetch_config(
        w, spark, run_id, space_id, catalog, schema, domain, apply_mode,
    )
    _config = ctx_config["config"]
    _snapshot = ctx_config["snapshot"]
    _genie_table_refs = ctx_config["genie_table_refs"]
    _domain = ctx_config["domain"]
    _log("Config fetched", tables=len(ctx_config["genie_table_refs"]))
except Exception as exc:
    _banner("Config Fetch FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b: Unity Catalog Metadata Collection
# MAGIC
# MAGIC Collect columns, tags, routines, and foreign-key constraints for all tables and views
# MAGIC referenced in the Genie Space. Uses a 3-tier fallback: prefetched (OBO cache) →
# MAGIC REST API → Spark SQL. Also runs data profiling to discover low-cardinality columns
# MAGIC with value hints, and computes join overlap scores for FK pairs.

# COMMAND ----------

try:
    _banner("Step 1b — UC Metadata")
    ctx_uc = preflight_collect_uc_metadata(
        w, spark, run_id, catalog, schema, _config, _snapshot,
        _genie_table_refs, apply_mode=apply_mode,
        configured_cols=ctx_config.get("configured_cols", 0),
    )
    _log("Metadata collected", columns=len(ctx_uc["uc_columns"]), tags=len(ctx_uc["uc_tags"]),
         routines=len(ctx_uc["uc_routines"]), fk=len(ctx_uc["uc_fk"]))
except Exception as exc:
    _banner("UC Metadata FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1c: Benchmark Generation
# MAGIC
# MAGIC Load existing benchmarks from the Unity Catalog evaluation dataset, or generate
# MAGIC new ones using the LLM if no suitable benchmarks exist. Benchmarks are
# MAGIC natural-language questions paired with optional expected SQL that drive the
# MAGIC 9-judge evaluation scoring.

# COMMAND ----------

try:
    _banner("Step 1c — Benchmark Generation")
    ctx_bench = preflight_generate_benchmarks(
        w, spark, run_id, catalog, schema, _config,
        ctx_uc["uc_columns"], ctx_uc["uc_tags"], ctx_uc["uc_routines"],
        _domain,
    )
    _benchmarks = ctx_bench["benchmarks"]
    _log("Benchmarks loaded", count=len(_benchmarks), regenerated=ctx_bench["regenerated"])
except Exception as exc:
    _banner("Benchmark Generation FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1d: Benchmark Validation
# MAGIC
# MAGIC Validate each benchmark's SQL via `EXPLAIN` to ensure it compiles against the
# MAGIC current schema. Invalid benchmarks (unresolved columns, missing tables, syntax errors)
# MAGIC are quarantined. If fewer than 5 valid benchmarks remain, regeneration is triggered.

# COMMAND ----------

try:
    _banner("Step 1d — Benchmark Validation")
    ctx_valid = preflight_validate_benchmarks(
        w, spark, run_id, catalog, schema, _config, _benchmarks,
        ctx_uc["uc_columns"], ctx_uc["uc_tags"], ctx_uc["uc_routines"],
        _domain,
    )
    _benchmarks = ctx_valid["benchmarks"]
    _log("Validation complete", valid=len(_benchmarks), pre_count=ctx_valid["pre_count"],
         rejected=ctx_valid["pre_count"] - len(_benchmarks))
except Exception as exc:
    _banner("Benchmark Validation FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1e: Past Human Feedback
# MAGIC
# MAGIC Load human corrections from prior completed optimization runs for this Genie Space.
# MAGIC Corrections include benchmark fixes, judge overrides, and quarantine decisions
# MAGIC that carry forward to improve accuracy in subsequent runs.

# COMMAND ----------

_banner("Step 1e — Human Feedback")
ctx_feedback = preflight_load_human_feedback(
    spark, run_id, space_id, catalog, schema, _domain,
)
_log("Feedback loaded", corrections=len(ctx_feedback["human_corrections"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1f: Experiment and Model Setup
# MAGIC
# MAGIC Create or resolve the MLflow experiment, register judge prompts, flag stale temporal
# MAGIC benchmarks, sync the evaluation dataset, and create the initial LoggedModel
# MAGIC (iteration 0). This cell writes the final `PREFLIGHT_STARTED → COMPLETE` stage record
# MAGIC to Delta.

# COMMAND ----------

try:
    _banner("Step 1f — Experiment & Model Setup")
    ctx_exp = preflight_setup_experiment(
        w, spark, run_id, space_id, catalog, schema, _domain,
        _config, _benchmarks,
        ctx_uc["uc_columns"], ctx_uc["uc_tags"], ctx_uc["uc_routines"],
        _genie_table_refs, experiment_name,
    )
    _log("Experiment created", model_id=ctx_exp["model_id"],
         experiment=ctx_exp["experiment_name"], prompts=len(ctx_exp["prompt_registrations"]))
except Exception as exc:
    _banner("Experiment Setup FAILED")
    _log("Failure details", error_type=type(exc).__name__, error_message=str(exc), traceback=traceback.format_exc())
    raise

# Assemble the preflight_out dict for downstream task value publication
import mlflow as _mlflow
_exp = _mlflow.get_experiment_by_name(ctx_exp["experiment_name"])
_experiment_id = _exp.experiment_id if _exp else ""

update_run_status(
    spark, run_id, catalog, schema,
    status="IN_PROGRESS",
    experiment_name=ctx_exp["experiment_name"],
    experiment_id=_experiment_id,
)

preflight_out = {
    "config": _config,
    "benchmarks": _benchmarks,
    "model_id": ctx_exp["model_id"],
    "experiment_name": ctx_exp["experiment_name"],
    "experiment_id": _experiment_id,
    "human_corrections": ctx_feedback["human_corrections"],
}

_log(
    "Preflight complete",
    benchmark_count=len(_benchmarks),
    model_id=preflight_out["model_id"],
    experiment_name=preflight_out["experiment_name"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📤 Task Value Publication Pattern
# MAGIC
# MAGIC Downstream tasks read values via:
# MAGIC
# MAGIC ```python
# MAGIC run_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="run_id")
# MAGIC ```
# MAGIC
# MAGIC > **📝 Note:** Only strings and JSON-serializable values. `levers` is stored as `json.dumps(levers)`; consumers must `json.loads(...)`.
# MAGIC
# MAGIC | Key | Consumed By |
# MAGIC |-----|-------------|
# MAGIC | `run_id`, `space_id`, `domain`, `catalog`, `schema` | All tasks |
# MAGIC | `experiment_name`, `experiment_id`, `model_id` | baseline, lever_loop, finalize |
# MAGIC | `benchmark_count` | baseline (validates benchmark load) |
# MAGIC | `max_iterations`, `levers`, `apply_mode`, `deploy_target` | lever_loop, finalize, deploy |
# MAGIC | `triggered_by` | lever_loop (for context/audit) |

# COMMAND ----------

# Pass task values to downstream tasks
_banner("Publishing Task Values")
dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="space_id", value=space_id)
dbutils.jobs.taskValues.set(key="domain", value=domain)
dbutils.jobs.taskValues.set(key="catalog", value=catalog)
dbutils.jobs.taskValues.set(key="schema", value=schema)
dbutils.jobs.taskValues.set(key="experiment_name", value=preflight_out["experiment_name"])
dbutils.jobs.taskValues.set(key="experiment_id", value=preflight_out.get("experiment_id", ""))
dbutils.jobs.taskValues.set(key="model_id", value=preflight_out["model_id"])
dbutils.jobs.taskValues.set(key="benchmark_count", value=len(preflight_out["benchmarks"]))
dbutils.jobs.taskValues.set(key="max_iterations", value=max_iterations)
dbutils.jobs.taskValues.set(key="levers", value=json.dumps(levers))
dbutils.jobs.taskValues.set(key="apply_mode", value=apply_mode)
dbutils.jobs.taskValues.set(key="deploy_target", value=deploy_target or "")
dbutils.jobs.taskValues.set(key="triggered_by", value=triggered_by)
dbutils.jobs.taskValues.set(key="human_corrections", value=json.dumps(preflight_out.get("human_corrections", []), default=str))

_log(
    "Task values published",
    run_id=run_id,
    benchmark_count=len(preflight_out["benchmarks"]),
    model_id=preflight_out["model_id"],
)
_banner("Task 1 Completed")
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "benchmark_count": len(preflight_out["benchmarks"]),
    "model_id": preflight_out["model_id"],
}, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ What Success Looks Like
# MAGIC
# MAGIC When this task completes successfully, you will see output similar to:
# MAGIC
# MAGIC ```
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-1 PREFLIGHT] Running _run_preflight
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 10:05:12 UTC] [TASK-1 PREFLIGHT] Starting preflight
# MAGIC   {"space_id": "01ab...", "catalog": "my_catalog", "schema": "my_schema"}
# MAGIC [2026-02-28 10:05:45 UTC] [TASK-1 PREFLIGHT] Preflight complete
# MAGIC   {"benchmark_count": 42, "model_id": "mv-abc123", "experiment_name": "/Shared/genie-optimization/revenue"}
# MAGIC [2026-02-28 10:05:46 UTC] [TASK-1 PREFLIGHT] Benchmark summary
# MAGIC   {"total": 42, "with_sql": 38, "question_only": 4}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-1 PREFLIGHT] Publishing Task Values
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [2026-02-28 10:05:47 UTC] [TASK-1 PREFLIGHT] Task values published
# MAGIC   {"run_id": "run-xyz", "benchmark_count": 42, "model_id": "mv-abc123"}
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC [TASK-1 PREFLIGHT] Task 1 Completed
# MAGIC ════════════════════════════════════════════════════════════════
# MAGIC ```
# MAGIC
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC - **Task 1 (Preflight)** validates inputs, ensures Delta tables, fetches config and UC metadata, loads or generates benchmarks, sets up the MLflow experiment and iteration 0 model, and publishes task values for Tasks 2–5.
# MAGIC - **On success:** Run status is `IN_PROGRESS`; baseline task can proceed.
# MAGIC - **On failure:** Run status is `FAILED`; inspect logs and `genie_opt_stages` for diagnostics.
# MAGIC - **Next:** Task 2 (baseline_eval) runs the full 9-judge evaluation on iteration 0 and checks quality thresholds.
