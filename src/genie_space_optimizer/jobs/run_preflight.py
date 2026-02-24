# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1: Preflight — Comprehensive Training Guide
# MAGIC
# MAGIC ## What Is Preflight?
# MAGIC
# MAGIC **Preflight** is the first stage of the Genie Space Optimizer's 5-task DAG. It runs *before* any optimization iterations and prepares everything downstream tasks need:
# MAGIC
# MAGIC | Responsibility | Why It Matters |
# MAGIC |----------------|----------------|
# MAGIC | **Validate orchestration parameters** | Ensures `run_id`, `space_id`, `catalog`, `schema`, etc. are present and sane before any work begins |
# MAGIC | **Ensure Delta state tables exist** | Creates `genie_opt_runs`, `genie_opt_stages`, `genie_opt_iterations`, `genie_opt_patches`, `genie_eval_asi_results` if missing |
# MAGIC | **Fetch Genie Space config** | Baseline configuration is needed for lever proposals and rollback |
# MAGIC | **Collect UC metadata** | Columns, tags, routines inform benchmark generation and model context |
# MAGIC | **Load or generate benchmarks** | Evaluation queries that drive accuracy scoring; LLM generates if none exist |
# MAGIC | **Register judge prompts & create iteration 0 model** | MLflow experiment setup and initial LoggedModel for baseline comparison |
# MAGIC
# MAGIC ## What Happens If Preflight Fails?
# MAGIC
# MAGIC - **The entire DAG stops.** Tasks 2–5 (baseline, lever_loop, finalize, deploy) never run.
# MAGIC - **Run status** is written as `FAILED` in `genie_opt_runs` (via `_safe_stage` in the harness).
# MAGIC - **Error details** are logged and re-raised; the Databricks Job run fails.
# MAGIC - **Debugging:** Check job run logs, `genie_opt_stages` for the last stage/status, and ensure Genie API access, UC permissions, and MLflow experiment paths are valid.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture: DAG Topology and Data Flow
# MAGIC
# MAGIC ### 5-Task DAG
# MAGIC
# MAGIC ```
# MAGIC   ┌─────────────┐
# MAGIC   │  preflight  │  ← Task 1 (this notebook)
# MAGIC   └──────┬──────┘
# MAGIC          │ taskValues: run_id, space_id, catalog, schema, domain, experiment_name,
# MAGIC          │             experiment_id, model_id, benchmark_count, max_iterations,
# MAGIC          │             levers, apply_mode, deploy_target
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
# MAGIC ## Imports and Helper Functions
# MAGIC
# MAGIC | Import | Purpose |
# MAGIC |--------|---------|
# MAGIC | `json` | Serialize `levers` and other payloads for logging and task values |
# MAGIC | `traceback` | Format full stack traces on failure for debugging |
# MAGIC | `datetime`, `timezone` | UTC timestamps for `_ts()` and consistent logging |
# MAGIC | `WorkspaceClient` | Databricks SDK — Genie API, MLflow, workspace operations |
# MAGIC | `SparkSession` | Delta table creation and SQL execution |
# MAGIC | `_run_preflight` | Harness stage function: config fetch, UC metadata, benchmarks, model creation |
# MAGIC | `ensure_optimization_tables` | Creates the 5 Delta tables if they don't exist |
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
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.harness import _run_preflight
from genie_space_optimizer.optimization.state import ensure_optimization_tables


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _banner(title: str) -> None:
    print("\n" + "=" * 120)
    print(f"[{_ts()}] [TASK-1 PREFLIGHT] {title}")
    print("=" * 120)


def _log(event: str, **payload) -> None:
    print(f"[{_ts()}] [TASK-1 PREFLIGHT] {event}")
    if payload:
        print(json.dumps(payload, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Parameters
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
# MAGIC | `max_iterations` | text | `"5"` | Max lever iterations before stopping |
# MAGIC | `levers` | text | `"[1,2,3,4,5,6]"` | JSON array of lever numbers to try |
# MAGIC | `apply_mode` | text | `"genie_config"` | Where patches apply: `genie_config` \| `uc_artifact` \| `both` |
# MAGIC | `deploy_target` | text | `""` | DABs target for post-optimization deploy (optional) |
# MAGIC
# MAGIC **Validation:** Empty `run_id`, `space_id`, `catalog`, or `schema` will cause downstream failures. The harness does not validate here; failures surface during `_run_preflight`.

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("experiment_name", "")
dbutils.widgets.text("max_iterations", "5")
dbutils.widgets.text("levers", "[1,2,3,4,5,6]")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("deploy_target", "")

run_id = dbutils.widgets.get("run_id")
space_id = dbutils.widgets.get("space_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain")
experiment_name = dbutils.widgets.get("experiment_name") or None
max_iterations = int(dbutils.widgets.get("max_iterations") or "5")
levers = json.loads(dbutils.widgets.get("levers") or "[1,2,3,4,5,6]")
apply_mode = dbutils.widgets.get("apply_mode") or "genie_config"
deploy_target = dbutils.widgets.get("deploy_target") or None

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
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WorkspaceClient and SparkSession Initialization
# MAGIC
# MAGIC - **`WorkspaceClient()`** — Uses the job's execution identity (service principal or user). Needed for Genie API (`fetch_space_config`), MLflow, and workspace operations.
# MAGIC - **`SparkSession.builder.getOrCreate()`** — Reuses the cluster's Spark context. Required for Delta DDL and SQL.
# MAGIC
# MAGIC **Common issues:** Missing `DATABRICKS_HOST` / `DATABRICKS_TOKEN` (or OAuth) causes `WorkspaceClient` to fail. Spark is typically already configured by the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta State Tables
# MAGIC
# MAGIC `ensure_optimization_tables(spark, catalog, schema)` creates these tables with `CREATE TABLE IF NOT EXISTS`:
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `genie_opt_runs` | One row per run: status, config snapshot, best iteration, convergence reason |
# MAGIC | `genie_opt_stages` | Stage transitions (PREFLIGHT_STARTED, BASELINE_EVAL_STARTED, etc.) with timestamps |
# MAGIC | `genie_opt_iterations` | Per-iteration scores and evaluation results |
# MAGIC | `genie_opt_patches` | Applied patches per iteration/lever; rollback tracking |
# MAGIC | `genie_eval_asi_results` | ASI (Automated Semantic Inference) judge feedback |
# MAGIC
# MAGIC **Idempotent:** Safe to call on every run. Missing catalog/schema permissions will raise.

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

_banner("Ensuring Delta State Tables")
ensure_optimization_tables(spark, catalog, schema)
_log("State tables verified", catalog=catalog, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What `_run_preflight` Does Internally
# MAGIC
# MAGIC The harness calls `run_preflight` (in `optimization/preflight.py`) wrapped by `_safe_stage` for error handling. The sequence:
# MAGIC
# MAGIC 1. **Config fetch** — Load from `genie_opt_runs.config_snapshot` if present, else `fetch_space_config(w, space_id)` via Genie API.
# MAGIC 2. **UC metadata** — `get_columns`, `get_tags`, `get_routines` for `catalog.schema`. Best-effort; continues if some fail (e.g. limited permissions).
# MAGIC 3. **Experiment resolution** — Pick MLflow experiment path from `triggered_by` or `/Shared/genie-optimization/{domain}`.
# MAGIC 4. **Benchmarks** — Load from UC evaluation dataset if ≥5 exist; otherwise LLM-generated via `generate_benchmarks`.
# MAGIC 5. **Benchmark validation** — `validate_benchmarks` runs `EXPLAIN` on each query; invalid ones are discarded.
# MAGIC 6. **Evaluation dataset** — `create_evaluation_dataset` syncs benchmarks to MLflow.
# MAGIC 7. **Judge prompts** — `register_judge_prompts` registers prompts for ASI judges (idempotent).
# MAGIC 8. **Model creation** — `create_genie_model_version` creates iteration 0 LoggedModel with config + UC metadata.
# MAGIC 9. **State updates** — `write_stage(PREFLIGHT_STARTED, COMPLETE)`, `update_run_status(IN_PROGRESS)`.
# MAGIC
# MAGIC **Returns:** `{benchmarks, config, model_id, experiment_name, experiment_id}` — used for task values and downstream tasks.

# COMMAND ----------

try:
    _banner("Running _run_preflight")
    preflight_out = _run_preflight(
        w, spark, run_id, space_id, catalog, schema, domain, experiment_name,
    )
    _log(
        "Preflight complete",
        benchmark_count=len(preflight_out["benchmarks"]),
        model_id=preflight_out["model_id"],
        experiment_name=preflight_out["experiment_name"],
        experiment_id=preflight_out.get("experiment_id", ""),
    )
except Exception as exc:
    _banner("Preflight FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Value Publication Pattern
# MAGIC
# MAGIC Downstream tasks read values via:
# MAGIC
# MAGIC ```python
# MAGIC run_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="run_id")
# MAGIC ```
# MAGIC
# MAGIC **Key rule:** Only strings and JSON-serializable values. `levers` is stored as `json.dumps(levers)`; consumers must `json.loads(...)`.
# MAGIC
# MAGIC | Key | Consumed By |
# MAGIC |-----|-------------|
# MAGIC | `run_id`, `space_id`, `domain`, `catalog`, `schema` | All tasks |
# MAGIC | `experiment_name`, `experiment_id`, `model_id` | baseline, lever_loop, finalize |
# MAGIC | `benchmark_count` | baseline (validates benchmark load) |
# MAGIC | `max_iterations`, `levers`, `apply_mode`, `deploy_target` | lever_loop, finalize, deploy |

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

_log(
    "Task values published",
    run_id=run_id,
    benchmark_count=len(preflight_out["benchmarks"]),
    model_id=preflight_out["model_id"],
)
_banner("Task 1 Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Task 1 (Preflight)** validates inputs, ensures Delta tables, fetches config and UC metadata, loads or generates benchmarks, sets up the MLflow experiment and iteration 0 model, and publishes task values for Tasks 2–5.
# MAGIC - **On success:** Run status is `IN_PROGRESS`; baseline task can proceed.
# MAGIC - **On failure:** Run status is `FAILED`; inspect logs and `genie_opt_stages` for diagnostics.
# MAGIC - **Next:** Task 2 (baseline_eval) runs the full 8-judge evaluation on iteration 0 and checks quality thresholds.
