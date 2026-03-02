# Databricks notebook source
# MAGIC %md
# MAGIC # Task 4: Finalize — Training Document
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Task 4 (Finalize) is the penultimate stage of the Genie Space optimization pipeline. It performs **repeatability testing**, **model promotion**, and **final report generation** before the optional deploy step. This task ensures that the optimized configuration is validated for consistency and that all downstream consumers receive a complete, auditable summary of the run.
# MAGIC
# MAGIC ## What Finalize Does
# MAGIC
# MAGIC 1. **Reads upstream values** from preflight and lever_loop (or baseline_eval if lever_loop was skipped)
# MAGIC 2. **Loads benchmarks** from the UC table for repeatability testing
# MAGIC 3. **Runs repeatability testing** — re-queries Genie multiple times and compares SQL hashes to detect non-determinism
# MAGIC 4. **Promotes the best model** to the production registry
# MAGIC 5. **Generates a final report** summarizing the run, scores, convergence reason, and optimization history
# MAGIC 6. **Determines terminal status** (CONVERGED, MAX_ITERATIONS, STALLED, or FAILED)
# MAGIC 7. **Publishes task values** for the deploy task
# MAGIC
# MAGIC ## Place in the 5-Task DAG
# MAGIC
# MAGIC ```
# MAGIC preflight → baseline_eval → lever_loop → finalize (this task) → deploy
# MAGIC ```
# MAGIC
# MAGIC - **Depends on:** lever_loop (must complete or skip first)
# MAGIC - **Feeds:** deploy (uses `status`, `convergence_reason`, `terminal_reason`, `repeatability_pct`, `report_path`)
# MAGIC
# MAGIC ## Input Sources: lever_loop vs baseline_eval
# MAGIC
# MAGIC This task reads scores and model metadata from **either** `lever_loop` **or** `baseline_eval`, depending on whether the lever loop was skipped:
# MAGIC
# MAGIC - **If lever loop was skipped** (baseline already met all thresholds): reads from `baseline_eval` — scores, model_id, and iteration_counter=0.
# MAGIC - **If lever loop ran**: reads from `lever_loop` — scores, model_id, and iteration_counter from the last iteration.
# MAGIC
# MAGIC The `skipped` task value from `lever_loop` determines which source to use.
# MAGIC
# MAGIC ## What Happens If This Task Fails
# MAGIC
# MAGIC - Deploy task may still run (depends on job configuration) but will lack finalize metadata
# MAGIC - Delta state is updated with `FINALIZE` = FAILED
# MAGIC - Run status is set to FAILED with `convergence_reason=error_in_FINALIZE` or `finalize_timeout`
# MAGIC - **Debugging:** Check job run logs for `[TASK-4 FINALIZE] Failure details`, inspect `genie_opt_stages` for the FINALIZE stage record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Timeout and Heartbeat Mechanism
# MAGIC
# MAGIC Finalize can be long-running (repeatability tests re-query Genie many times). To keep the task observable and avoid silent hangs:
# MAGIC
# MAGIC | Parameter | Default | Env Var | Purpose |
# MAGIC |-----------|---------|--------|---------|
# MAGIC | `FINALIZE_TIMEOUT_SECONDS` | 6600 | `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS` | Soft timeout; raises `TimeoutError` with terminal reason `finalize_timeout` |
# MAGIC | `FINALIZE_HEARTBEAT_SECONDS` | 30 | `GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS` | Interval between heartbeat events; updates `run.updated_at` and writes `FINALIZE_HEARTBEAT` stage records so stale-state reconciliation does not mark the run as dead |
# MAGIC
# MAGIC ## Repeatability Testing
# MAGIC
# MAGIC Repeatability testing runs exactly **2 evaluation passes** over all benchmarks, re-querying Genie for each question and comparing the generated SQL hashes across invocations. The final repeatability percentage is the **average** of both runs' per-question match rates.
# MAGIC
# MAGIC It matters because:
# MAGIC - Non-deterministic SQL generation can cause flaky evaluations and unreliable optimization decisions.
# MAGIC - A low repeatability score indicates the model or space configuration may need tuning.
# MAGIC - Results are written to Delta and included in the final report.
# MAGIC
# MAGIC ## Terminal Status Determination
# MAGIC
# MAGIC After repeatability and promotion, the harness resolves a **terminal status**:
# MAGIC
# MAGIC | Status | Condition | `convergence_reason` |
# MAGIC |--------|-----------|---------------------|
# MAGIC | **CONVERGED** | All thresholds met | `threshold_met` |
# MAGIC | **MAX_ITERATIONS** | Iteration limit reached | `max_iterations` |
# MAGIC | **STALLED** | No further improvement possible | `no_further_improvement` |
# MAGIC | **FAILED** | Timeout or exception | `finalize_timeout` or `finalize_error` |
# MAGIC
# MAGIC The `terminal_reason` is published for downstream tasks and audit logs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Helper Functions
# MAGIC
# MAGIC | Import | Purpose |
# MAGIC |--------|---------|
# MAGIC | `json` | Deserialize upstream scores JSON and serialize debug payloads |
# MAGIC | `traceback` | Format full stack traces on failure for debugging |
# MAGIC | `partial` | Bind `_TASK_LABEL` to shared `_banner` and `_log` helpers |
# MAGIC | `WorkspaceClient` | Databricks SDK — MLflow, workspace operations |
# MAGIC | `SparkSession` | Delta state writes and benchmark loading |
# MAGIC | `_banner`, `_log` | Shared logging helpers from `_helpers.py` |
# MAGIC | `load_benchmarks_from_dataset` | Load benchmarks from UC table for repeatability testing |
# MAGIC | `_run_finalize` | Harness stage function: repeatability, promotion, report generation |
# MAGIC
# MAGIC ### Helper Functions
# MAGIC
# MAGIC | Function | What It Does |
# MAGIC |----------|---------------|
# MAGIC | `_banner(title)` | Prints a 120-char separator and `[TASK-4 FINALIZE] {title}` for visual section breaks |
# MAGIC | `_log(event, **payload)` | Logs `event` with optional JSON payload; uses `default=str` for non-JSON-serializable values |

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
from genie_space_optimizer.optimization.harness import _run_finalize

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-4 FINALIZE"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Upstream Task Values
# MAGIC
# MAGIC Task 4 reads from two upstream tasks depending on whether lever_loop ran or was skipped:
# MAGIC
# MAGIC **From preflight (always):**
# MAGIC
# MAGIC | Key | Purpose |
# MAGIC |-----|---------|
# MAGIC | `run_id` | Optimization run identifier |
# MAGIC | `space_id` | Genie Space being optimized |
# MAGIC | `domain` | Domain for benchmark table lookup |
# MAGIC | `catalog`, `schema` | Unity Catalog location for state and benchmarks |
# MAGIC | `experiment_name` | MLflow experiment path |
# MAGIC
# MAGIC **From lever_loop or baseline_eval (conditional):**
# MAGIC
# MAGIC | Key | Source when skipped | Source when ran | Purpose |
# MAGIC |-----|--------------------|-----------------|---------| 
# MAGIC | `scores` | `baseline_eval` | `lever_loop` | Per-judge score dict |
# MAGIC | `model_id` | `baseline_eval` | `lever_loop` | Best model version ID |
# MAGIC | `iteration_counter` | `0` (hardcoded) | `lever_loop` | Number of lever iterations completed |
# MAGIC
# MAGIC The `skipped` key from `lever_loop` determines which source to use. When `True`, baseline values are used directly since no optimization was needed.

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

# Read from lever_loop (or baseline if lever_loop was skipped)
lever_skipped_raw = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="skipped")
lever_skipped = str(lever_skipped_raw).lower() in ("true", "1")
if lever_skipped:
    scores_json = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="scores")
    prev_model_id = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="model_id")
    iteration_counter = 0
else:
    scores_json = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="scores")
    prev_model_id = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="model_id")
    iteration_counter = int(dbutils.jobs.taskValues.get(taskKey="lever_loop", key="iteration_counter"))

prev_scores = json.loads(scores_json)

_banner("Resolved Upstream Task Values")
_log(
    "Inputs",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    experiment_name=exp_name,
    lever_skipped=bool(lever_skipped),
    prev_model_id=prev_model_id,
    iteration_counter=iteration_counter,
    score_keys=sorted(list(prev_scores.keys())) if isinstance(prev_scores, dict) else [],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Benchmarks for Repeatability
# MAGIC
# MAGIC Benchmarks are loaded from the UC table `{catalog}.{schema}.genie_benchmarks_{domain}` for use in repeatability testing. The same benchmark set used during baseline and lever_loop evaluation is re-used here to ensure consistency.

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
_banner("Loaded Benchmarks for Repeatability")
_log(
    "Benchmark dataset",
    uc_schema=uc_schema,
    domain=domain,
    benchmark_count=len(benchmarks),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What `_run_finalize` Does Internally
# MAGIC
# MAGIC The harness function `_run_finalize()` (in `optimization/harness.py`) performs:
# MAGIC
# MAGIC 1. **Stage write** — Records `FINALIZE_STARTED` in Delta `genie_opt_stages`.
# MAGIC 2. **Repeatability testing** — Runs 2 evaluation passes over all benchmarks, comparing SQL hashes across invocations. Computes per-question match rates and averages them into a final `repeatability_pct`.
# MAGIC 3. **Terminal status resolution** — Determines whether the run converged, hit max iterations, stalled, or failed. This drives the `convergence_reason` and `terminal_reason` fields.
# MAGIC 4. **Model promotion** — Promotes the best-performing model (by accuracy and repeatability) to the production registry via MLflow.
# MAGIC 5. **Report generation** — Produces a comprehensive report artifact summarizing scores, convergence reason, iteration history, and repeatability results.
# MAGIC 6. **State updates** — Writes `FINALIZE_COMPLETE` stage, updates `genie_opt_runs` with terminal status and report path.
# MAGIC 7. **Heartbeat loop** — During long-running repeatability tests, periodically writes `FINALIZE_HEARTBEAT` stage records and updates `run.updated_at` to prevent stale-state detection.
# MAGIC
# MAGIC **Returns:** `{status, convergence_reason, terminal_reason, repeatability_pct, report_path, elapsed_seconds, heartbeat_count}`

# COMMAND ----------

try:
    _banner("Running _run_finalize")
    finalize_out = _run_finalize(
        w, spark, run_id, space_id, domain, exp_name,
        prev_scores, prev_model_id, iteration_counter,
        catalog, schema,
        run_repeatability=True,
        benchmarks=benchmarks,
    )
    _log(
        "Finalize finished",
        status=finalize_out["status"],
        convergence_reason=finalize_out["convergence_reason"],
        terminal_reason=finalize_out.get("terminal_reason"),
        repeatability_pct=finalize_out["repeatability_pct"],
        elapsed_seconds=finalize_out.get("elapsed_seconds"),
        heartbeat_count=finalize_out.get("heartbeat_count"),
        report_path=finalize_out.get("report_path", ""),
    )
except Exception as exc:
    _banner("Finalize FAILED")
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
# MAGIC The deploy task reads these keys via `dbutils.jobs.taskValues.get(taskKey="finalize", key="...")`.
# MAGIC
# MAGIC | Key | Type | Consumed By | Description |
# MAGIC |-----|------|-------------|-------------|
# MAGIC | `status` | str | deploy | Terminal status (CONVERGED, MAX_ITERATIONS, STALLED, FAILED) |
# MAGIC | `convergence_reason` | str | deploy | Why the run ended (threshold_met, max_iterations, etc.) |
# MAGIC | `terminal_reason` | str | deploy, audit | Detailed reason for terminal state |
# MAGIC | `repeatability_pct` | float | deploy, report | Repeatability percentage from SQL hash comparison |
# MAGIC | `report_path` | str | deploy, UI | Workspace path to the generated report artifact |

# COMMAND ----------

_banner("Publishing Task Values")
dbutils.jobs.taskValues.set(key="status", value=finalize_out["status"])
dbutils.jobs.taskValues.set(key="convergence_reason", value=finalize_out["convergence_reason"])
dbutils.jobs.taskValues.set(key="repeatability_pct", value=finalize_out["repeatability_pct"])
dbutils.jobs.taskValues.set(key="report_path", value=finalize_out.get("report_path", ""))
dbutils.jobs.taskValues.set(
    key="terminal_reason",
    value=finalize_out.get("terminal_reason", finalize_out["convergence_reason"]),
)

_log(
    "Task values published",
    status=finalize_out["status"],
    convergence_reason=finalize_out["convergence_reason"],
    terminal_reason=finalize_out.get("terminal_reason", finalize_out["convergence_reason"]),
    repeatability_pct=finalize_out["repeatability_pct"],
)
_banner("Task 4 Completed")
dbutils.notebook.exit(json.dumps({
    "status": finalize_out["status"],
    "convergence_reason": finalize_out["convergence_reason"],
    "repeatability_pct": finalize_out["repeatability_pct"],
}, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Known Failure Modes
# MAGIC
# MAGIC ### 1. Finalize Timeout
# MAGIC
# MAGIC **Cause:** Repeatability testing exceeds `FINALIZE_TIMEOUT_SECONDS` (default 6600s / ~110 min). This can happen with large benchmark sets or slow Genie API responses.
# MAGIC
# MAGIC **Symptoms:** `TimeoutError` with `terminal_reason=finalize_timeout`. The run status is set to FAILED.
# MAGIC
# MAGIC **Remediation:**
# MAGIC - Increase `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS` (e.g. 10800 for 3 hours)
# MAGIC - Reduce benchmark count for large runs
# MAGIC - Check Genie API latency and cluster health
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. MLflow Model Promotion Errors
# MAGIC
# MAGIC **Cause:** Insufficient permissions to promote the model, or the model version no longer exists.
# MAGIC
# MAGIC **Remediation:**
# MAGIC - Verify the service principal has `MANAGE` permissions on the MLflow experiment
# MAGIC - Check that the `model_id` from lever_loop/baseline is valid and accessible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Repeatability Regressions
# MAGIC
# MAGIC **Note:** Low repeatability does **not** cause task failure — it is recorded as a metric. However, a very low repeatability percentage (<50%) may indicate instability in the Genie Space configuration or API.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How to Interpret Error Logs
# MAGIC
# MAGIC 1. Look for `[TASK-4 FINALIZE] Failure details` — logs `error_type`, `error_message`, and full `traceback`
# MAGIC 2. Check `elapsed_seconds` and `heartbeat_count` to understand how far finalize progressed
# MAGIC 3. Check Delta `genie_opt_stages` for `FINALIZE` stage records and `error_message` column
# MAGIC
# MAGIC ### Remediation Checklist
# MAGIC
# MAGIC | Symptom | Action |
# MAGIC |---------|--------|
# MAGIC | `TimeoutError` / `finalize_timeout` | Increase `FINALIZE_TIMEOUT_SECONDS`; check Genie API health |
# MAGIC | MLflow permission denied | Grant `MANAGE` on experiment; verify model_id exists |
# MAGIC | Low repeatability (<50%) | Investigate Genie Space config stability; re-run optimization |
# MAGIC | Heartbeats stop but task hasn't finished | Check cluster health; may need larger compute |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Task 4 (Finalize)** runs repeatability testing, promotes the best model, generates the final report, and determines terminal status.
# MAGIC - **Input branching:** Reads from `lever_loop` if it ran, or `baseline_eval` if it was skipped. The `skipped` task value controls the source.
# MAGIC - **On success:** Publishes `status`, `convergence_reason`, `repeatability_pct`, `report_path`, and `terminal_reason` for Task 5 (deploy).
# MAGIC - **On failure:** Run status is FAILED; inspect logs and `genie_opt_stages` for diagnostics.
# MAGIC - **Next:** Task 5 (deploy) applies the optimized configuration to a target environment (conditional on `deploy_target`).
