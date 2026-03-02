# Databricks notebook source
# MAGIC %md
# MAGIC # Task 5: Deploy — Training Document
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC Task 5 (Deploy) is the **final and conditional** step in the 5-task optimization DAG. It applies the optimized Genie Space configuration to a target environment (e.g., via DABs) after optimization has completed successfully.
# MAGIC
# MAGIC ## Place in the 5-Task DAG
# MAGIC
# MAGIC ```
# MAGIC preflight → baseline_eval → lever_loop → finalize → deploy (this task)
# MAGIC ```
# MAGIC
# MAGIC - **Depends on:** finalize (must complete first)
# MAGIC - **Upstream data:** preflight (run context), lever_loop or baseline_eval (model_id, iteration_counter)
# MAGIC - **Terminal task:** No downstream consumers
# MAGIC
# MAGIC ## When Does Deploy Run?
# MAGIC
# MAGIC Deploy is **conditional** — it executes only when `deploy_target` is set:
# MAGIC
# MAGIC - **`deploy_target`** is provided by the **preflight** task (from job parameters or widget input).
# MAGIC - A **condition task** (`deploy_check`) gates the deploy step: it runs only when `deploy_target` is non-empty.
# MAGIC - If `deploy_target` is empty or unset, the deploy task is skipped and the pipeline completes after finalize.
# MAGIC
# MAGIC ## What Deploy Does
# MAGIC
# MAGIC When `deploy_target` is set:
# MAGIC
# MAGIC 1. **Writes** `DEPLOY_STARTED` stage record to Delta for audit
# MAGIC 2. **Applies** the optimized configuration to the target Genie Space (DABs integration — full implementation pending)
# MAGIC 3. **Writes** `DEPLOY_COMPLETE` stage record to Delta
# MAGIC 4. **Returns** `{"status": "DEPLOYED", "deploy_target": deploy_target}` on success
# MAGIC
# MAGIC When `deploy_target` is empty:
# MAGIC
# MAGIC 1. **Writes** `DEPLOY_SKIPPED` stage record to Delta
# MAGIC 2. **Returns** `{"status": "SKIPPED", "reason": "no_deploy_target"}`
# MAGIC
# MAGIC ## What Happens If This Task Fails
# MAGIC
# MAGIC - The optimization results are **not lost** — scores, model, and report from finalize are already persisted in Delta and MLflow
# MAGIC - Delta state is updated with `DEPLOY` = FAILED
# MAGIC - **Debugging:** Check job run logs for `[TASK-5 DEPLOY] Failure details`, inspect `genie_opt_stages` for the DEPLOY stage record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Helper Functions
# MAGIC
# MAGIC | Import | Purpose |
# MAGIC |--------|---------|
# MAGIC | `json` | Serialize deploy result for `dbutils.notebook.exit()` |
# MAGIC | `traceback` | Format full stack traces on failure for debugging |
# MAGIC | `partial` | Bind `_TASK_LABEL` to shared `_banner` and `_log` helpers |
# MAGIC | `WorkspaceClient` | Databricks SDK — workspace operations, DABs integration |
# MAGIC | `SparkSession` | Delta state writes |
# MAGIC | `_banner`, `_log` | Shared logging helpers from `_helpers.py` |
# MAGIC | `_run_deploy` | Harness stage function: deploy to target, Delta state writes |
# MAGIC
# MAGIC ### Helper Functions
# MAGIC
# MAGIC | Function | What It Does |
# MAGIC |----------|---------------|
# MAGIC | `_banner(title)` | Prints a 120-char separator and `[TASK-5 DEPLOY] {title}` for visual section breaks |
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
from genie_space_optimizer.optimization.harness import _run_deploy

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-5 DEPLOY"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Upstream Task Values
# MAGIC
# MAGIC Task 5 reads from two upstream tasks depending on whether lever_loop ran or was skipped:
# MAGIC
# MAGIC **From preflight (always):**
# MAGIC
# MAGIC | Key | Purpose |
# MAGIC |-----|---------|
# MAGIC | `run_id` | Optimization run identifier |
# MAGIC | `space_id` | Genie Space being optimized |
# MAGIC | `domain` | Domain context for deploy operations |
# MAGIC | `catalog`, `schema` | Unity Catalog location for state tables |
# MAGIC | `experiment_name` | MLflow experiment path |
# MAGIC | `deploy_target` | DABs target for deployment (empty = skip deploy) |
# MAGIC
# MAGIC **From lever_loop or baseline_eval (conditional):**
# MAGIC
# MAGIC | Key | Source when skipped | Source when ran | Purpose |
# MAGIC |-----|--------------------|-----------------|---------| 
# MAGIC | `model_id` | `baseline_eval` | `lever_loop` | Best model version ID to deploy |
# MAGIC | `iteration_counter` | `0` (hardcoded) | `lever_loop` | Number of lever iterations completed |
# MAGIC
# MAGIC The `skipped` key from `lever_loop` determines which source to use. This mirrors the branching logic in Task 4 (finalize).

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
deploy_target = dbutils.jobs.taskValues.get(taskKey="preflight", key="deploy_target") or None

# Read from lever_loop (or baseline if lever_loop was skipped)
lever_skipped_raw = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="skipped")
lever_skipped = str(lever_skipped_raw).lower() in ("true", "1")
if lever_skipped:
    prev_model_id = dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="model_id")
    iteration_counter = 0
else:
    prev_model_id = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="model_id")
    iteration_counter = int(dbutils.jobs.taskValues.get(taskKey="lever_loop", key="iteration_counter"))

_banner("Resolved Upstream Task Values")
_log(
    "Inputs",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    experiment_name=exp_name,
    deploy_target=deploy_target,
    lever_skipped=bool(lever_skipped),
    prev_model_id=prev_model_id,
    iteration_counter=iteration_counter,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What `_run_deploy` Does Internally
# MAGIC
# MAGIC The harness function `_run_deploy()` (in `optimization/harness.py`) performs:
# MAGIC
# MAGIC 1. **No-target check** — If `deploy_target` is `None` or empty, writes `DEPLOY_SKIPPED` to Delta and returns immediately.
# MAGIC 2. **Stage write** — Records `DEPLOY_STARTED` in Delta `genie_opt_stages`.
# MAGIC 3. **Deploy execution** — Applies the optimized configuration to the target (DABs integration placeholder — full implementation pending).
# MAGIC 4. **Completion** — Writes `DEPLOY_COMPLETE` stage record and returns deploy status.
# MAGIC
# MAGIC **Returns:** `{status, deploy_target}` where status is `DEPLOYED`, `SKIPPED`, or raises on failure.

# COMMAND ----------

try:
    _banner("Running _run_deploy")
    deploy_out = _run_deploy(
        w, spark, run_id, deploy_target, space_id, exp_name,
        domain, prev_model_id, iteration_counter,
        catalog, schema,
    )
    _log("Deploy result", **deploy_out)
except Exception as exc:
    _banner("Deploy FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

_banner("Task 5 Completed")
dbutils.notebook.exit(json.dumps(deploy_out, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Known Failure Modes
# MAGIC
# MAGIC ### 1. Empty `deploy_target`
# MAGIC
# MAGIC **Behavior:** Not a failure — the harness writes `DEPLOY_SKIPPED` to Delta and returns `{"status": "SKIPPED"}`. The condition task in the job definition should prevent this notebook from running at all when `deploy_target` is empty, but the harness handles it gracefully as a safety net.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Permission Errors
# MAGIC
# MAGIC **Cause:** The service principal lacks permissions to modify the target Genie Space or write to the deploy location.
# MAGIC
# MAGIC **Remediation:**
# MAGIC - Verify the service principal has appropriate permissions on the target workspace
# MAGIC - Check that the `deploy_target` path is valid and accessible
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Stale Model ID
# MAGIC
# MAGIC **Cause:** The `model_id` from lever_loop/baseline may reference a model version that was deleted or moved between tasks.
# MAGIC
# MAGIC **Remediation:**
# MAGIC - Check MLflow experiment for the model version
# MAGIC - Re-run finalize to generate a fresh model if needed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How to Interpret Error Logs
# MAGIC
# MAGIC 1. Look for `[TASK-5 DEPLOY] Failure details` — logs `error_type`, `error_message`, and full `traceback`
# MAGIC 2. Check Delta `genie_opt_stages` for `DEPLOY` stage records and `error_message` column
# MAGIC 3. Verify `deploy_target` is a valid, accessible path
# MAGIC
# MAGIC ### Remediation Checklist
# MAGIC
# MAGIC | Symptom | Action |
# MAGIC |---------|--------|
# MAGIC | Deploy skipped unexpectedly | Check `deploy_target` was set in preflight task values |
# MAGIC | Permission denied | Grant workspace permissions to the service principal |
# MAGIC | Model not found | Verify `model_id` exists in MLflow; re-run finalize if needed |
# MAGIC | Deploy task never ran | Check condition task (`deploy_check`) in job definition |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Task 5 (Deploy)** is a conditional step that applies the optimized Genie Space configuration to a target environment.
# MAGIC - **Input branching:** Reads `model_id` and `iteration_counter` from `lever_loop` if it ran, or `baseline_eval` if it was skipped. This mirrors the same branching logic as Task 4 (finalize).
# MAGIC - **Condition task:** The job definition gates deploy on a non-empty `deploy_target`. If empty, the task is skipped.
# MAGIC - **On success:** The optimized configuration is deployed and `DEPLOY_COMPLETE` is written to Delta.
# MAGIC - **On skip:** `DEPLOY_SKIPPED` is written to Delta; no configuration changes are made.
# MAGIC - **On failure:** Optimization results are preserved in Delta and MLflow; only the deploy step needs retry.
