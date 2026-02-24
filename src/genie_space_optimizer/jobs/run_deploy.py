# Databricks notebook source
# MAGIC %md
# MAGIC # Task 5: Deploy
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Task 5 (Deploy) is a **conditional** step in the 5-task optimization DAG. It runs only when a deploy target is configured. Its purpose is to apply the optimized Genie space configuration to a target environment (e.g., via DABs) after optimization has completed successfully.
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
# MAGIC When `deploy_target` is set, deploy:
# MAGIC
# MAGIC - Applies the optimized configuration to the target Genie space (DABs integration placeholder — full implementation pending).
# MAGIC - Writes `DEPLOY_STARTED` / `DEPLOY_COMPLETE` stage records to Delta for audit.
# MAGIC - Returns `{"status": "DEPLOYED", "deploy_target": deploy_target}` on success.
# MAGIC
# MAGIC ## Condition Task
# MAGIC
# MAGIC The job definition uses a `condition_task` (e.g., `deploy_check`) that depends on `finalize` and checks `deploy_target`. The deploy notebook task depends on this condition with `outcome: "true"`. When the condition fails (no deploy target), the deploy task is skipped.
# MAGIC
# MAGIC ## Logging
# MAGIC
# MAGIC Logs all inputs and deploy outcomes in a structured format so operators can quickly diagnose task behavior in Workflows logs.

# COMMAND ----------

import json
import traceback
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.harness import _run_deploy


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _banner(title: str) -> None:
    print("\n" + "=" * 120)
    print(f"[{_ts()}] [TASK-5 DEPLOY] {title}")
    print("=" * 120)


def _log(event: str, **payload) -> None:
    print(f"[{_ts()}] [TASK-5 DEPLOY] {event}")
    if payload:
        print(json.dumps(payload, indent=2, default=str))

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

# Read from lever_loop
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
    prev_model_id=prev_model_id,
    iteration_counter=iteration_counter,
)

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
