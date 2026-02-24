# Databricks notebook source
# MAGIC %md
# MAGIC # Task 4: Finalize
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Task 4 (Finalize) is the penultimate stage of the Genie Space optimization pipeline. It performs **repeatability testing**, **model promotion**, and **final report generation** before the optional deploy step. This task ensures that the optimized configuration is validated for consistency and that all downstream consumers receive a complete, auditable summary of the run.
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC 1. **Repeatability testing** — Re-queries Genie for each benchmark question multiple times and compares SQL hashes across invocations to detect non-determinism. A high repeatability percentage indicates stable, consistent SQL generation.
# MAGIC 2. **Model promotion** — Promotes the best-performing model (by accuracy and repeatability) to the production registry.
# MAGIC 3. **Final report generation** — Produces a comprehensive report summarizing the run, including scores, convergence reason, and optimization history.
# MAGIC
# MAGIC ## Input Sources: lever_loop vs baseline_eval
# MAGIC
# MAGIC This task reads scores and model metadata from **either** `lever_loop` **or** `baseline_eval`, depending on whether the lever loop was skipped:
# MAGIC
# MAGIC - **If lever loop was skipped** (baseline already met all thresholds): reads from `baseline_eval` — scores, model_id, and iteration_counter=0.
# MAGIC - **If lever loop ran**: reads from `lever_loop` — scores, model_id, and iteration_counter from the last iteration.
# MAGIC
# MAGIC The `lever_skipped` task value from `lever_loop` determines which source to use.
# MAGIC
# MAGIC ## Timeout and Heartbeat Mechanism
# MAGIC
# MAGIC Finalize can be long-running (repeatability tests re-query Genie many times). To keep the task observable and avoid silent hangs:
# MAGIC
# MAGIC - **`FINALIZE_TIMEOUT_SECONDS`** (default: 6600, env: `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS`) — Soft timeout. If finalize exceeds this duration, it raises `TimeoutError` with an explicit terminal reason (`finalize_timeout`).
# MAGIC - **`FINALIZE_HEARTBEAT_SECONDS`** (default: 30, env: `GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS`) — Interval between heartbeat events. Heartbeats update `run.updated_at` and write `FINALIZE_HEARTBEAT` stage records so stale-state reconciliation does not mark the run as dead.
# MAGIC
# MAGIC ## Repeatability Testing
# MAGIC
# MAGIC Repeatability testing re-queries Genie for each benchmark question multiple times and compares the generated SQL hashes. It matters because:
# MAGIC
# MAGIC - Non-deterministic SQL generation can cause flaky evaluations and unreliable optimization decisions.
# MAGIC - A low repeatability score indicates the model or space configuration may need tuning.
# MAGIC - Results are written to Delta and included in the final report.
# MAGIC
# MAGIC ## Terminal Status Determination
# MAGIC
# MAGIC After repeatability and promotion, the harness resolves a **terminal status**:
# MAGIC
# MAGIC - **CONVERGED** — All thresholds were met (`threshold_met`).
# MAGIC - **MAX_ITERATIONS** — Iteration limit reached (`max_iterations`).
# MAGIC - **STALLED** — No further improvement possible (`no_further_improvement`).
# MAGIC - **FAILED** — Timeout or exception (`finalize_timeout`, `finalize_error`).
# MAGIC
# MAGIC The `terminal_reason` is published for downstream tasks and audit logs.
# MAGIC
# MAGIC ## Logging
# MAGIC
# MAGIC This task logs:
# MAGIC - Resolved upstream inputs and loop outputs
# MAGIC - Benchmark load summary
# MAGIC - Finalize result including heartbeat/terminal reason fields
# MAGIC
# MAGIC Fails loudly with structured diagnostics on any exception.

# COMMAND ----------

import json
import traceback
from datetime import datetime, timezone
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import _run_finalize

dbutils = cast(Any, globals().get("dbutils"))


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _banner(title: str) -> None:
    print("\n" + "=" * 120)
    print(f"[{_ts()}] [TASK-4 FINALIZE] {title}")
    print("=" * 120)


def _log(event: str, **payload) -> None:
    print(f"[{_ts()}] [TASK-4 FINALIZE] {event}")
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

# Read from lever_loop (or baseline if lever_loop was skipped)
lever_skipped = dbutils.jobs.taskValues.get(taskKey="lever_loop", key="skipped")
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
