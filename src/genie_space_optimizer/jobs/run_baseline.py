# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2: Baseline Evaluation
# MAGIC
# MAGIC ## Purpose
# MAGIC
# MAGIC This task establishes the **quality baseline** for the Genie Space before any optimization. It runs the full 8-judge evaluation suite against the benchmark dataset and records scores that the downstream **lever_loop** task will use to measure improvement.
# MAGIC
# MAGIC ## What Baseline Evaluation Does
# MAGIC
# MAGIC 1. **Reads upstream values** from the preflight task (`run_id`, `space_id`, `domain`, `catalog`, `schema`, `experiment_name`, `model_id`)
# MAGIC 2. **Loads benchmarks** from the UC table `{catalog}.{schema}.genie_benchmarks_{domain}`
# MAGIC 3. **Runs the 8-judge evaluation** via `mlflow.genai.evaluate()` with retry for transient harness failures — benchmark quarantine (SQL/routine gating) is now integrated inside `run_evaluation()` and shared across all eval scopes (baseline, slice, P0, full)
# MAGIC 4. **Checks thresholds** (syntax_validity, schema_accuracy, logical_accuracy, etc.)
# MAGIC 5. **Publishes task values** (`scores`, `overall_accuracy`, `thresholds_met`, `model_id`) for lever_loop
# MAGIC
# MAGIC ## 8-Judge Scoring System
# MAGIC
# MAGIC The evaluation uses 8 custom scorers passed to `mlflow.genai.evaluate(scorers=all_scorers)`:
# MAGIC
# MAGIC | Judge | Purpose |
# MAGIC |-------|---------|
# MAGIC | syntax_validity | SQL parses and executes without error |
# MAGIC | schema_accuracy | Correct tables, columns, joins |
# MAGIC | logical_accuracy | Correct aggregations, filters, GROUP BY, ORDER BY |
# MAGIC | semantic_equivalence | Same business metric even if written differently |
# MAGIC | completeness | No missing dimensions, measures, or filters |
# MAGIC | result_correctness | Query results match expected (hash/signature comparison) |
# MAGIC | asset_routing | Correct asset type (TABLE/MV/TVF) selected |
# MAGIC | arbiter | Tie-breaker when results disagree (LLM-based) |
# MAGIC
# MAGIC ## Place in the 5-Task DAG
# MAGIC
# MAGIC ```
# MAGIC preflight → baseline_eval (this task) → lever_loop → finalize → deploy
# MAGIC ```
# MAGIC
# MAGIC - **Depends on:** preflight (must complete first)
# MAGIC - **Feeds:** lever_loop (uses `scores`, `overall_accuracy`, `thresholds_met`, `model_id`)
# MAGIC
# MAGIC ## What Happens If This Task Fails
# MAGIC
# MAGIC - The job stops; lever_loop never runs
# MAGIC - Delta state is updated with `BASELINE_EVAL` = FAILED
# MAGIC - Run status is set to FAILED with `convergence_reason=error_in_BASELINE_EVAL`
# MAGIC - This task is expected to **fail loudly** when infrastructure-level evaluation issues are detected (see Known Failure Modes below)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture: How Baseline Works
# MAGIC
# MAGIC ### Data Flow
# MAGIC
# MAGIC 1. **Inputs from preflight** — `dbutils.jobs.taskValues.get(taskKey="preflight", key="...")` provides:
# MAGIC    - `run_id`, `space_id`, `domain`, `catalog`, `schema`, `experiment_name`, `model_id`
# MAGIC
# MAGIC 2. **`_run_baseline()`** (in `optimization.harness`) performs:
# MAGIC    - **Predict function creation** — `make_predict_fn(w, space_id, spark, catalog, schema)` returns a closure that: rate-limits, calls Genie, executes both SQLs, normalizes results, compares hashes
# MAGIC    - **Scorer assembly** — `make_all_scorers(w, spark, catalog, schema)` returns the 8-judge list for `mlflow.genai.evaluate(scorers=...)`
# MAGIC    - **Evaluation with retry** — `run_evaluation()` wraps `mlflow.genai.evaluate()` with `_run_evaluate_with_retries()`: up to 4 attempts, exponential backoff, single-worker fallback on retries for transient harness bugs. Benchmark quarantine (SQL/routine gating) is now integrated inside `run_evaluation()` — invalid benchmarks are quarantined within the shared evaluation path rather than as a separate pre-step.
# MAGIC    - **Threshold checking** — Compares per-judge means against `MLFLOW_THRESHOLDS`; sets `thresholds_met` boolean
# MAGIC    - **State writes** — Writes iteration 0 to Delta, links scores to model, updates run status
# MAGIC
# MAGIC 3. **Results flow to lever_loop** — Task values `scores`, `overall_accuracy`, `thresholds_met`, `model_id` are consumed by lever_loop to decide whether to proceed and to compare against post-lever scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Known Failure Modes (Read This If Baseline Fails Often)
# MAGIC
# MAGIC This task fails more often than others because it touches MLflow GenAI harness, Spark SQL, and Genie APIs. Below are the main failure modes and how to handle them.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. `AttributeError: 'NoneType' object has no attribute 'info'`
# MAGIC
# MAGIC **Cause:** Transient bug inside `mlflow.genai.evaluation.harness`. The harness sometimes receives `eval_item.trace` or `eval_item.trace.info` as `None` and accesses `.info` or `.assessments` on it.
# MAGIC
# MAGIC **Handling:** The evaluation engine wraps `mlflow.genai.evaluate()` in `_run_evaluate_with_retries()`. Exceptions matching this pattern are classified as **retryable** via `_is_retryable_eval_exception()`. The engine will:
# MAGIC - Retry up to 4 times (configurable via `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS`)
# MAGIC - Sleep 10s × attempt between retries (configurable via `GENIE_SPACE_OPTIMIZER_EVAL_RETRY_SLEEP_SECONDS`)
# MAGIC - Fall back to single-worker mode on retries (`GENIE_SPACE_OPTIMIZER_EVAL_RETRY_WORKERS=1`) to reduce concurrency-related races
# MAGIC
# MAGIC **Interpretation:** If you see this error in logs and the task still fails, it means all retries exhausted. Check:
# MAGIC - Cluster size and load (try a larger cluster or fewer parallel jobs)
# MAGIC - MLflow/GenAI service health
# MAGIC - Increase `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS` (e.g. 6) and `GENIE_SPACE_OPTIMIZER_EVAL_RETRY_SLEEP_SECONDS` (e.g. 15)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Infrastructure SQL Errors (Catalog/Schema Context)
# MAGIC
# MAGIC **Cause:** Spark SQL context not set correctly. Errors like:
# MAGIC - `"TABLE_OR_VIEW_NOT_FOUND"` / `"not in the current catalog"`
# MAGIC - `"please set the current catalog"`
# MAGIC - `"catalog does not exist"` / `"schema does not exist"`
# MAGIC - `"permission denied"` / `"insufficient_permissions"`
# MAGIC
# MAGIC These are detected by `_is_infrastructure_sql_error()` in the evaluation module.
# MAGIC
# MAGIC **Handling:** `_run_baseline()` calls `_ensure_sql_context(spark, catalog, schema)` before evaluation to set `USE CATALOG` and `USE SCHEMA`. If you still see these errors:
# MAGIC - Verify the job runs with a user/service principal that has `USE CATALOG` and `USE SCHEMA` on the target catalog/schema
# MAGIC - Ensure the catalog and schema exist and are accessible from the cluster
# MAGIC - Check that benchmark `expected_sql` uses `{catalog}.{schema}` template variables correctly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. `FAIL_ON_INFRA_EVAL_ERRORS` Behavior
# MAGIC
# MAGIC **Env var:** `GENIE_SPACE_OPTIMIZER_FAIL_ON_INFRA_EVAL_ERRORS` (default: `true`)
# MAGIC
# MAGIC When `true`, if **any** evaluation row contains an infrastructure SQL error (detected in `outputs/comparison/error` or scorer rationale/metadata), the evaluation **fails immediately** with:
# MAGIC
# MAGIC ```
# MAGIC RuntimeError: Infrastructure SQL errors detected during evaluation: <first 3 error messages>
# MAGIC ```
# MAGIC
# MAGIC This is intentional: we do not want to report "passing" scores when some rows failed due to catalog/schema/permission issues.
# MAGIC
# MAGIC **To allow evaluation to complete despite infra errors** (e.g. for debugging): set `GENIE_SPACE_OPTIMIZER_FAIL_ON_INFRA_EVAL_ERRORS=false`. Scores will still be computed for rows that succeeded; infra errors will be logged but will not abort the run.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. gRPC / Spark Connect Timeouts
# MAGIC
# MAGIC **Cause:** Transient network or Spark Connect issues during scorer execution.
# MAGIC
# MAGIC **Handling:** These are also classified as retryable. If `_multithreadedrendezvous` or `grpc` appears in the error message, the retry loop will attempt again.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How to Interpret Error Logs
# MAGIC
# MAGIC 1. Look for `[TASK-2 BASELINE] Failure details` — it logs `error_type`, `error_message`, and full `traceback`
# MAGIC 2. Check `_eval_attempts` on the exception (if present) — shows each attempt's status, workers, and error
# MAGIC 3. Check MLflow run artifacts: `evaluation_failure/infrastructure_sql_errors.json` if infra errors were detected
# MAGIC 4. Check Delta `genie_opt_stages` for `BASELINE_EVAL` = FAILED and the `error_message` column
# MAGIC
# MAGIC ### Remediation Checklist
# MAGIC
# MAGIC | Symptom | Action |
# MAGIC |---------|--------|
# MAGIC | `AttributeError` / `NoneType` / `info` | Retry; increase `EVAL_MAX_ATTEMPTS`; use single-worker fallback |
# MAGIC | `not in the current catalog` | Fix catalog/schema context; verify permissions |
# MAGIC | `permission denied` | Grant `USE CATALOG`, `USE SCHEMA`, `SELECT` on benchmark tables |
# MAGIC | gRPC / timeout | Retry; check cluster/network; reduce benchmark count for large runs |
# MAGIC | No benchmarks found | Ensure `genie_benchmarks_{domain}` exists and has rows for the domain |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and Helpers
# MAGIC
# MAGIC - `_ts()` — UTC timestamp for log lines
# MAGIC - `_banner()` — Section separator with task label
# MAGIC - `_log()` — Structured logging with optional JSON payload

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
from genie_space_optimizer.optimization.harness import _run_baseline

dbutils = cast(Any, globals().get("dbutils"))

_TASK_LABEL = "TASK-2 BASELINE"
_banner = partial(_banner_base, _TASK_LABEL)
_log = partial(_log_base, _TASK_LABEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Upstream Task Values from Preflight
# MAGIC
# MAGIC All values required for baseline evaluation are published by the preflight task. If any key is missing, `dbutils.jobs.taskValues.get()` returns `None` and downstream logic will fail with a clear error.

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

# Read task values from preflight
run_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="run_id")
space_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="space_id")
domain = dbutils.jobs.taskValues.get(taskKey="preflight", key="domain")
catalog = dbutils.jobs.taskValues.get(taskKey="preflight", key="catalog")
schema = dbutils.jobs.taskValues.get(taskKey="preflight", key="schema")
exp_name = dbutils.jobs.taskValues.get(taskKey="preflight", key="experiment_name")
model_id = dbutils.jobs.taskValues.get(taskKey="preflight", key="model_id")

_banner("Resolved Upstream Task Values")
_log(
    "Inputs",
    run_id=run_id,
    space_id=space_id,
    domain=domain,
    catalog=catalog,
    schema=schema,
    experiment_name=exp_name,
    model_id=model_id,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Benchmarks
# MAGIC
# MAGIC Benchmarks are loaded from the UC table `{catalog}.{schema}.genie_benchmarks_{domain}`. Each row is a benchmark question with `question`, `expected_sql`, `expected_asset`, and metadata. If no benchmarks are found, the task raises immediately.

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
_banner("Loaded Benchmarks")
_log("Benchmark dataset", uc_schema=uc_schema, domain=domain, benchmark_count=len(benchmarks))
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Baseline Evaluation
# MAGIC
# MAGIC `_run_baseline()` creates the predict function and scorers, runs `mlflow.genai.evaluate()` with retry (benchmark quarantine is integrated inside the evaluation path), checks thresholds, and writes Delta state. On success it returns `scores`, `overall_accuracy`, `thresholds_met`, and `model_id`. On failure it re-raises after logging full details.

# COMMAND ----------

try:
    _banner("Running _run_baseline")
    baseline_out = _run_baseline(
        w, spark, run_id, space_id, benchmarks, exp_name, model_id,
        catalog, schema, domain,
    )
    _log(
        "Baseline finished",
        overall_accuracy=baseline_out["overall_accuracy"],
        thresholds_met=baseline_out["thresholds_met"],
        model_id=baseline_out["model_id"],
        judge_count=len(baseline_out.get("scores", {})),
    )
except Exception as exc:
    _banner("Baseline FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish Task Values for Downstream
# MAGIC
# MAGIC The lever_loop task reads these keys via `dbutils.jobs.taskValues.get(taskKey="baseline_eval", key="...")`. All values must be published for the DAG to proceed correctly.

# COMMAND ----------

_banner("Publishing Task Values")
dbutils.jobs.taskValues.set(key="scores", value=json.dumps(baseline_out["scores"]))
dbutils.jobs.taskValues.set(key="overall_accuracy", value=baseline_out["overall_accuracy"])
dbutils.jobs.taskValues.set(key="thresholds_met", value=baseline_out["thresholds_met"])
dbutils.jobs.taskValues.set(key="model_id", value=baseline_out["model_id"])

_log(
    "Task values published",
    overall_accuracy=baseline_out["overall_accuracy"],
    thresholds_met=baseline_out["thresholds_met"],
    model_id=baseline_out["model_id"],
)
_banner("Task 2 Completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Task 2 (Baseline Evaluation) has completed successfully. The lever_loop task will use the published scores and thresholds to:
# MAGIC - Decide whether baseline already meets thresholds (early exit possible)
# MAGIC - Compare post-lever scores against baseline to detect regressions
# MAGIC - Track the best model and accuracy across iterations
