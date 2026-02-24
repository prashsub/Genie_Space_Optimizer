# Databricks notebook source
# MAGIC %md
# MAGIC # Standalone Evaluation
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook runs evaluation **independently** — it is **not** part of the 5-task optimization DAG (preflight → baseline_eval → lever_loop → finalize → deploy). It is used when evaluation needs to run in isolation, for example:
# MAGIC
# MAGIC - **`run_evaluation_via_job()`** — The harness submits this notebook as a Databricks Job run when evaluation is executed as a separate task (e.g., for multi-task architecture or Serverless compute).
# MAGIC - **Manual runs** — Operators can run this notebook directly with widget parameters for ad-hoc evaluation.
# MAGIC
# MAGIC ## Widget Parameters
# MAGIC
# MAGIC | Parameter | Type | Description |
# MAGIC |-----------|------|-------------|
# MAGIC | `space_id` | text | Genie Space ID to evaluate |
# MAGIC | `experiment_name` | text | MLflow experiment name for logging |
# MAGIC | `iteration` | text | Iteration number (default: "0") |
# MAGIC | `domain` | text | Domain (e.g., for benchmark filtering) |
# MAGIC | `model_id` | text | Optional model ID to associate with this eval |
# MAGIC | `eval_scope` | text | Evaluation scope: `full`, `slice`, or `p0` (default: "full") |
# MAGIC | `catalog` | text | Unity Catalog catalog for benchmarks and state |
# MAGIC | `schema` | text | Unity Catalog schema for benchmarks and state |
# MAGIC
# MAGIC ## SQL Context Setup
# MAGIC
# MAGIC Before creating the predict function and running evaluation, the notebook sets the Spark SQL context (`USE CATALOG` / `USE SCHEMA`). This is required for SQL Connect stability when querying Unity Catalog tables and running Genie predictions.
# MAGIC

# COMMAND ----------

import json
import traceback
from datetime import datetime, timezone
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.evaluation import (
    load_benchmarks_from_dataset,
    make_predict_fn,
    run_evaluation,
)
from genie_space_optimizer.optimization.scorers import make_all_scorers

dbutils = cast(Any, globals().get("dbutils"))


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _banner(title: str) -> None:
    print("\n" + "=" * 120)
    print(f"[{_ts()}] [EVALUATION] {title}")
    print("=" * 120)


def _log(event: str, **payload) -> None:
    print(f"[{_ts()}] [EVALUATION] {event}")
    if payload:
        print(json.dumps(payload, indent=2, default=str))


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_sql_context(spark: SparkSession, catalog: str, schema: str) -> None:
    """Set Spark SQL catalog/schema context explicitly for SQL Connect stability."""
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")

# COMMAND ----------

dbutils.widgets.text("space_id", "")
dbutils.widgets.text("experiment_name", "")
dbutils.widgets.text("iteration", "0")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("model_id", "")
dbutils.widgets.text("eval_scope", "full")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

space_id = dbutils.widgets.get("space_id")
experiment_name = dbutils.widgets.get("experiment_name")
iteration = int(dbutils.widgets.get("iteration") or "0")
domain = dbutils.widgets.get("domain")
model_id = dbutils.widgets.get("model_id") or None
eval_scope = dbutils.widgets.get("eval_scope") or "full"
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

_banner("Loaded Benchmarks")
_log("Benchmark dataset", uc_schema=uc_schema, domain=domain, benchmark_count=len(benchmarks))

# COMMAND ----------

_ensure_sql_context(spark, catalog, schema)
predict_fn = make_predict_fn(w, space_id, spark, catalog, schema)
scorers = make_all_scorers(w, spark, catalog, schema)

# COMMAND ----------

try:
    _banner("Running Evaluation")
    result = run_evaluation(
        space_id, experiment_name, iteration, benchmarks,
        domain, model_id, eval_scope,
        predict_fn, scorers,
        catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
    )
    _log(
        "Evaluation complete",
        overall_accuracy=result.get("overall_accuracy"),
        thresholds_met=result.get("thresholds_met"),
    )
except Exception as exc:
    _banner("Evaluation FAILED")
    _log(
        "Failure details",
        error_type=type(exc).__name__,
        error_message=str(exc),
        traceback=traceback.format_exc(),
    )
    raise

# COMMAND ----------

print(f"Evaluation complete: accuracy={result['overall_accuracy']:.1f}%, thresholds_met={result['thresholds_met']}")
dbutils.notebook.exit(str(result.get("overall_accuracy", 0.0)))
