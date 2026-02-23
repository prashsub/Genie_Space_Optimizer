# Databricks notebook source
# MAGIC %md
# MAGIC # Task 4: Finalize
# MAGIC
# MAGIC Runs repeatability test, promotes the best model, generates a comprehensive report,
# MAGIC and determines the final optimization status.

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import _run_finalize

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

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)

# COMMAND ----------

finalize_out = _run_finalize(
    w, spark, run_id, space_id, domain, exp_name,
    prev_scores, prev_model_id, iteration_counter,
    catalog, schema,
    run_repeatability=True,
    benchmarks=benchmarks,
)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="status", value=finalize_out["status"])
dbutils.jobs.taskValues.set(key="convergence_reason", value=finalize_out["convergence_reason"])
dbutils.jobs.taskValues.set(key="repeatability_pct", value=finalize_out["repeatability_pct"])
dbutils.jobs.taskValues.set(key="report_path", value=finalize_out.get("report_path", ""))

print(f"Finalize: status={finalize_out['status']}, repeatability={finalize_out['repeatability_pct']:.1f}%")
