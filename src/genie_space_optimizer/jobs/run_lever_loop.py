# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Lever Loop
# MAGIC
# MAGIC Iterates through levers 1-6 with convergence checking, patch application,
# MAGIC slice/P0 gates, rollback, and full evaluation. Skipped if baseline meets thresholds.

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import _run_lever_loop

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

# COMMAND ----------

if thresholds_met:
    print("Baseline already meets thresholds — skipping lever loop")
    dbutils.jobs.taskValues.set(key="scores", value=scores_json)
    dbutils.jobs.taskValues.set(key="accuracy", value=prev_accuracy)
    dbutils.jobs.taskValues.set(key="model_id", value=prev_model_id)
    dbutils.jobs.taskValues.set(key="iteration_counter", value=0)
    dbutils.jobs.taskValues.set(key="skipped", value=True)
    dbutils.notebook.exit("SKIPPED: baseline meets thresholds")

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
config = fetch_space_config(w, space_id)

# COMMAND ----------

loop_out = _run_lever_loop(
    w, spark, run_id, space_id, domain, benchmarks, exp_name,
    prev_scores, prev_accuracy, prev_model_id, config,
    catalog, schema, levers, max_iterations,
    apply_mode=apply_mode,
)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="scores", value=json.dumps(loop_out["scores"]))
dbutils.jobs.taskValues.set(key="accuracy", value=loop_out["accuracy"])
dbutils.jobs.taskValues.set(key="model_id", value=loop_out["model_id"])
dbutils.jobs.taskValues.set(key="iteration_counter", value=loop_out["iteration_counter"])
dbutils.jobs.taskValues.set(key="best_iteration", value=loop_out["best_iteration"])
dbutils.jobs.taskValues.set(key="skipped", value=False)

print(
    f"Lever loop: accuracy={loop_out['accuracy']:.1f}%, "
    f"accepted={loop_out['levers_accepted']}, "
    f"rolled_back={loop_out['levers_rolled_back']}"
)
