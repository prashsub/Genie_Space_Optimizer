# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2: Baseline Evaluation
# MAGIC
# MAGIC Runs the full 8-judge evaluation on iteration 0 and checks quality thresholds.

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.evaluation import load_benchmarks_from_dataset
from genie_space_optimizer.optimization.harness import _run_baseline

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

# COMMAND ----------

uc_schema = f"{catalog}.{schema}"
benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
if not benchmarks:
    raise RuntimeError(f"No benchmarks found in {uc_schema}.genie_benchmarks_{domain}")

# COMMAND ----------

baseline_out = _run_baseline(
    w, spark, run_id, space_id, benchmarks, exp_name, model_id,
    catalog, schema, domain,
)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="scores", value=json.dumps(baseline_out["scores"]))
dbutils.jobs.taskValues.set(key="overall_accuracy", value=baseline_out["overall_accuracy"])
dbutils.jobs.taskValues.set(key="thresholds_met", value=baseline_out["thresholds_met"])
dbutils.jobs.taskValues.set(key="model_id", value=baseline_out["model_id"])

print(f"Baseline: accuracy={baseline_out['overall_accuracy']:.1f}%, thresholds_met={baseline_out['thresholds_met']}")
