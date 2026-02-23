# Databricks notebook source
# MAGIC %md
# MAGIC # Task 5: Deploy
# MAGIC
# MAGIC Optional deployment via DABs. Skipped when no deploy_target is set.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.harness import _run_deploy

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

# COMMAND ----------

deploy_out = _run_deploy(
    w, spark, run_id, deploy_target, space_id, exp_name,
    domain, prev_model_id, iteration_counter,
    catalog, schema,
)

print(f"Deploy: {deploy_out['status']}")
