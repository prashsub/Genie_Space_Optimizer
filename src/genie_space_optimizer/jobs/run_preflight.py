# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1: Preflight
# MAGIC
# MAGIC Fetches Genie Space config, UC metadata, generates/loads benchmarks,
# MAGIC creates MLflow experiment, registers judge prompts, creates iteration-0 model.

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.harness import _run_preflight
from genie_space_optimizer.optimization.state import ensure_optimization_tables

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("domain", "")
dbutils.widgets.text("experiment_name", "")
dbutils.widgets.text("max_iterations", "5")
dbutils.widgets.text("levers", "[1,2,3,4,5,6]")
dbutils.widgets.text("apply_mode", "genie_config")
dbutils.widgets.text("deploy_target", "")

run_id = dbutils.widgets.get("run_id")
space_id = dbutils.widgets.get("space_id")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
domain = dbutils.widgets.get("domain")
experiment_name = dbutils.widgets.get("experiment_name") or None
max_iterations = int(dbutils.widgets.get("max_iterations") or "5")
levers = json.loads(dbutils.widgets.get("levers") or "[1,2,3,4,5,6]")
apply_mode = dbutils.widgets.get("apply_mode") or "genie_config"
deploy_target = dbutils.widgets.get("deploy_target") or None

# COMMAND ----------

w = WorkspaceClient()
spark = SparkSession.builder.getOrCreate()

ensure_optimization_tables(spark, catalog, schema)

from genie_space_optimizer.optimization.state import create_run

create_run(
    spark, run_id, space_id, domain, catalog, schema,
    max_iterations=max_iterations,
    levers=levers,
    apply_mode=apply_mode,
    deploy_target=deploy_target,
)

# COMMAND ----------

preflight_out = _run_preflight(
    w, spark, run_id, space_id, catalog, schema, domain, experiment_name,
)

# COMMAND ----------

# Pass task values to downstream tasks
dbutils.jobs.taskValues.set(key="run_id", value=run_id)
dbutils.jobs.taskValues.set(key="space_id", value=space_id)
dbutils.jobs.taskValues.set(key="domain", value=domain)
dbutils.jobs.taskValues.set(key="catalog", value=catalog)
dbutils.jobs.taskValues.set(key="schema", value=schema)
dbutils.jobs.taskValues.set(key="experiment_name", value=preflight_out["experiment_name"])
dbutils.jobs.taskValues.set(key="experiment_id", value=preflight_out.get("experiment_id", ""))
dbutils.jobs.taskValues.set(key="model_id", value=preflight_out["model_id"])
dbutils.jobs.taskValues.set(key="benchmark_count", value=len(preflight_out["benchmarks"]))
dbutils.jobs.taskValues.set(key="max_iterations", value=max_iterations)
dbutils.jobs.taskValues.set(key="levers", value=json.dumps(levers))
dbutils.jobs.taskValues.set(key="apply_mode", value=apply_mode)
dbutils.jobs.taskValues.set(key="deploy_target", value=deploy_target or "")

print(f"Preflight complete: {len(preflight_out['benchmarks'])} benchmarks, model={preflight_out['model_id']}")
