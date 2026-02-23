# Databricks notebook source
# MAGIC %md
# MAGIC # Standalone Evaluation
# MAGIC
# MAGIC Runs evaluation independently (not part of the 5-task job).
# MAGIC Used by `run_evaluation_via_job()` when evaluation is submitted as a separate run.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from genie_space_optimizer.optimization.evaluation import (
    load_benchmarks_from_dataset,
    make_predict_fn,
    run_evaluation,
)
from genie_space_optimizer.optimization.scorers import make_all_scorers

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

# COMMAND ----------

predict_fn = make_predict_fn(w, space_id, spark, catalog, schema)
scorers = make_all_scorers(w, spark, catalog, schema)

# COMMAND ----------

result = run_evaluation(
    space_id, experiment_name, iteration, benchmarks,
    domain, model_id, eval_scope,
    predict_fn, scorers,
    catalog=catalog, gold_schema=schema, uc_schema=uc_schema,
)

# COMMAND ----------

print(f"Evaluation complete: accuracy={result['overall_accuracy']:.1f}%, thresholds_met={result['thresholds_met']}")
dbutils.notebook.exit(str(result.get("overall_accuracy", 0.0)))
