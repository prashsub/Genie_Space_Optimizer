"""
Preflight logic — extracted from the harness to keep orchestration lean.

Fetches Genie Space config, UC metadata, loads or generates benchmarks,
validates SQL, registers judge prompts, and creates the initial MLflow
LoggedModel (iteration 0).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import mlflow

from genie_space_optimizer.common.config import (
    EXPERIMENT_PATH_TEMPLATE,
    TARGET_BENCHMARK_COUNT,
)
from genie_space_optimizer.common.genie_client import fetch_space_config
from genie_space_optimizer.common.uc_metadata import get_columns, get_routines, get_tags
from genie_space_optimizer.optimization.benchmarks import validate_benchmarks
from genie_space_optimizer.optimization.evaluation import (
    create_evaluation_dataset,
    generate_benchmarks,
    load_benchmarks_from_dataset,
    register_judge_prompts,
)
from genie_space_optimizer.optimization.models import create_genie_model_version
from genie_space_optimizer.optimization.state import write_stage

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def run_preflight(
    w: WorkspaceClient,
    spark: SparkSession,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    domain: str,
    experiment_name: str | None = None,
) -> tuple[dict, list[dict], str, str]:
    """Execute the full preflight sequence (Stage 1).

    Steps:
      1. Fetch Genie Space config via API
      2. Fetch UC metadata (columns, tags, routines)
      3. Resolve or create MLflow experiment
      4. Load or generate benchmarks
      5. Validate benchmark SQL via EXPLAIN
      6. Sync benchmarks to MLflow evaluation dataset
      7. Register judge prompts (idempotent)
      8. Create LoggedModel iteration 0
      9. Write PREFLIGHT_STARTED → PREFLIGHT_COMPLETE to Delta

    Returns:
        (config, benchmarks, model_id, experiment_name)
    """
    write_stage(
        spark, run_id, "PREFLIGHT_STARTED", "STARTED",
        task_key="preflight", catalog=catalog, schema=schema,
    )

    config = fetch_space_config(w, space_id)
    logger.info("Fetched config for space %s", space_id)

    uc_columns = get_columns(spark, catalog, schema).collect()
    uc_columns_dicts = [r.asDict() for r in uc_columns]
    uc_tags = get_tags(spark, catalog, schema).collect()
    uc_tags_dicts = [r.asDict() for r in uc_tags]
    uc_routines = get_routines(spark, catalog, schema).collect()
    uc_routines_dicts = [r.asDict() for r in uc_routines]
    logger.info(
        "UC metadata: %d columns, %d tags, %d routines",
        len(uc_columns_dicts), len(uc_tags_dicts), len(uc_routines_dicts),
    )

    uc_schema = f"{catalog}.{schema}"
    if experiment_name is None:
        try:
            user_email = w.current_user.me().user_name
        except Exception:
            user_email = "unknown"
        experiment_name = EXPERIMENT_PATH_TEMPLATE.format(
            user_email=user_email, domain=domain,
        )
    mlflow.set_experiment(experiment_name)
    exp = mlflow.get_experiment_by_name(experiment_name)
    experiment_id = exp.experiment_id if exp else ""
    logger.info("Experiment: %s (id=%s)", experiment_name, experiment_id)

    benchmarks = _load_or_generate_benchmarks(
        w, spark, config, uc_columns_dicts, uc_tags_dicts, uc_routines_dicts,
        domain, catalog, schema, uc_schema, run_id,
    )

    validation_results = validate_benchmarks(
        benchmarks, spark, catalog=catalog, gold_schema=schema,
    )
    valid_ids = {
        r["question"] for r in validation_results if r["valid"]
    }
    pre_count = len(benchmarks)
    benchmarks = [b for b in benchmarks if b.get("question") in valid_ids]
    if len(benchmarks) < pre_count:
        logger.warning(
            "Discarded %d benchmarks that failed EXPLAIN validation",
            pre_count - len(benchmarks),
        )

    create_evaluation_dataset(
        spark, benchmarks, uc_schema, domain,
        space_id=space_id, catalog=catalog, gold_schema=schema,
    )

    register_judge_prompts(uc_schema, domain, experiment_name)

    model_id = create_genie_model_version(
        w, space_id, config, iteration=0, domain=domain,
        uc_schema=uc_schema,
        uc_columns=uc_columns_dicts,
        uc_tags=uc_tags_dicts,
        uc_routines=uc_routines_dicts,
    )

    write_stage(
        spark, run_id, "PREFLIGHT_STARTED", "COMPLETE",
        task_key="preflight",
        detail={
            "benchmark_count": len(benchmarks),
            "experiment_name": experiment_name,
            "model_id": model_id,
        },
        catalog=catalog, schema=schema,
    )

    return config, benchmarks, model_id, experiment_name


def _load_or_generate_benchmarks(
    w: WorkspaceClient,
    spark: SparkSession,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    uc_schema: str,
    run_id: str,
) -> list[dict]:
    """Try loading existing benchmarks; fall back to LLM generation."""
    benchmarks = load_benchmarks_from_dataset(spark, uc_schema, domain)
    if benchmarks and len(benchmarks) >= 5:
        logger.info("Loaded %d existing benchmarks from UC dataset", len(benchmarks))
        return benchmarks

    logger.info("No existing benchmarks (or too few), generating via LLM")
    write_stage(
        spark, run_id, "BENCHMARK_GENERATION", "STARTED",
        task_key="preflight", catalog=catalog, schema=schema,
    )

    benchmarks = generate_benchmarks(
        w, config, uc_columns, uc_tags, uc_routines,
        domain, catalog, schema, spark,
        target_count=TARGET_BENCHMARK_COUNT,
    )

    write_stage(
        spark, run_id, "BENCHMARK_GENERATION", "COMPLETE",
        task_key="preflight",
        detail={"generated_count": len(benchmarks)},
        catalog=catalog, schema=schema,
    )
    return benchmarks
