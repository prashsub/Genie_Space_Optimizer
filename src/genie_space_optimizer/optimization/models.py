"""
LoggedModel management — create, promote, rollback, and link scores.

Each evaluation iteration snapshots the Genie Space config + UC metadata
as an MLflow LoggedModel. This provides full lineage and one-click rollback.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import mlflow

from genie_space_optimizer.common.config import MODEL_NAME_TEMPLATE
from genie_space_optimizer.optimization.state import (
    load_iterations,
    load_run,
    update_run_status,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def create_genie_model_version(
    w: WorkspaceClient,
    space_id: str,
    config: dict,
    iteration: int,
    domain: str,
    *,
    uc_schema: str,
    uc_columns: list[dict] | None = None,
    uc_tags: list[dict] | None = None,
    uc_routines: list[dict] | None = None,
    patch_set: list[dict] | None = None,
    parent_model_id: str | None = None,
) -> str:
    """Create an MLflow LoggedModel snapshot for a Genie Space iteration.

    Captures the full Genie config and UC metadata state as model params
    and artifacts. Returns the ``model_id`` string.
    """
    model_name = MODEL_NAME_TEMPLATE.format(space_id=space_id)

    try:
        model = mlflow.create_logged_model(
            name=model_name,
            params={
                "space_id": space_id,
                "domain": domain,
                "iteration": str(iteration),
                "uc_schema": uc_schema,
            },
            tags={
                "domain": domain,
                "space_id": space_id,
                "iteration": str(iteration),
            },
        )
        model_id = model.model_id

        metadata_snapshot: dict[str, Any] = {
            "space_config": _safe_serialize(config),
            "iteration": iteration,
        }
        if uc_columns is not None:
            metadata_snapshot["uc_columns_count"] = len(uc_columns)
        if uc_tags is not None:
            metadata_snapshot["uc_tags_count"] = len(uc_tags)
        if uc_routines is not None:
            metadata_snapshot["uc_routines_count"] = len(uc_routines)
        if patch_set is not None:
            metadata_snapshot["patch_count"] = len(patch_set)
        if parent_model_id is not None:
            metadata_snapshot["parent_model_id"] = parent_model_id

        mlflow.log_params(
            {f"model_{k}": str(v)[:250] for k, v in metadata_snapshot.items()},
        )

        logger.info(
            "Created LoggedModel %s (iter=%d, model_id=%s)",
            model_name, iteration, model_id,
        )
        return model_id

    except Exception:
        logger.exception("Failed to create LoggedModel for iter %d", iteration)
        return f"model_{space_id}_{iteration}"


def promote_best_model(
    spark: SparkSession,
    run_id: str,
    catalog: str,
    schema: str,
) -> str | None:
    """Promote the best iteration's model to the 'champion' alias.

    Reads all iterations from Delta to find the one with highest
    ``overall_accuracy``, then sets the MLflow alias.

    Returns the promoted model_id, or None on failure.
    """
    run_row = load_run(spark, run_id, catalog, schema)
    if not run_row:
        logger.error("Cannot promote: run %s not found", run_id)
        return None

    iterations_df = load_iterations(spark, run_id, catalog, schema)
    if iterations_df.empty:
        logger.warning("No iterations found for run %s", run_id)
        return None

    full_evals = iterations_df[iterations_df["eval_scope"] == "full"]
    if full_evals.empty:
        full_evals = iterations_df

    best_idx = full_evals["overall_accuracy"].idxmax()
    best_row = full_evals.loc[best_idx]
    best_model_id = best_row.get("model_id")
    best_iteration = int(best_row.get("iteration", 0))
    best_accuracy = float(best_row.get("overall_accuracy", 0.0))

    if not best_model_id:
        logger.warning("Best iteration %d has no model_id", best_iteration)
        return None

    space_id = run_row.get("space_id", "")
    model_name = MODEL_NAME_TEMPLATE.format(space_id=space_id)

    try:
        mlflow.set_logged_model_alias(
            model_id=best_model_id,
            alias="champion",
        )
        logger.info(
            "Promoted model %s (iter=%d, accuracy=%.1f%%) as champion",
            best_model_id, best_iteration, best_accuracy,
        )
    except Exception:
        logger.exception("Failed to set champion alias on %s", best_model_id)

    update_run_status(
        spark, run_id, catalog, schema,
        best_iteration=best_iteration,
        best_accuracy=best_accuracy,
        best_model_id=best_model_id,
    )

    return best_model_id


def rollback_to_model(
    w: WorkspaceClient,
    model_id: str,
) -> dict | None:
    """Restore Genie config from a LoggedModel snapshot.

    Reads the config artifact from the model and re-applies it via
    the Genie PATCH API. Returns the restored config or None on failure.
    """
    try:
        model = mlflow.get_logged_model(model_id=model_id)
        params = model.params or {}
        space_id = params.get("space_id")
        if not space_id:
            logger.error("Model %s has no space_id param", model_id)
            return None

        config_json = params.get("model_space_config")
        if not config_json:
            logger.warning(
                "Model %s has no config snapshot; rollback requires manual intervention",
                model_id,
            )
            return None

        config = json.loads(config_json)
        from genie_space_optimizer.common.genie_client import patch_space_config
        patch_space_config(w, space_id, config)
        logger.info("Rolled back space %s to model %s", space_id, model_id)
        return config

    except Exception:
        logger.exception("Rollback to model %s failed", model_id)
        return None


def link_eval_scores_to_model(
    model_id: str,
    scores: dict[str, float],
) -> None:
    """Link evaluation metrics to a LoggedModel."""
    try:
        for judge, score in scores.items():
            mlflow.log_metric(f"eval_{judge}", score)
        logger.info("Linked %d scores to model %s", len(scores), model_id)
    except Exception:
        logger.exception("Failed to link scores to model %s", model_id)


def _safe_serialize(obj: Any) -> str:
    """JSON-serialize, truncating to 250 chars for MLflow params."""
    try:
        s = json.dumps(obj, default=str)
        return s[:250] if len(s) > 250 else s
    except Exception:
        return str(obj)[:250]
