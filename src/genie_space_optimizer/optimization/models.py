"""
LoggedModel management — create, promote, rollback, and link scores.

Each evaluation iteration snapshots the Genie Space config + UC metadata
as an MLflow LoggedModel. This provides full lineage and one-click rollback.
"""

from __future__ import annotations

from contextlib import nullcontext
import json
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import mlflow

from genie_space_optimizer.common.config import MODEL_NAME_TEMPLATE, format_mlflow_template
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
    experiment_name: str | None = None,
    *,
    uc_schema: str,
    uc_columns: list[dict] | None = None,
    uc_tags: list[dict] | None = None,
    uc_routines: list[dict] | None = None,
    patch_set: list[dict] | None = None,
    parent_model_id: str | None = None,
    optimization_run_id: str = "",
) -> str:
    """Create an MLflow LoggedModel snapshot for a Genie Space iteration.

    Captures the full Genie config and UC metadata state as model params
    and artifacts. Returns the ``model_id`` string.
    """
    model_name = format_mlflow_template(MODEL_NAME_TEMPLATE, space_id=space_id)

    try:
        if experiment_name:
            mlflow.set_experiment(experiment_name)

        resolved_columns, resolved_tags, resolved_routines = _resolve_uc_metadata(
            config=config,
            uc_columns=uc_columns,
            uc_tags=uc_tags,
            uc_routines=uc_routines,
        )
        metadata_snapshot: dict[str, Any] = {
            "space_config": config,
            "iteration": iteration,
            "uc_schema": uc_schema,
            "uc_columns": resolved_columns,
            "uc_tags": resolved_tags,
            "uc_routines": resolved_routines,
            "patch_set": patch_set or [],
            "parent_model_id": parent_model_id,
        }
        artifact_prefix = f"model_snapshots/iter_{iteration}"

        active_run = mlflow.active_run()
        run_ctx = nullcontext(active_run) if active_run else mlflow.start_run(
            run_name=f"model_snapshot_iter_{iteration}",
        )
        with run_ctx as run:
            current_run = run if run is not None else mlflow.active_run()
            run_id = current_run.info.run_id if current_run else ""
            _log_dict_artifact(config, f"{artifact_prefix}/space_config.json")
            _log_dict_artifact(metadata_snapshot, f"{artifact_prefix}/metadata_snapshot.json")

            model = _initialize_logged_model(
                name=model_name,
                source_run_id=run_id or None,
                params={
                    "space_id": space_id,
                    "domain": domain,
                    "iteration": str(iteration),
                    "uc_schema": uc_schema,
                    "uc_columns_count": str(len(resolved_columns)),
                    "uc_tags_count": str(len(resolved_tags)),
                    "uc_routines_count": str(len(resolved_routines)),
                    "patch_count": str(len(patch_set or [])),
                    "parent_model_id": str(parent_model_id or ""),
                    "snapshot_run_id": run_id or "",
                    "space_config_artifact": f"{artifact_prefix}/space_config.json",
                    "metadata_artifact": f"{artifact_prefix}/metadata_snapshot.json",
                    # Backward-compatible key used by rollback fallback path.
                    "model_space_config": _safe_serialize(config),
                },
                tags={
                    "domain": domain,
                    "space_id": space_id,
                    "iteration": str(iteration),
                    "uc_schema": uc_schema,
                    "traceability": "genie_space_optimizer",
                    **({"genie.optimization_run_id": optimization_run_id} if optimization_run_id else {}),
                },
            )
            model_id = model.model_id
            _finalize_logged_model(model_id)

        logger.info(
            "Created LoggedModel %s (iter=%d, model_id=%s)",
            model_name, iteration, model_id,
        )
        return model_id

    except Exception:
        logger.exception("Failed to create LoggedModel for iter %d", iteration)
        return ""


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
    model_name = format_mlflow_template(MODEL_NAME_TEMPLATE, space_id=space_id)

    try:
        alias_fn = getattr(mlflow, "set_logged_model_alias", None)
        if callable(alias_fn):
            alias_fn(
                model_id=best_model_id,
                alias="champion",
            )
        else:
            logger.warning("mlflow.set_logged_model_alias is unavailable in this environment")
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
    """Link evaluation metrics to a LoggedModel.

    Logs metrics both on the active MLflow Run (for backward compat) and
    directly to the LoggedModel so they appear on the Model card in the UI.
    """
    try:
        for judge, score in scores.items():
            mlflow.log_metric(f"eval_{judge}", score)

        if model_id and model_id.startswith("m-"):
            try:
                mlflow.set_active_model(model_id=model_id)
                metrics_dict = {f"eval_{judge}": score for judge, score in scores.items()}
                mlflow.log_metrics(metrics_dict, model_id=model_id)
            except (TypeError, AttributeError):
                pass
            except Exception:
                logger.debug("Model-level metric logging not supported, run-level only", exc_info=True)

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


def _resolve_uc_metadata(
    *,
    config: dict,
    uc_columns: list[dict] | None,
    uc_tags: list[dict] | None,
    uc_routines: list[dict] | None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Resolve UC metadata from explicit args first, then prefetched snapshot."""
    prefetched = config.get("_prefetched_uc_metadata", {})
    prefetched_columns = prefetched.get("uc_columns", []) if isinstance(prefetched, dict) else []
    prefetched_tags = prefetched.get("uc_tags", []) if isinstance(prefetched, dict) else []
    prefetched_routines = prefetched.get("uc_routines", []) if isinstance(prefetched, dict) else []

    resolved_columns = uc_columns if isinstance(uc_columns, list) else (
        prefetched_columns if isinstance(prefetched_columns, list) else []
    )
    resolved_tags = uc_tags if isinstance(uc_tags, list) else (
        prefetched_tags if isinstance(prefetched_tags, list) else []
    )
    resolved_routines = uc_routines if isinstance(uc_routines, list) else (
        prefetched_routines if isinstance(prefetched_routines, list) else []
    )
    return resolved_columns, resolved_tags, resolved_routines


def _initialize_logged_model(
    *,
    name: str,
    source_run_id: str | None,
    params: dict[str, str],
    tags: dict[str, str],
) -> Any:
    """Create a logged model across MLflow API variants."""
    init_fn = getattr(mlflow, "initialize_logged_model", None)
    if callable(init_fn):
        return init_fn(
            name=name,
            source_run_id=source_run_id,
            params=params,
            tags=tags,
            model_type="agent",
        )

    create_fn = getattr(mlflow, "create_logged_model", None)
    if callable(create_fn):
        return create_fn(name=name, params=params, tags=tags)

    raise RuntimeError("No supported MLflow LoggedModel creation API found")


def _finalize_logged_model(model_id: str) -> None:
    """Finalize logged model if the MLflow API supports it."""
    finalize_fn = getattr(mlflow, "finalize_logged_model", None)
    if not callable(finalize_fn):
        return
    try:
        finalize_fn(model_id=model_id, status="READY")
    except Exception:
        logger.debug("Ignoring finalize_logged_model failure for %s", model_id, exc_info=True)


def _log_dict_artifact(payload: dict[str, Any], artifact_file: str) -> None:
    """Log dict artifact across MLflow API variants."""
    log_dict_fn = getattr(mlflow, "log_dict", None)
    if callable(log_dict_fn):
        log_dict_fn(payload, artifact_file)
        return

    tmp_dir = Path(tempfile.mkdtemp(prefix="genie-opt-"))
    tmp_file = tmp_dir / Path(artifact_file).name
    tmp_file.write_text(json.dumps(payload, default=str, indent=2), encoding="utf-8")
    mlflow.log_artifact(str(tmp_file), artifact_path=str(Path(artifact_file).parent))
