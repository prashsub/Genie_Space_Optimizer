"""MLflow Labeling Sessions for human-in-the-loop optimization review.

Provides:
  - Custom labeling schemas for Genie Space optimization
  - Auto-creation of review sessions after lever loop runs
  - Ingestion of human feedback to improve subsequent optimization runs

Uses the MLflow 3.x labeling API:
  - mlflow.genai.labeling.create_labeling_session()
  - session.add_dataset() / session.add_traces()
  - session.sync(dataset_name=...)
  - mlflow.genai.labeling.get_labeling_sessions()
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import mlflow

logger = logging.getLogger(__name__)

SCHEMA_JUDGE_VERDICT = "judge_verdict_accuracy"
SCHEMA_CORRECTED_SQL = "corrected_expected_sql"
SCHEMA_PATCH_APPROVAL = "patch_approval"
SCHEMA_IMPROVEMENTS = "improvement_suggestions"

ALL_SCHEMA_NAMES = [
    SCHEMA_JUDGE_VERDICT,
    SCHEMA_CORRECTED_SQL,
    SCHEMA_PATCH_APPROVAL,
    SCHEMA_IMPROVEMENTS,
]


def _create_or_reuse_schema(schemas_mod: Any, **kwargs: Any) -> bool:
    """Create a label schema if it doesn't exist; reuse it if it does.

    MLflow's ``create_label_schema`` internally deletes-then-recreates when
    a schema with the same name exists, which fails when a labeling session
    references the schema.  We check existence first via ``get_label_schema``
    and treat "referenced by labeling sessions" errors as proof the schema
    already exists.
    """
    name = kwargs["name"]
    kwargs.pop("overwrite", None)
    try:
        existing = schemas_mod.get_label_schema(name)
        if existing is not None:
            logger.info("Label schema '%s' already exists — reusing", name)
            return True
    except Exception:
        pass

    try:
        schemas_mod.create_label_schema(**kwargs, overwrite=False)
        logger.info("Created label schema '%s'", name)
        return True
    except Exception as exc:
        err_lower = str(exc).lower()
        _exists_signals = [
            "already exists",
            "referenced by",
            "cannot rename or remove",
            "labeling sessions",
        ]
        if any(s in err_lower for s in _exists_signals):
            logger.info(
                "Label schema '%s' already exists (confirmed by error) — reusing", name,
            )
            return True
        logger.warning("Failed to create label schema '%s': %s", name, exc)
        return False


def ensure_labeling_schemas() -> list[str]:
    """Create or reuse custom labeling schemas for Genie optimization review.

    Returns the list of schema names that are available (created or pre-existing).
    Gracefully degrades if the MLflow version doesn't support labeling.
    """
    try:
        import mlflow.genai.label_schemas as schemas
        from mlflow.genai.label_schemas import (
            InputCategorical,
            InputText,
            InputTextList,
        )
    except ImportError:
        logger.warning("mlflow.genai.label_schemas not available — skipping schema creation")
        return []

    available: list[str] = []

    if _create_or_reuse_schema(
        schemas,
        name=SCHEMA_JUDGE_VERDICT,
        type="feedback",
        title="Is the judge's verdict correct for this question?",
        input=InputCategorical(options=[
            "Correct - judge is right",
            "Wrong - Genie answer is actually fine",
            "Wrong - both answers are wrong",
            "Ambiguous - question is unclear",
        ]),
        instruction=(
            "Review the benchmark question, expected SQL, Genie-generated SQL, "
            "and each judge's rationale. Was the overall verdict correct?"
        ),
        enable_comment=True,
    ):
        available.append(SCHEMA_JUDGE_VERDICT)

    if _create_or_reuse_schema(
        schemas,
        name=SCHEMA_CORRECTED_SQL,
        type="expectation",
        title="Provide the correct expected SQL (if benchmark is wrong)",
        input=InputText(),
        instruction=(
            "If the benchmark's expected SQL is incorrect or suboptimal, "
            "provide the corrected SQL. Leave blank if the benchmark is correct."
        ),
    ):
        available.append(SCHEMA_CORRECTED_SQL)

    if _create_or_reuse_schema(
        schemas,
        name=SCHEMA_PATCH_APPROVAL,
        type="feedback",
        title="Should the proposed optimization patch be applied?",
        input=InputCategorical(options=["Approve", "Reject", "Modify"]),
        instruction=(
            "Review the proposed metadata change (column description, instruction, "
            "join condition, etc.). Approve it, reject it, or suggest modifications."
        ),
        enable_comment=True,
    ):
        available.append(SCHEMA_PATCH_APPROVAL)

    if _create_or_reuse_schema(
        schemas,
        name=SCHEMA_IMPROVEMENTS,
        type="expectation",
        title="Suggest improvements for this Genie Space",
        input=InputTextList(max_count=5, max_length_each=500),
        instruction=(
            "Provide specific, actionable improvements: better column descriptions, "
            "missing join conditions, instruction changes, or table-level fixes."
        ),
    ):
        available.append(SCHEMA_IMPROVEMENTS)

    logger.info("Ensured %d labeling schemas: %s", len(available), ", ".join(available))
    return available


def create_review_session(
    run_id: str,
    domain: str,
    experiment_name: str,
    uc_schema: str,
    failure_trace_ids: list[str],
    regression_trace_ids: list[str],
    reviewers: list[str] | None = None,
    eval_mlflow_run_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Create a labeling session populated with evaluation traces for human review.

    Uses *eval_mlflow_run_ids* (the MLflow run IDs from each evaluation)
    as the primary source of traces.  This is far more reliable than
    searching the whole experiment and filtering by individual trace IDs,
    which can fail due to format mismatches, indexing lag, or the 200-trace
    cap on ``mlflow.search_traces()``.

    Falls back to experiment-wide search when no eval run IDs are provided.

    Returns dict with session_name, session_run_id, session_url,
    trace_count (or empty on failure).
    """
    try:
        import mlflow.genai.labeling as labeling
    except ImportError:
        logger.warning("mlflow.genai.labeling not available — skipping session creation")
        return {}

    schema_names = ensure_labeling_schemas()
    if not schema_names:
        logger.warning("No labeling schemas created; skipping session")
        return {}

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_name = f"genie_opt_review_{domain}_{run_id[:8]}_{ts}"

    logger.info(
        "Creating labeling session %s — %d failure tids, %d regression tids, %d eval run ids",
        session_name, len(failure_trace_ids), len(regression_trace_ids),
        len(eval_mlflow_run_ids or []),
    )

    try:
        session = labeling.create_labeling_session(
            name=session_name,
            label_schemas=schema_names,
            assigned_users=reviewers or [],
        )
    except Exception:
        logger.exception("Failed to create labeling session")
        return {}

    traces_added = 0
    try:
        traces_added = _populate_session_traces(
            session=session,
            session_name=session_name,
            experiment_name=experiment_name,
            failure_trace_ids=failure_trace_ids,
            regression_trace_ids=regression_trace_ids,
            eval_mlflow_run_ids=eval_mlflow_run_ids or [],
        )
    except Exception:
        logger.exception("Failed to add traces to labeling session")

    session_run_id = getattr(session, "mlflow_run_id", "")
    session_id = getattr(session, "labeling_session_id", "")
    session_url = getattr(session, "url", "")

    logger.info(
        "Created labeling session: %s (run_id=%s, session_id=%s, traces=%d, url=%s)",
        session_name, session_run_id, session_id, traces_added,
        session_url or "(none)",
    )
    return {
        "session_name": session_name,
        "session_run_id": session_run_id,
        "labeling_session_id": session_id,
        "session_url": session_url,
        "trace_count": traces_added,
    }


_MAX_SESSION_TRACES = 100


def _populate_session_traces(
    *,
    session: Any,
    session_name: str,
    experiment_name: str,
    failure_trace_ids: list[str],
    regression_trace_ids: list[str],
    eval_mlflow_run_ids: list[str],
) -> int:
    """Collect traces from eval runs and add them to the labeling session.

    Strategy:
      1. If *eval_mlflow_run_ids* are available, search traces per eval run.
         This is the reliable path — each eval run's traces are directly
         addressable by ``mlflow.search_traces(run_id=...)``.
      2. Fall back to experiment-wide search (legacy path) when no run IDs
         are provided.

    Within the collected traces, priority-filter by failure/regression
    trace IDs, then backfill with passing traces for spot-checking.
    """
    import pandas as pd

    all_traces: pd.DataFrame | None = None

    if eval_mlflow_run_ids:
        frames: list[pd.DataFrame] = []
        unique_run_ids = list(dict.fromkeys(eval_mlflow_run_ids))
        for rid in unique_run_ids:
            try:
                run_traces = mlflow.search_traces(run_id=rid)
                if run_traces is not None and len(run_traces) > 0:
                    logger.info("Found %d traces in eval run %s", len(run_traces), rid)
                    frames.append(run_traces)
            except Exception:
                logger.warning("Failed to search traces for eval run %s", rid, exc_info=True)
        if frames:
            all_traces = pd.concat(frames, ignore_index=True)
            all_traces = all_traces.drop_duplicates(subset=["trace_id"], keep="first")
            logger.info(
                "Collected %d unique traces from %d eval runs",
                len(all_traces), len(unique_run_ids),
            )

    if all_traces is None or len(all_traces) == 0:
        logger.info("Falling back to experiment-wide trace search for session %s", session_name)
        exp = mlflow.get_experiment_by_name(experiment_name)
        if not exp:
            logger.warning("Experiment '%s' not found — session will be empty", experiment_name)
            return 0
        all_traces = mlflow.search_traces(
            experiment_ids=[exp.experiment_id],
            max_results=500,
        )
        if all_traces is None or len(all_traces) == 0:
            logger.warning("No traces found for experiment '%s'", experiment_name)
            return 0

    priority_set = set(
        dict.fromkeys(regression_trace_ids + failure_trace_ids)
    )

    if priority_set and "trace_id" in all_traces.columns:
        priority_mask = all_traces["trace_id"].isin(priority_set)
        priority_traces = all_traces[priority_mask]
        other_traces = all_traces[~priority_mask]
    else:
        priority_traces = pd.DataFrame()
        other_traces = all_traces

    remaining = _MAX_SESSION_TRACES - len(priority_traces)
    if remaining > 0 and len(other_traces) > 0:
        backfill = other_traces.head(remaining)
        combined = pd.concat([priority_traces, backfill], ignore_index=True)
    else:
        combined = priority_traces.head(_MAX_SESSION_TRACES)

    if len(combined) == 0:
        logger.warning("No traces to add to labeling session %s", session_name)
        return 0

    session.add_traces(combined)
    logger.info(
        "Added %d traces to labeling session %s (%d priority + %d backfill)",
        len(combined), session_name,
        len(priority_traces), len(combined) - len(priority_traces),
    )
    return len(combined)


def _find_session_by_name(session_name: str) -> Any | None:
    """Find a labeling session by name. Returns None if not found."""
    try:
        import mlflow.genai.labeling as labeling
        all_sessions = labeling.get_labeling_sessions()
        for s in all_sessions:
            if s.name == session_name:
                return s
    except Exception:
        logger.debug("Failed to search labeling sessions", exc_info=True)
    return None


def ingest_human_feedback(
    session_name: str,
) -> dict[str, Any]:
    """Read human labels from a labeling session and return actionable feedback.

    Finds the session by name, then reads traces via the session's
    ``mlflow_run_id`` to extract assessments.

    Returns a dict with:
      - corrections: list of actionable items
      - total_reviewed: number of traces reviewed
    """
    session = _find_session_by_name(session_name)
    if session is None:
        logger.info("Labeling session '%s' not found — no feedback to ingest", session_name)
        return {"corrections": [], "total_reviewed": 0}

    session_run_id = getattr(session, "mlflow_run_id", "")
    if not session_run_id:
        logger.warning("Labeling session '%s' has no mlflow_run_id", session_name)
        return {"corrections": [], "total_reviewed": 0}

    try:
        traces_df = mlflow.search_traces(run_id=session_run_id)
    except Exception:
        logger.exception("Failed to search traces for session %s", session_name)
        return {"corrections": [], "total_reviewed": 0}

    if traces_df is None or len(traces_df) == 0:
        return {"corrections": [], "total_reviewed": 0}

    corrections: list[dict[str, Any]] = []
    total_reviewed = len(traces_df)

    for _, row in traces_df.iterrows():
        assessments = row.get("assessments")
        if not assessments:
            continue
        if not isinstance(assessments, list):
            try:
                assessments = list(assessments)
            except (TypeError, ValueError):
                continue

        trace_id = row.get("trace_id", "")
        for a in assessments:
            a_name = getattr(a, "name", None) or (a.get("name") if isinstance(a, dict) else None)
            a_value = getattr(a, "value", None) or (a.get("value") if isinstance(a, dict) else None)
            a_rationale = getattr(a, "rationale", None) or (a.get("rationale") if isinstance(a, dict) else None)

            if a_name == SCHEMA_JUDGE_VERDICT and a_value and "Wrong" in str(a_value):
                corrections.append({
                    "type": "judge_override",
                    "trace_id": trace_id,
                    "feedback": a_value,
                    "comment": a_rationale or "",
                })
            elif a_name == SCHEMA_CORRECTED_SQL and a_value:
                _q = ""
                try:
                    _req = row.get("request") or ""
                    if isinstance(_req, str):
                        import json
                        _q = json.loads(_req).get("messages", [{}])[-1].get("content", "")
                except Exception:
                    pass
                corrections.append({
                    "type": "benchmark_correction",
                    "trace_id": trace_id,
                    "corrected_sql": a_value,
                    "question": _q,
                })
            elif a_name == SCHEMA_IMPROVEMENTS and a_value:
                corrections.append({
                    "type": "improvement",
                    "trace_id": trace_id,
                    "suggestions": a_value,
                })
            elif a_name == SCHEMA_PATCH_APPROVAL and a_value:
                corrections.append({
                    "type": "patch_review",
                    "trace_id": trace_id,
                    "decision": a_value,
                    "comment": a_rationale or "",
                })

    logger.info(
        "Ingested %d corrections from %d reviewed traces (session '%s')",
        len(corrections), total_reviewed, session_name,
    )
    return {"corrections": corrections, "total_reviewed": total_reviewed}


def sync_corrections_to_dataset(
    session_name: str,
    dataset_name: str,
) -> bool:
    """Sync corrected expectations from a labeling session to the eval dataset.

    Finds the session by name and calls ``session.sync(dataset_name=...)``
    to propagate human corrections back to the UC-backed evaluation dataset.
    """
    session = _find_session_by_name(session_name)
    if session is None:
        logger.warning("Labeling session '%s' not found — cannot sync", session_name)
        return False

    try:
        session.sync(dataset_name=dataset_name)
        logger.info("Synced session '%s' to dataset %s", session_name, dataset_name)
        return True
    except Exception:
        logger.exception("Failed to sync session '%s' to dataset %s", session_name, dataset_name)
        return False
