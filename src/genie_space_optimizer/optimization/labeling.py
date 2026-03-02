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


def ensure_labeling_schemas() -> list[str]:
    """Create or update custom labeling schemas for Genie optimization review.

    Returns the list of schema names that were created/updated.
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

    created: list[str] = []
    try:
        schemas.create_label_schema(
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
            overwrite=True,
        )
        created.append(SCHEMA_JUDGE_VERDICT)
    except Exception:
        logger.debug("Failed to create schema %s", SCHEMA_JUDGE_VERDICT, exc_info=True)

    try:
        schemas.create_label_schema(
            name=SCHEMA_CORRECTED_SQL,
            type="expectation",
            title="Provide the correct expected SQL (if benchmark is wrong)",
            input=InputText(),
            instruction=(
                "If the benchmark's expected SQL is incorrect or suboptimal, "
                "provide the corrected SQL. Leave blank if the benchmark is correct."
            ),
            overwrite=True,
        )
        created.append(SCHEMA_CORRECTED_SQL)
    except Exception:
        logger.debug("Failed to create schema %s", SCHEMA_CORRECTED_SQL, exc_info=True)

    try:
        schemas.create_label_schema(
            name=SCHEMA_PATCH_APPROVAL,
            type="feedback",
            title="Should the proposed optimization patch be applied?",
            input=InputCategorical(options=["Approve", "Reject", "Modify"]),
            instruction=(
                "Review the proposed metadata change (column description, instruction, "
                "join condition, etc.). Approve it, reject it, or suggest modifications."
            ),
            enable_comment=True,
            overwrite=True,
        )
        created.append(SCHEMA_PATCH_APPROVAL)
    except Exception:
        logger.debug("Failed to create schema %s", SCHEMA_PATCH_APPROVAL, exc_info=True)

    try:
        schemas.create_label_schema(
            name=SCHEMA_IMPROVEMENTS,
            type="expectation",
            title="Suggest improvements for this Genie Space",
            input=InputTextList(max_count=5, max_length_each=500),
            instruction=(
                "Provide specific, actionable improvements: better column descriptions, "
                "missing join conditions, instruction changes, or table-level fixes."
            ),
            overwrite=True,
        )
        created.append(SCHEMA_IMPROVEMENTS)
    except Exception:
        logger.debug("Failed to create schema %s", SCHEMA_IMPROVEMENTS, exc_info=True)

    logger.info("Ensured %d labeling schemas: %s", len(created), ", ".join(created))
    return created


def create_review_session(
    run_id: str,
    domain: str,
    experiment_name: str,
    uc_schema: str,
    failure_trace_ids: list[str],
    regression_trace_ids: list[str],
    reviewers: list[str] | None = None,
) -> dict[str, Any]:
    """Create a labeling session and populate it with the eval dataset and failure traces.

    Uses ``session.add_dataset()`` to load all benchmark questions, then
    ``session.add_traces()`` to overlay specific failure/regression traces
    from the evaluation run.

    Returns dict with session_name, session_run_id, trace_count (or empty on failure).
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

    try:
        session = labeling.create_labeling_session(
            name=session_name,
            label_schemas=schema_names,
            assigned_users=reviewers or [],
        )
    except Exception:
        logger.exception("Failed to create labeling session")
        return {}

    dataset_name = f"{uc_schema}.genie_benchmarks_{domain}"
    try:
        session.add_dataset(dataset_name)
        logger.info("Added dataset %s to labeling session %s", dataset_name, session_name)
    except Exception:
        logger.debug("Failed to add dataset %s to session", dataset_name, exc_info=True)

    priority_ids = list(dict.fromkeys(regression_trace_ids + failure_trace_ids))[:100]
    traces_added = 0
    if priority_ids:
        try:
            exp = mlflow.get_experiment_by_name(experiment_name)
            if exp:
                traces_df = mlflow.search_traces(
                    experiment_ids=[exp.experiment_id],
                    max_results=100,
                )
                if traces_df is not None and len(traces_df) > 0:
                    priority_set = set(priority_ids)
                    mask = traces_df["trace_id"].isin(priority_set)
                    filtered = traces_df[mask] if mask.any() else traces_df
                    session.add_traces(filtered)
                    traces_added = len(filtered)
                    logger.info("Added %d traces to labeling session %s", traces_added, session_name)
        except Exception:
            logger.debug("Failed to add traces to labeling session", exc_info=True)

    session_run_id = getattr(session, "mlflow_run_id", "")
    session_id = getattr(session, "labeling_session_id", "")

    logger.info(
        "Created labeling session: %s (run_id=%s, session_id=%s, traces=%d)",
        session_name, session_run_id, session_id, traces_added,
    )
    return {
        "session_name": session_name,
        "session_run_id": session_run_id,
        "labeling_session_id": session_id,
        "trace_count": traces_added,
    }


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
                corrections.append({
                    "type": "benchmark_correction",
                    "trace_id": trace_id,
                    "corrected_sql": a_value,
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
