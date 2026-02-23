"""Space endpoints: list, detail, and optimization trigger."""

from __future__ import annotations

import json
import logging
import uuid

from fastapi import HTTPException

from ..core import Dependencies, create_router
from ..models import OptimizeResponse, SpaceDetail, SpaceSummary, RunSummary, TableInfo
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)


@router.get("/spaces", response_model=list[SpaceSummary], operation_id="listSpaces")
def list_spaces(ws: Dependencies.UserClient, config: Dependencies.Config):
    """List Genie Spaces enriched with latest optimization metadata from Delta."""
    from genie_space_optimizer.common.genie_client import (
        list_spaces as _list_spaces,
        user_can_edit_space,
    )
    from genie_space_optimizer.optimization.state import load_recent_activity

    all_spaces = _list_spaces(ws)
    spaces = [s for s in all_spaces if user_can_edit_space(ws, s["id"])]

    score_by_space: dict[str, float] = {}
    try:
        spark = get_spark()
        activity_df = load_recent_activity(
            spark, config.catalog, config.schema_name, limit=200,
        )
        if not activity_df.empty:
            for _, row in activity_df.iterrows():
                sid = row.get("space_id", "")
                if sid and sid not in score_by_space:
                    score_by_space[sid] = float(row.get("best_accuracy", 0.0) or 0.0)
    except Exception:
        logger.debug("Delta tables not yet available, skipping activity enrichment")

    result: list[SpaceSummary] = []
    for s in spaces:
        space_id = s["id"]
        config_data: dict = {}
        try:
            config_data = ws.api_client.do(
                "GET",
                f"/api/2.0/genie/spaces/{space_id}",
                query={"include_serialized_space": "true"},
            )
        except Exception:
            logger.warning("Failed to fetch config for space %s", space_id)

        ss = config_data.get("serialized_space", {})
        if isinstance(ss, str):
            try:
                ss = json.loads(ss)
            except (json.JSONDecodeError, TypeError):
                ss = {}

        ds = ss.get("data_sources", {}) if isinstance(ss, dict) else {}
        tables = ds.get("tables", []) if isinstance(ds, dict) else []

        result.append(
            SpaceSummary(
                id=space_id,
                name=s.get("title", ""),
                description=config_data.get("description", ""),
                tableCount=len(tables),
                lastModified=config_data.get("update_time", config_data.get("create_time", "")),
                qualityScore=score_by_space.get(space_id),
            )
        )
    return result


@router.get("/spaces/{space_id}", response_model=SpaceDetail, operation_id="getSpaceDetail")
def get_space_detail(space_id: str, ws: Dependencies.UserClient, config: Dependencies.Config):
    """Full space config with UC metadata and optimization history."""
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import load_runs_for_space

    try:
        space_config = fetch_space_config(ws, space_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Space not found: {exc}") from exc

    ss = space_config.get("_parsed_space", {})
    ds = ss.get("data_sources", {}) if isinstance(ss, dict) else {}

    tables_raw = ds.get("tables", []) if isinstance(ds, dict) else []
    tables: list[TableInfo] = []
    for t in tables_raw:
        ident = t.get("identifier", "")
        parts = ident.split(".") if ident else []
        cat = parts[0] if len(parts) >= 3 else config.catalog
        sch = parts[1] if len(parts) >= 3 else config.schema_name
        desc_val = t.get("description", [])
        desc_str = "\n".join(desc_val) if isinstance(desc_val, list) else str(desc_val or "")
        cols = t.get("column_configs", [])
        tables.append(
            TableInfo(
                name=ident,
                catalog=cat,
                schema_name=sch,
                description=desc_str,
                columnCount=len(cols),
            )
        )

    instr = ss.get("instructions", {}) if isinstance(ss, dict) else {}
    text_instr = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    instructions_str = ""
    if text_instr and isinstance(text_instr, list):
        content = text_instr[0].get("content", []) if text_instr else []
        if isinstance(content, list):
            instructions_str = "\n".join(str(c) for c in content)
        else:
            instructions_str = str(content)

    example_qs = instr.get("example_question_sqls", []) if isinstance(instr, dict) else []
    sample_questions: list[str] = []
    for q in example_qs:
        if not isinstance(q, dict):
            continue
        val = q.get("question", "")
        if isinstance(val, list):
            val = val[0] if val else ""
        sample_questions.append(str(val))

    history: list[RunSummary] = []
    try:
        spark = get_spark()
        runs_df = load_runs_for_space(spark, space_id, config.catalog, config.schema_name)
        if not runs_df.empty:
            for _, row in runs_df.iterrows():
                history.append(
                    RunSummary(
                        runId=row.get("run_id", ""),
                        status=row.get("status", ""),
                        baselineScore=_safe_float(row.get("best_accuracy")),
                        optimizedScore=_safe_float(row.get("best_accuracy")),
                        timestamp=str(row.get("started_at", "")),
                    )
                )
    except Exception:
        logger.debug("Delta tables not yet available, skipping optimization history")

    return SpaceDetail(
        id=space_id,
        name=space_config.get("title", space_config.get("name", "")),
        description=space_config.get("description", ""),
        instructions=instructions_str,
        sampleQuestions=sample_questions,
        tables=tables,
        optimizationHistory=history,
    )


@router.post(
    "/spaces/{space_id}/optimize",
    response_model=OptimizeResponse,
    operation_id="startOptimization",
)
def start_optimization(
    space_id: str,
    ws: Dependencies.UserClient,
    config: Dependencies.Config,
    apply_mode: str = "genie_config",
):
    """Create a QUEUED run in Delta and submit the multi-task optimization job."""
    from genie_space_optimizer.optimization.state import create_run, update_run_status

    spark = get_spark()
    run_id = str(uuid.uuid4())

    domain = _infer_domain(ws, space_id)

    create_run(
        spark,
        run_id,
        space_id,
        domain,
        config.catalog,
        config.schema_name,
        apply_mode=apply_mode,
    )

    try:
        job_name = f"genie-space-optimizer-genie-optimization"
        jobs = list(ws.jobs.list(name=job_name))
        if not jobs:
            job_name_alt = f"genie_space_optimizer-genie-optimization"
            jobs = list(ws.jobs.list(name=job_name_alt))
        if not jobs:
            raise HTTPException(
                status_code=500,
                detail=f"Optimization job not found: {job_name}",
            )
        job_id = jobs[0].job_id

        run = ws.jobs.run_now(
            job_id=job_id,
            notebook_params={
                "run_id": run_id,
                "space_id": space_id,
                "domain": domain,
                "catalog": config.catalog,
                "schema": config.schema_name,
                "apply_mode": apply_mode,
                "levers": "[1,2,3,4,5,6]",
                "max_iterations": "5",
            },
        )
        job_run_id = str(run.run_id)

        update_run_status(
            spark, run_id, config.catalog, config.schema_name,
            job_run_id=job_run_id,
        )

        return OptimizeResponse(runId=run_id, jobRunId=job_run_id)

    except HTTPException:
        raise
    except Exception as exc:
        update_run_status(
            spark, run_id, config.catalog, config.schema_name,
            status="FAILED",
            convergence_reason=f"job_submission_error: {exc}",
        )
        raise HTTPException(status_code=500, detail=f"Job submission failed: {exc}") from exc


def _infer_domain(ws, space_id: str) -> str:
    """Best-effort domain inference from the Genie Space title."""
    try:
        space = ws.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
        title = space.get("title", "")
        return title.lower().replace(" ", "_").replace("-", "_")
    except Exception:
        return "default"


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
