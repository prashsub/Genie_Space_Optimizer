"""Space endpoints: list, detail, and optimization trigger."""

from __future__ import annotations

import json
import logging
import math
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import PermissionDenied
from databricks.sdk.service.sql import Disposition, Format
from fastapi import HTTPException

from ..constants import ACTIVE_RUN_STATUSES, TERMINAL_JOB_STATES
from ..core import Dependencies, create_router
from ..models import (
    FunctionInfo,
    JoinInfo,
    OptimizeResponse,
    RunSummary,
    SpaceDetail,
    SpaceSummary,
    TableInfo,
)
from ..utils import ensure_utc_iso, get_sp_principal, safe_float
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)
_ACTIVE_RUN_STATUSES = ACTIVE_RUN_STATUSES
_TERMINAL_JOB_STATES = TERMINAL_JOB_STATES
_STALE_QUEUE_TIMEOUT = timedelta(minutes=10)
_SUPPORTED_APPLY_MODES = {"genie_config", "uc_artifact", "both"}
_PIPELINE_APPLY_MODE = "genie_config"


def _genie_client(ws: WorkspaceClient, sp_ws: WorkspaceClient) -> WorkspaceClient:
    """Pick the best client for Genie API calls.

    Tries the user OBO client first; if the token lacks the ``genie`` scope
    falls back to the service-principal client transparently.
    """
    try:
        ws.genie.list_spaces(page_size=1)
        return ws
    except PermissionDenied:
        logger.info("OBO token missing genie scope — falling back to SP client")
        return sp_ws
    except Exception:
        logger.warning("Unexpected error probing OBO genie access — falling back to SP client", exc_info=True)
        return sp_ws


def _parse_utc(raw: object) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _reconcile_active_runs(
    *,
    spark,
    runs_df,
    sp_ws: WorkspaceClient,
    catalog: str,
    schema_name: str,
) -> bool:
    """Mark stale/terminated job-backed active rows as FAILED.

    This prevents old QUEUED/IN_PROGRESS rows from permanently blocking
    new optimization runs in the UI/API.
    """
    from genie_space_optimizer.optimization.state import update_run_status

    changed = False
    now = datetime.now(timezone.utc)

    for _, row in runs_df.iterrows():
        status = str(row.get("status") or "")
        if status not in _ACTIVE_RUN_STATUSES:
            continue

        run_id = str(row.get("run_id") or "")
        job_run_id = row.get("job_run_id")

        if job_run_id:
            try:
                run = sp_ws.jobs.get_run(run_id=int(str(job_run_id)))
                life_cycle = str(run.state.life_cycle_state).split(".")[-1] if run.state else ""
                if life_cycle in _TERMINAL_JOB_STATES:
                    result_state = str(run.state.result_state).split(".")[-1].lower() if run.state else ""
                    suffix = f":{result_state}" if result_state and result_state != "none" else ""
                    update_run_status(
                        spark,
                        run_id,
                        catalog,
                        schema_name,
                        status="FAILED",
                        convergence_reason=f"job_{life_cycle.lower()}_without_state_update{suffix}",
                    )
                    changed = True
            except Exception:
                update_run_status(
                    spark,
                    run_id,
                    catalog,
                    schema_name,
                    status="FAILED",
                    convergence_reason="job_run_lookup_failed",
                )
                changed = True
            continue

        started_at = _parse_utc(row.get("started_at"))
        if started_at and (now - started_at) > _STALE_QUEUE_TIMEOUT:
            update_run_status(
                spark,
                run_id,
                catalog,
                schema_name,
                status="FAILED",
                convergence_reason="stale_queued_no_job_run",
            )
            changed = True

    return changed


def _sql_rows(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    statement: str,
) -> list[dict]:
    """Execute SQL through statement execution and return list-of-dicts rows."""
    res = ws.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    rows = res.result.data_array if res.result and res.result.data_array else []
    cols_info = (
        res.manifest.schema.columns
        if res.manifest and res.manifest.schema and res.manifest.schema.columns
        else []
    )
    cols = [c.name for c in cols_info]
    return [{k: v for k, v in zip(cols, row)} for row in rows]


def _fetch_uc_metadata_obo(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    catalog: str,
    schema_name: str,
    genie_table_refs: list[tuple[str, str, str]] | None = None,
) -> dict[str, list[dict]]:
    """Fetch UC metadata via OBO user's SQL permissions.

    If ``genie_table_refs`` is provided (from ``extract_genie_space_table_refs``),
    queries are scoped to the exact Genie space data assets. Otherwise falls back
    to the optimizer's operational schema (legacy behavior).
    """
    if not warehouse_id:
        return {}

    if genie_table_refs:
        return _fetch_uc_metadata_obo_for_tables(ws, warehouse_id=warehouse_id, refs=genie_table_refs)

    safe_schema = schema_name.replace("'", "''")
    queries = {
        "uc_columns": (
            "SELECT table_name, column_name, data_type, comment "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{safe_schema}'"
        ),
        "uc_tags": (
            "SELECT * "
            f"FROM {catalog}.information_schema.table_tags "
            f"WHERE schema_name = '{safe_schema}'"
        ),
        "uc_routines": (
            "SELECT routine_name, routine_type, routine_definition, "
            "data_type AS return_type, routine_schema "
            f"FROM {catalog}.information_schema.routines "
            f"WHERE routine_schema = '{safe_schema}'"
        ),
    }

    result: dict[str, list[dict]] = {}
    for key, statement in queries.items():
        try:
            result[key] = _sql_rows(
                ws,
                warehouse_id=warehouse_id,
                statement=statement,
            )
        except Exception as exc:
            logger.warning("OBO metadata fetch failed for %s: %s", key, exc)
            result[key] = []
    logger.info(
        "OBO metadata prefetch (schema fallback): %s",
        {k: len(v) for k, v in result.items()},
    )
    return result


def _fetch_uc_metadata_obo_for_tables(
    ws: WorkspaceClient,
    *,
    warehouse_id: str,
    refs: list[tuple[str, str, str]],
) -> dict[str, list[dict]]:
    """Fetch UC metadata scoped to specific Genie space table references."""
    from genie_space_optimizer.common.uc_metadata import get_unique_schemas

    schema_groups: dict[tuple[str, str], list[str]] = {}
    for cat, sch, tbl in refs:
        if cat and sch and tbl:
            schema_groups.setdefault((cat, sch), []).append(tbl)

    col_unions: list[str] = []
    tag_unions: list[str] = []
    for (cat, sch), tables in schema_groups.items():
        safe_tables = ", ".join(f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in tables)
        col_unions.append(
            f"SELECT table_name, column_name, data_type, comment "
            f"FROM {cat}.information_schema.columns "
            f"WHERE table_schema = '{sch}' AND table_name IN ({safe_tables})"
        )
        tag_unions.append(
            f"SELECT * FROM {cat}.information_schema.table_tags "
            f"WHERE schema_name = '{sch}' AND table_name IN ({safe_tables})"
        )

    routine_unions: list[str] = []
    for cat, sch in get_unique_schemas(refs):
        routine_unions.append(
            f"SELECT routine_name, routine_type, routine_definition, "
            f"data_type AS return_type, routine_schema "
            f"FROM {cat}.INFORMATION_SCHEMA.ROUTINES "
            f"WHERE routine_schema = '{sch}'"
        )

    result: dict[str, list[dict]] = {}
    query_map = {
        "uc_columns": " UNION ALL ".join(col_unions) if col_unions else None,
        "uc_tags": " UNION ALL ".join(tag_unions) if tag_unions else None,
        "uc_routines": " UNION ALL ".join(routine_unions) if routine_unions else None,
    }
    for key, statement in query_map.items():
        if not statement:
            result[key] = []
            continue
        try:
            result[key] = _sql_rows(ws, warehouse_id=warehouse_id, statement=statement)
        except Exception as exc:
            logger.warning("OBO metadata fetch failed for %s (genie tables): %s", key, exc)
            result[key] = []
    logger.info(
        "OBO metadata prefetch (genie tables): %s",
        {k: len(v) for k, v in result.items()},
    )
    return result


@router.get("/spaces", response_model=list[SpaceSummary], operation_id="listSpaces")
def list_spaces(
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
):
    """List Genie Spaces enriched with latest optimization metadata from Delta.

    Tries the user's OBO token first for proper permission scoping; falls back
    to the service-principal client if the ``dashboards.genie`` scope is missing.
    """
    from genie_space_optimizer.common.genie_client import (
        list_spaces as _list_spaces,
        user_can_edit_space,
    )
    from genie_space_optimizer.optimization.state import load_recent_activity

    client = _genie_client(ws, sp_ws)
    try:
        all_spaces = _list_spaces(client)
    except Exception as exc:
        logger.error("Genie list_spaces failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Could not fetch Genie spaces: {exc}") from exc

    if client is ws:
        spaces = [s for s in all_spaces if user_can_edit_space(ws, s["id"])]
    else:
        spaces = all_spaces

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
                    raw = row.get("best_accuracy", 0.0) or 0.0
                    val = float(raw)
                    if not math.isfinite(val):
                        val = 0.0
                    score_by_space[sid] = val
    except Exception:
        logger.debug("Delta tables not yet available, skipping activity enrichment")

    result: list[SpaceSummary] = []
    for s in spaces:
        space_id = s["id"]
        config_data: dict[str, Any] = {}
        try:
            raw_config = client.api_client.do(
                "GET",
                f"/api/2.0/genie/spaces/{space_id}",
                query={"include_serialized_space": "true"},
            )
            if isinstance(raw_config, dict):
                config_data = cast(dict[str, Any], raw_config)
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
                name=str(s.get("title", "") or ""),
                description=str(config_data.get("description", "") or ""),
                tableCount=len(tables),
                lastModified=str(
                    config_data.get("update_time")
                    or config_data.get("create_time")
                    or ""
                ),
                qualityScore=score_by_space.get(space_id),
            )
        )
    return result


@router.get("/spaces/{space_id}", response_model=SpaceDetail, operation_id="getSpaceDetail")
def get_space_detail(
    space_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
):
    """Full space config with UC metadata and optimization history."""
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import load_runs_for_space

    client = _genie_client(ws, sp_ws)
    try:
        space_config = fetch_space_config(client, space_id)
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

    joins_raw = ds.get("join_specs", []) if isinstance(ds, dict) else []
    joins: list[JoinInfo] = []
    for join in joins_raw:
        if not isinstance(join, dict):
            continue
        left = str(join.get("left_table_name") or "")
        right = str(join.get("right_table_name") or "")
        rel = join.get("relationship_type")
        join_columns_raw = join.get("join_columns", [])
        join_columns: list[str] = []
        if isinstance(join_columns_raw, list):
            for jc in join_columns_raw:
                if not isinstance(jc, dict):
                    continue
                left_col = str(jc.get("left_column") or "")
                right_col = str(jc.get("right_column") or "")
                if left_col or right_col:
                    join_columns.append(f"{left_col} = {right_col}")
        joins.append(
            JoinInfo(
                leftTable=left,
                rightTable=right,
                relationshipType=str(rel) if rel is not None else None,
                joinColumns=join_columns,
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

    benchmark_questions: list[str] = []
    try:
        spark = get_spark()
        title = str(space_config.get("title", "") or "")
        domain = re.sub(r"[^a-z0-9_]+", "_", title.lower().replace(" ", "_").replace("-", "_")).strip("_")
        if not domain:
            domain = "default"
        bench_table = f"{config.catalog}.{config.schema_name}.genie_benchmarks_{domain}"
        bench_df = spark.sql(
            f"SELECT question FROM {bench_table} WHERE question IS NOT NULL ORDER BY question LIMIT 200"
        )
        seen: set[str] = set()
        for row in bench_df.collect():
            q = str(row["question"] or "")
            q = q.strip()
            if q and q not in seen:
                seen.add(q)
                benchmark_questions.append(q)
    except Exception:
        logger.debug("Benchmark questions not available for space %s", space_id)

    sql_functions_raw = instr.get("sql_functions", []) if isinstance(instr, dict) else []
    functions: list[FunctionInfo] = []
    for fn in sql_functions_raw:
        if not isinstance(fn, dict):
            continue
        ident = fn.get("identifier", "")
        parts = ident.split(".") if ident else []
        cat = parts[0] if len(parts) >= 3 else config.catalog
        sch = parts[1] if len(parts) >= 3 else config.schema_name
        functions.append(FunctionInfo(name=ident, catalog=cat, schema_name=sch))

    history: list[RunSummary] = []
    try:
        spark = get_spark()
        runs_df = load_runs_for_space(spark, space_id, config.catalog, config.schema_name)
        if not runs_df.empty and _reconcile_active_runs(
            spark=spark,
            runs_df=runs_df,
            sp_ws=sp_ws,
            catalog=config.catalog,
            schema_name=config.schema_name,
        ):
            runs_df = load_runs_for_space(spark, space_id, config.catalog, config.schema_name)
        if not runs_df.empty:
            for _, row in runs_df.iterrows():
                history.append(
                    RunSummary(
                        runId=row.get("run_id", ""),
                        status=row.get("status", ""),
                        baselineScore=_safe_float(row.get("best_accuracy")),
                        optimizedScore=_safe_float(row.get("best_accuracy")),
                        timestamp=ensure_utc_iso(row.get("started_at")) or "",
                    )
                )
    except Exception:
        logger.debug("Delta tables not yet available, skipping optimization history")

    has_active = any(r.status in _ACTIVE_RUN_STATUSES for r in history)

    return SpaceDetail(
        id=space_id,
        name=space_config.get("title", space_config.get("name", "")),
        description=space_config.get("description", ""),
        instructions=instructions_str,
        sampleQuestions=sample_questions,
        benchmarkQuestions=benchmark_questions,
        tables=tables,
        joins=joins,
        functions=functions,
        optimizationHistory=history,
        hasActiveRun=has_active,
    )


def _check_sp_data_access(
    spark, catalog: str, schema_name: str, genie_refs: list, sp_ws: WorkspaceClient,
) -> list[tuple[str, str]]:
    """Return list of (catalog, schema) pairs the SP does NOT have grants for."""
    from .settings import _load_grants

    needed: set[tuple[str, str]] = set()
    for ref in genie_refs:
        cat = ref[0] if isinstance(ref, (list, tuple)) else ""
        sch = ref[1] if isinstance(ref, (list, tuple)) and len(ref) > 1 else ""
        if cat and sch:
            needed.add((cat.lower(), sch.lower()))

    if not needed:
        return []

    grants = _load_grants(spark, catalog, schema_name)
    granted = {
        (str(g.get("target_catalog", "")).lower(), str(g.get("target_schema", "")).lower())
        for g in grants
    }

    missing = sorted(needed - granted)
    return missing


@router.post(
    "/spaces/{space_id}/optimize",
    response_model=OptimizeResponse,
    operation_id="startOptimization",
)
def start_optimization(
    space_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
    apply_mode: str = "genie_config",
):
    """Create a QUEUED run in Delta and dynamically submit a serverless optimization job.

    Uses OBO for Genie domain inference, SP for job submission.
    User identity comes from forwarded headers.
    """
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import (
        create_run,
        ensure_optimization_tables,
        load_runs_for_space,
        update_run_status,
    )
    from ..job_launcher import submit_optimization

    requested_apply_mode = (apply_mode or "genie_config").strip().lower()
    if requested_apply_mode not in _SUPPORTED_APPLY_MODES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported apply_mode '{apply_mode}'. "
                f"Use one of: {sorted(_SUPPORTED_APPLY_MODES)}"
            ),
        )

    spark = get_spark()
    ensure_optimization_tables(spark, config.catalog, config.schema_name)

    runs_df = load_runs_for_space(spark, space_id, config.catalog, config.schema_name)
    if not runs_df.empty and _reconcile_active_runs(
        spark=spark,
        runs_df=runs_df,
        sp_ws=sp_ws,
        catalog=config.catalog,
        schema_name=config.schema_name,
    ):
        runs_df = load_runs_for_space(spark, space_id, config.catalog, config.schema_name)
    if not runs_df.empty:
        active = runs_df[runs_df["status"].isin(list(_ACTIVE_RUN_STATUSES))]
        if not active.empty:
            active_id = active.iloc[0]["run_id"]
            raise HTTPException(
                status_code=409,
                detail=f"An optimization is already in progress for this space (run {active_id})",
            )

    prev_experiment: str | None = None
    if not runs_df.empty:
        terminal = runs_df[~runs_df["status"].isin(list(_ACTIVE_RUN_STATUSES))]
        if not terminal.empty:
            exp_val = terminal.iloc[0].get("experiment_name")
            if exp_val and str(exp_val) not in ("", "None", "nan"):
                prev_experiment = str(exp_val)

    run_id = str(uuid.uuid4())

    client = _genie_client(ws, sp_ws)
    space_snapshot: dict = {}
    try:
        space_snapshot = fetch_space_config(client, space_id)
    except Exception:
        logger.warning("Failed to capture space config snapshot for %s", space_id)

    if space_snapshot:
        title = str(space_snapshot.get("title", "") or "")
        domain = title.lower().replace(" ", "_").replace("-", "_") if title else "default"
    else:
        domain = _infer_domain(client, space_id)

    from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs

    genie_refs = extract_genie_space_table_refs(space_snapshot) if space_snapshot else []

    # OBO prefetch first -- runs as the triggering user and is the most
    # reliable path for fetching UC metadata the SP may not yet have
    # grants for.  Separated from the access check so one cannot prevent
    # the other.
    try:
        obo_uc_metadata = _fetch_uc_metadata_obo(
            ws,
            warehouse_id=config.warehouse_id,
            catalog=config.catalog,
            schema_name=config.schema_name,
            genie_table_refs=genie_refs or None,
        )
        if space_snapshot and obo_uc_metadata and any(obo_uc_metadata.values()):
            space_snapshot["_prefetched_uc_metadata"] = obo_uc_metadata
    except Exception:
        logger.warning("OBO UC metadata prefetch failed for run %s", run_id, exc_info=True)

    # SP data-access check -- advisory: log a warning when grants are
    # missing from the app ledger but never silently swallow the error.
    try:
        missing = _check_sp_data_access(spark, config.catalog, config.schema_name, genie_refs, sp_ws)
        if missing:
            sp_id = get_sp_principal(sp_ws)
            lines = []
            for cat, sch in missing:
                lines.append(
                    f"GRANT USE CATALOG ON CATALOG `{cat}` TO `{sp_id}`;\n"
                    f"GRANT USE SCHEMA ON SCHEMA `{cat}`.`{sch}` TO `{sp_id}`;\n"
                    f"GRANT SELECT ON SCHEMA `{cat}`.`{sch}` TO `{sp_id}`;\n"
                    f"GRANT EXECUTE ON SCHEMA `{cat}`.`{sch}` TO `{sp_id}`;"
                )
            schemas_str = ", ".join(f"`{c}`.`{s}`" for c, s in missing)
            logger.warning(
                "SP missing grants on %s for run %s — the job will rely on "
                "prefetched OBO metadata or Spark fallback. To fix, run:\n%s",
                schemas_str, run_id, "\n\n".join(lines),
            )
    except HTTPException:
        raise
    except Exception:
        logger.warning("SP data-access check failed for run %s", run_id, exc_info=True)

    current_user = headers.user_email or headers.user_name or ""
    if not current_user:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot determine the requesting user's identity. "
                "Optimization jobs run under the user's permissions (OBO) and "
                "require a valid user email from the authentication headers."
            ),
        )

    create_run(
        spark,
        run_id,
        space_id,
        domain,
        config.catalog,
        config.schema_name,
        apply_mode=requested_apply_mode,
        triggered_by=current_user,
        experiment_name=prev_experiment,
        config_snapshot=space_snapshot if space_snapshot else None,
    )

    try:
        job_run_id, job_id = submit_optimization(
            sp_ws,
            run_id=run_id,
            space_id=space_id,
            domain=domain,
            catalog=config.catalog,
            schema=config.schema_name,
            apply_mode=_PIPELINE_APPLY_MODE,
            triggered_by=current_user,
            experiment_name=prev_experiment or "",
        )

        update_run_status(
            spark, run_id, config.catalog, config.schema_name,
            status="IN_PROGRESS",
            job_run_id=job_run_id,
            job_id=str(job_id),
        )

        host = (sp_ws.config.host or "").rstrip("/")
        workspace_id: int | None = None
        if host:
            try:
                workspace_id = sp_ws.get_workspace_id()
            except Exception:
                workspace_id = None
        if host and workspace_id is not None:
            job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}?o={workspace_id}"
        elif host:
            job_url = f"{host}/jobs/{job_id}/runs/{job_run_id}"
        else:
            job_url = None
        return OptimizeResponse(runId=run_id, jobRunId=job_run_id, jobUrl=job_url)

    except Exception as exc:
        logger.exception("Job submission failed for run %s", run_id)
        update_run_status(
            spark, run_id, config.catalog, config.schema_name,
            status="FAILED",
            convergence_reason=f"job_submission_error: {exc}",
        )
        raise HTTPException(status_code=500, detail=f"Job submission failed: {exc}") from exc


def _infer_domain(w, space_id: str) -> str:
    """Best-effort domain inference from the Genie Space title."""
    try:
        space = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
        title = space.get("title", "")
        return title.lower().replace(" ", "_").replace("-", "_")
    except Exception:
        return "default"


_safe_float = safe_float
