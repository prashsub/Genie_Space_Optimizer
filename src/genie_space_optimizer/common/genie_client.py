"""
Genie Space API wrapper.

All Genie Space API interactions. Every function takes ``WorkspaceClient``
as its first argument (APX pattern: dependency injection, no global state).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, cast

from databricks.sdk import WorkspaceClient

from .config import (
    GENIE_MAX_WAIT,
    GENIE_POLL_INITIAL,
    GENIE_POLL_MAX,
    NON_EXPORTABLE_FIELDS,
)

logger = logging.getLogger(__name__)


# ── Space Discovery & Config ───────────────────────────────────────────


def list_spaces(w: WorkspaceClient) -> list[dict[str, str]]:
    """List available Genie Spaces via SDK.

    Returns a list of ``{"id": ..., "title": ...}`` dicts.
    """
    resp = w.genie.list_spaces()
    return [{"id": s.space_id, "title": s.title} for s in (resp.spaces or [])]


EDITABLE_PERMISSIONS = {"CAN_MANAGE", "CAN_EDIT"}


def user_can_edit_space(w: WorkspaceClient, space_id: str) -> bool:
    """Check whether the calling user has CAN_MANAGE or CAN_EDIT on a Genie space."""
    try:
        me = w.current_user.me()
        my_email = (me.user_name or "").lower()
        my_groups: set[str] = set()
        if me.groups:
            my_groups = {g.display.lower() for g in me.groups if g.display}

        perms = w.permissions.get("genie", space_id)
        for acl in perms.access_control_list or []:
            principal = (acl.user_name or acl.group_name or "").lower()
            is_me = (
                principal == my_email
                or principal in my_groups
                or acl.group_name == "admins"
            )
            if not is_me:
                continue
            for p in acl.all_permissions or []:
                if str(p.permission_level).replace("PermissionLevel.", "") in EDITABLE_PERMISSIONS:
                    return True
        return False
    except Exception:
        logger.warning("Could not check permissions for space %s", space_id)
        return True


def fetch_space_config(w: WorkspaceClient, space_id: str) -> dict:
    """GET Genie Space config with full serialized_space content.

    Returns the raw API response augmented with convenience keys:
    ``_parsed_space``, ``_tables``, ``_metric_views``, ``_functions``,
    ``_instructions``.
    """
    raw_config = w.api_client.do(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}",
        query={"include_serialized_space": "true"},
    )
    if not isinstance(raw_config, dict):
        raise RuntimeError(
            f"Unexpected Genie space response type: {type(raw_config).__name__}"
        )
    config = cast(dict[str, Any], raw_config)

    ss = config.get("serialized_space", {})
    if isinstance(ss, str):
        ss = json.loads(ss)
    config["_parsed_space"] = ss

    ds = ss.get("data_sources", {})
    if isinstance(ds, dict):
        tables_list = ds.get("tables", [])
        mvs_list = ds.get("metric_views", [])
        funcs_list = ds.get("functions", [])
    else:
        tables_list, mvs_list, funcs_list = [], [], []

    instr = ss.get("instructions", {})
    text_instr = instr.get("text_instructions", []) if isinstance(instr, dict) else []
    has_instructions = bool(text_instr) or bool(config.get("description", ""))

    config["_tables"] = [t.get("identifier", "") for t in tables_list if isinstance(t, dict)]
    config["_metric_views"] = [m.get("identifier", "") for m in mvs_list if isinstance(m, dict)]
    config["_functions"] = [f.get("identifier", "") for f in funcs_list if isinstance(f, dict)]
    config["_instructions"] = text_instr

    logger.info(
        "Space state: tables=%d, metric_views=%d, tvfs=%d, instructions=%s",
        len(tables_list),
        len(mvs_list),
        len(funcs_list),
        "present" if has_instructions else "absent",
    )
    return config


# ── Genie Query ────────────────────────────────────────────────────────


def run_genie_query(
    w: WorkspaceClient,
    space_id: str,
    question: str,
    max_wait: int = GENIE_MAX_WAIT,
) -> dict:
    """Send a question to Genie and return generated SQL + status.

    Uses adaptive polling (``GENIE_POLL_INITIAL`` → ``GENIE_POLL_MAX``).
    Returns ``{"status", "sql", "conversation_id", "message_id"}``.
    """
    try:
        resp = w.genie.start_conversation(space_id=space_id, content=question)
        conversation_id = resp.conversation_id
        message_id = resp.message_id

        poll_interval = GENIE_POLL_INITIAL
        start = time.time()
        msg = None
        status = "UNKNOWN"

        while time.time() - start < max_wait:
            time.sleep(poll_interval)
            msg = w.genie.get_message(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id,
            )
            status = str(msg.status) if hasattr(msg, "status") else "UNKNOWN"
            if any(s in status for s in ["COMPLETED", "FAILED", "CANCELLED"]):
                break
            poll_interval = min(poll_interval + 1, GENIE_POLL_MAX)

        sql = None
        if msg and hasattr(msg, "attachments") and msg.attachments:
            for att in msg.attachments:
                if hasattr(att, "query") and att.query:
                    sql = att.query.query if hasattr(att.query, "query") else str(att.query)

        return {
            "status": status,
            "sql": sql,
            "conversation_id": conversation_id,
            "message_id": message_id,
        }
    except Exception as e:
        logger.exception("Genie query failed for space %s", space_id)
        return {"status": "ERROR", "sql": None, "error": str(e)}


# ── Asset Detection ────────────────────────────────────────────────────


def detect_asset_type(sql: str) -> str:
    """Detect asset type (MV, TVF, TABLE, NONE) from a SQL string."""
    if not sql:
        return "NONE"
    sql_lower = sql.lower()
    if "mv_" in sql_lower or "measure(" in sql_lower:
        return "MV"
    elif "get_" in sql_lower:
        return "TVF"
    return "TABLE"


# ── SQL Helpers ────────────────────────────────────────────────────────


def resolve_sql(sql: str, catalog: str, gold_schema: str) -> str:
    """Substitute ``${catalog}`` and ``${gold_schema}`` template variables."""
    if not sql:
        return sql
    return sql.replace("${catalog}", catalog).replace("${gold_schema}", gold_schema)


def sanitize_sql(sql: str) -> str:
    """Extract the first SQL statement, strip comments and trailing semicolons.

    Genie may return multi-statement SQL for compound questions.
    """
    if not sql:
        return sql
    sql = sql.strip().rstrip(";").strip()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    if not statements:
        return sql
    first = statements[0]
    lines = [line for line in first.split("\n") if not line.strip().startswith("--")]
    return "\n".join(lines).strip()


# ── Config Mutation ────────────────────────────────────────────────────


def strip_non_exportable_fields(config: dict) -> dict:
    """Remove read-only server-managed fields before PATCH requests.

    The GET ``/api/2.0/genie/spaces/{id}`` response includes top-level
    metadata fields that are NOT part of the ``GenieSpaceExport`` protobuf.
    Including them in the PATCH payload causes ``InvalidParameterValue``.
    """
    return {k: v for k, v in config.items() if k not in NON_EXPORTABLE_FIELDS}


def sort_genie_config(config: dict) -> dict:
    """Sort arrays in a Genie config for deterministic comparison.

    The Genie API rejects unsorted data in certain array fields.
    """
    if "data_sources" in config:
        for key in ["tables", "metric_views"]:
            if key in config["data_sources"]:
                config["data_sources"][key] = sorted(
                    config["data_sources"][key],
                    key=lambda x: x.get("identifier", ""),
                )
    if "instructions" in config:
        if "sql_functions" in config["instructions"]:
            config["instructions"]["sql_functions"] = sorted(
                config["instructions"]["sql_functions"],
                key=lambda x: (x.get("id", ""), x.get("identifier", "")),
            )
        for key in ["text_instructions", "example_question_sqls"]:
            if key in config["instructions"]:
                config["instructions"][key] = sorted(
                    config["instructions"][key],
                    key=lambda x: x.get("id", ""),
                )
    return config


def patch_space_config(w: WorkspaceClient, space_id: str, config: dict) -> dict:
    """PATCH a Genie Space with updated serialized_space config.

    Strips non-exportable fields and sorts arrays before sending.
    Returns the raw API response.
    """
    clean = strip_non_exportable_fields(config)
    clean = sort_genie_config(clean)
    payload = {"serialized_space": json.dumps(clean)}
    raw_resp = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=payload)
    if isinstance(raw_resp, dict):
        return raw_resp
    return {}
