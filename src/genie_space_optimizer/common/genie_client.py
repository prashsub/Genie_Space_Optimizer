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

from databricks.sdk.errors.platform import ResourceExhausted

from .config import (
    GENIE_MAX_WAIT,
    GENIE_POLL_INITIAL,
    GENIE_POLL_MAX,
    GENIE_RATE_LIMIT_BASE_DELAY,
    GENIE_RATE_LIMIT_RETRIES,
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
    """Send a question to Genie and return generated SQL + result metadata.

    Uses adaptive polling (``GENIE_POLL_INITIAL`` → ``GENIE_POLL_MAX``).
    Returns ``{"status", "sql", "conversation_id", "message_id",
    "attachment_id", "statement_id"}``.

    ``statement_id`` can be used with ``fetch_genie_result_df`` to retrieve
    the query results that Genie already computed (avoiding re-execution).

    Retries with exponential backoff on ``ResourceExhausted`` (HTTP 429)
    and ``TimeoutError`` from the SDK's own retry layer.
    """
    for rate_attempt in range(GENIE_RATE_LIMIT_RETRIES + 1):
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
            attachment_id = None
            statement_id = None
            analysis_text = None

            if msg and hasattr(msg, "attachments") and msg.attachments:
                for att in msg.attachments:
                    if hasattr(att, "query") and att.query:
                        sql = att.query.query if hasattr(att.query, "query") else str(att.query)
                        attachment_id = getattr(att, "id", None) or getattr(att, "attachment_id", None)
                    if hasattr(att, "text") and att.text:
                        text_content = getattr(att.text, "content", None)
                        if text_content and text_content.strip():
                            analysis_text = text_content.strip()

            if msg and hasattr(msg, "query_result") and msg.query_result:
                statement_id = getattr(msg.query_result, "statement_id", None)

            if not statement_id and attachment_id:
                try:
                    qr = w.genie.get_message_attachment_query_result(
                        space_id=space_id,
                        conversation_id=conversation_id,
                        message_id=message_id,
                        attachment_id=attachment_id,
                    )
                    statement_id = getattr(qr, "statement_id", None)
                except Exception:
                    logger.debug("Could not fetch attachment query result for statement_id", exc_info=True)

            return {
                "status": status,
                "sql": sql,
                "conversation_id": conversation_id,
                "message_id": message_id,
                "attachment_id": attachment_id,
                "statement_id": statement_id,
                "analysis_text": analysis_text,
            }
        except (ResourceExhausted, TimeoutError) as e:
            if rate_attempt < GENIE_RATE_LIMIT_RETRIES:
                delay = GENIE_RATE_LIMIT_BASE_DELAY * (2 ** rate_attempt)
                logger.warning(
                    "Genie rate-limited (attempt %d/%d), retrying in %ds: %s",
                    rate_attempt + 1,
                    GENIE_RATE_LIMIT_RETRIES,
                    delay,
                    e,
                )
                time.sleep(delay)
                continue
            logger.exception("Genie query failed after %d rate-limit retries for space %s", GENIE_RATE_LIMIT_RETRIES, space_id)
            return {"status": "ERROR", "sql": None, "error": str(e)}
        except Exception as e:
            logger.exception("Genie query failed for space %s", space_id)
            return {"status": "ERROR", "sql": None, "error": str(e)}
    return {"status": "ERROR", "sql": None, "error": "exhausted rate-limit retries"}


def fetch_genie_result_df(w: WorkspaceClient, statement_id: str):
    """Fetch Genie's query result as a pandas DataFrame using the Statement Execution API.

    Returns ``None`` if the result cannot be retrieved.
    """
    import pandas as pd

    try:
        stmt = w.statement_execution.get_statement(statement_id)
        if stmt.result and stmt.result.data_array and stmt.manifest and stmt.manifest.schema:
            cols = stmt.manifest.schema.columns
            if cols:
                col_names = [c.name for c in cols]
                return pd.DataFrame(stmt.result.data_array, columns=col_names)  # type: ignore[arg-type]
        return None
    except Exception:
        logger.debug("Could not fetch statement %s results", statement_id, exc_info=True)
        return None


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


def _migrate_column_configs_v1_to_v2(config: dict) -> dict:
    """Migrate v1 column config fields to v2 and strip non-exportable column fields.

    The Genie Space export API v2 renamed:
      - ``get_example_values``    -> ``enable_format_assistance``
      - ``build_value_dictionary`` -> ``enable_entity_matching``

    Also removes ``data_type`` which is not part of the ColumnConfig proto.
    """
    _V1_TO_V2 = {
        "get_example_values": "enable_format_assistance",
        "build_value_dictionary": "enable_entity_matching",
    }
    _STRIP_FIELDS = {"data_type"}

    ds = config.get("data_sources", {})
    for key in ("tables", "metric_views"):
        for tbl in ds.get(key, []):
            for cc in tbl.get("column_configs", []):
                for old_key, new_key in _V1_TO_V2.items():
                    if old_key in cc:
                        if new_key not in cc:
                            cc[new_key] = cc[old_key]
                        del cc[old_key]
                for field in _STRIP_FIELDS:
                    cc.pop(field, None)
    return config


def strip_non_exportable_fields(config: dict) -> dict:
    """Remove read-only server-managed fields before PATCH requests.

    The GET ``/api/2.0/genie/spaces/{id}`` response includes top-level
    metadata fields that are NOT part of the ``GenieSpaceExport`` protobuf.
    Including them in the PATCH payload causes ``InvalidParameterValue``.
    """
    cleaned = {k: v for k, v in config.items() if k not in NON_EXPORTABLE_FIELDS}
    return _migrate_column_configs_v1_to_v2(cleaned)


def sort_genie_config(config: dict) -> dict:
    """Sort all arrays in a Genie config to satisfy API sort requirements.

    The Genie API rejects unsorted data. Each collection must be sorted
    by the key documented at:
    https://docs.databricks.com/aws/en/genie/conversation-api#sorting-requirements
    """
    # ── data_sources.tables / metric_views  (by identifier) ──────
    if "data_sources" in config:
        for key in ["tables", "metric_views"]:
            if key in config["data_sources"]:
                config["data_sources"][key] = sorted(
                    config["data_sources"][key],
                    key=lambda x: x.get("identifier", ""),
                )
                for tbl in config["data_sources"][key]:
                    if "column_configs" in tbl and tbl["column_configs"]:
                        tbl["column_configs"] = sorted(
                            tbl["column_configs"],
                            key=lambda x: x.get("column_name", ""),
                        )

    # ── config.sample_questions  (by id) ─────────────────────────
    if "config" in config:
        sqs = config["config"].get("sample_questions")
        if sqs:
            config["config"]["sample_questions"] = sorted(
                sqs, key=lambda x: x.get("id", "")
            )

    # ── instructions ─────────────────────────────────────────────
    if "instructions" in config:
        inst = config["instructions"]

        if "sql_functions" in inst:
            inst["sql_functions"] = sorted(
                inst["sql_functions"],
                key=lambda x: (x.get("id", ""), x.get("identifier", "")),
            )
        for key in ["text_instructions", "example_question_sqls", "join_specs"]:
            if key in inst:
                inst[key] = sorted(inst[key], key=lambda x: x.get("id", ""))

        # sql_snippets sub-arrays (by id)
        snippets = inst.get("sql_snippets")
        if isinstance(snippets, dict):
            for snippet_key in ["filters", "expressions", "measures"]:
                if snippet_key in snippets and snippets[snippet_key]:
                    snippets[snippet_key] = sorted(
                        snippets[snippet_key],
                        key=lambda x: x.get("id", ""),
                    )

    # ── benchmarks.questions  (by id) ────────────────────────────
    if "benchmarks" in config:
        questions = config["benchmarks"].get("questions", [])
        if questions:
            config["benchmarks"]["questions"] = sorted(
                questions,
                key=lambda x: x.get("id", ""),
            )

    return config


def patch_space_config(w: WorkspaceClient, space_id: str, config: dict) -> dict:
    """PATCH a Genie Space with updated serialized_space config.

    Strips non-exportable fields, sorts arrays, and validates the payload
    structure before sending.  Returns the raw API response.
    """
    from .genie_schema import validate_serialized_space

    clean = strip_non_exportable_fields(config)
    clean = sort_genie_config(clean)

    ok, errors = validate_serialized_space(clean, strict=True)
    if not ok:
        logger.error(
            "Config validation failed before PATCH for space %s: %s",
            space_id,
            errors,
        )
        raise ValueError(f"Genie config validation failed: {errors}")

    payload = {"serialized_space": json.dumps(clean)}
    raw_resp = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=payload)
    if isinstance(raw_resp, dict):
        return raw_resp
    return {}


# ── Benchmark Publishing ──────────────────────────────────────────────

GENIE_MAX_BENCHMARK_QUESTIONS = 500


def _benchmarks_to_genie_format(benchmarks: list[dict]) -> list[dict]:
    """Convert optimizer benchmark dicts to Genie-native ``benchmarks.questions`` format.

    Prioritises curated/P0 benchmarks first, then fills with synthetic.
    """
    curated: list[dict] = []
    synthetic: list[dict] = []
    for b in benchmarks:
        source = b.get("source", "")
        priority = b.get("priority", "")
        if source == "genie_space" or priority == "P0":
            curated.append(b)
        else:
            synthetic.append(b)

    ordered = curated + synthetic

    genie_questions: list[dict] = []
    seen: set[str] = set()
    for b in ordered:
        question = str(b.get("question", "")).strip()
        if not question:
            continue
        q_lower = question.lower()
        if q_lower in seen:
            continue
        seen.add(q_lower)

        entry: dict[str, Any] = {"question": [question]}
        expected_sql = str(b.get("expected_sql", "")).strip()
        if expected_sql:
            entry["answer"] = [{"format": "SQL", "content": [expected_sql]}]
        genie_questions.append(entry)

    return genie_questions


def publish_benchmarks_to_genie_space(
    w: WorkspaceClient,
    space_id: str,
    benchmarks: list[dict],
    max_questions: int = GENIE_MAX_BENCHMARK_QUESTIONS,
) -> int:
    """Write optimizer benchmarks into the Genie Space's native benchmarks section.

    Fetches the current space config, converts benchmarks to Genie-native
    format, merges them into ``serialized_space.benchmarks.questions``, and
    PATCHes the space via the existing ``updateSpace`` API.

    Returns the number of benchmark questions published.
    """
    config = fetch_space_config(w, space_id)
    parsed = config.get("_parsed_space", {})
    if not isinstance(parsed, dict):
        parsed = {}

    genie_questions = _benchmarks_to_genie_format(benchmarks)
    if len(genie_questions) > max_questions:
        logger.warning(
            "Truncating benchmarks from %d to %d (Genie space limit)",
            len(genie_questions),
            max_questions,
        )
        genie_questions = genie_questions[:max_questions]

    parsed["benchmarks"] = {"questions": genie_questions}

    patch_space_config(w, space_id, parsed)

    logger.info(
        "Published %d benchmark questions to Genie space %s",
        len(genie_questions),
        space_id,
    )
    return len(genie_questions)
