"""
Metadata Optimizer — failure analysis, proposal generation, conflict detection.

Analyzes evaluation failures (ASI) + current metadata snapshot to produce
targeted metadata change proposals grouped by lever.  LLM calls use
Databricks Claude Opus 4.6 via the Foundation Model API.
"""

from __future__ import annotations

import copy
import json
import logging
import re
from collections import Counter, defaultdict
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    CONFLICT_RULES,
    FAILURE_TAXONOMY,
    GENERIC_FIX_PREFIXES,
    LEVER_1_2_COLUMN_PROMPT,
    LEVER_4_JOIN_DISCOVERY_PROMPT,
    LEVER_4_JOIN_SPEC_PROMPT,
    LEVER_5_INSTRUCTION_PROMPT,
    LEVER_NAMES,
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    LOW_RISK_PATCHES,
    MAX_PATCH_OBJECTS,
    MAX_VALUE_DICTIONARY_COLUMNS,
    MEDIUM_RISK_PATCHES,
    PATCH_TYPES,
    PROPOSAL_GENERATION_PROMPT,
    REGRESSION_THRESHOLD,
    REPEATABILITY_FIX_BY_ASSET,
    _LEVER_TO_PATCH_TYPE,
)

logger = logging.getLogger(__name__)

# ── Dual Persistence Paths ─────────────────────────────────────────────

DUAL_PERSIST_PATHS: dict[int, dict[str, str]] = {
    1: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "gold_layer_design/yaml/{domain}/*.yaml",
    },
    2: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/semantic/metric_views/*.yaml",
    },
    3: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/semantic/tvfs/*.sql",
    },
    4: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
    5: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
}


# ═══════════════════════════════════════════════════════════════════════
# 1. Failure-to-Lever Mapping (pure)
# ═══════════════════════════════════════════════════════════════════════


_JUDGE_TO_LEVER: dict[str, int] = {
    "schema_accuracy": 1,
    "syntax_validity": 1,
    "completeness": 2,
    "logical_accuracy": 2,
    "semantic_equivalence": 2,
    "result_correctness": 2,
    "asset_routing": 5,
}


def _map_to_lever(
    root_cause: str,
    asi_failure_type: str | None = None,
    blame_set: list | str | None = None,
    judge: str | None = None,
) -> int:
    """Map a failure root cause to its primary control lever (1-5).

    ASI ``failure_type`` takes precedence when present since it comes
    directly from the FAILURE_TAXONOMY and is more precise.
    Falls back to the judge name when rationale-based pattern extraction
    yields "other".
    """
    ft = asi_failure_type or root_cause

    if ft == "repeatability_issue":
        bs = str(blame_set).upper() if blame_set else ""
        return 5 if "TVF" in bs else 1

    mapping = {
        "wrong_column": 1,
        "wrong_table": 1,
        "description_mismatch": 1,
        "missing_synonym": 1,
        "wrong_aggregation": 2,
        "wrong_measure": 2,
        "missing_filter": 2,
        "missing_temporal_filter": 2,
        "tvf_parameter_error": 3,
        "wrong_join": 4,
        "missing_join_spec": 4,
        "wrong_join_spec": 4,
        "asset_routing_error": 5,
        "missing_instruction": 5,
        "ambiguous_question": 5,
    }

    if asi_failure_type and asi_failure_type in mapping:
        return mapping[asi_failure_type]
    if root_cause in mapping:
        return mapping[root_cause]
    if judge and judge in _JUDGE_TO_LEVER:
        return _JUDGE_TO_LEVER[judge]
    return 5


def _resolve_scope(lever: int, apply_mode: str = APPLY_MODE) -> str:
    """Determine where a patch is applied based on lever and apply_mode.

    Levers 4-5 are always ``genie_config`` (Genie Space native structures).
    Levers 1-3 are governed by ``apply_mode``.
    """
    if lever in (4, 5):
        return "genie_config"
    return apply_mode


# ═══════════════════════════════════════════════════════════════════════
# 2. Failure Clustering (pure)
# ═══════════════════════════════════════════════════════════════════════


def _extract_pattern(rationale: str) -> str:
    """Extract a generalizable pattern from a judge rationale string."""
    r = (rationale or "").lower()
    if not r:
        return "other"
    if "table" in r and ("wrong" in r or "missing" in r or "incorrect" in r):
        return "wrong_table"
    if "column" in r and ("wrong" in r or "missing" in r):
        return "wrong_column"
    if "aggregation" in r or "measure" in r:
        return "wrong_aggregation"
    if "join" in r:
        return "wrong_join"
    if "filter" in r or "where" in r:
        return "missing_filter"
    if "asset" in r or "routing" in r:
        return "asset_routing_error"
    if "instruction" in r or "ambiguous" in r or "unclear" in r:
        return "missing_instruction"
    return "other"


_SQL_KW = re.compile(r"\b(FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|CROSS\s+JOIN|FULL\s+JOIN)\s+", re.I)
_SQL_WHERE = re.compile(r"\bWHERE\b", re.I)
_SQL_GROUP = re.compile(r"\bGROUP\s+BY\b", re.I)
_SQL_MEASURE = re.compile(r"\bMEASURE\s*\(", re.I)
_SQL_TVF = re.compile(r"\b(\w+)\s*\((?:[^)]*,){2,}", re.I)


def _extract_sql_tables(sql: str) -> set[str]:
    """Extract table-like references after FROM/JOIN keywords."""
    tables: set[str] = set()
    for m in _SQL_KW.finditer(sql or ""):
        rest = sql[m.end():m.end() + 200].strip()
        token = rest.split()[0] if rest.split() else ""
        token = token.rstrip(",;)")
        if token and not token.upper().startswith("("):
            tables.add(token.lower())
    return tables


def _classify_sql_diff(ctx: dict) -> str:
    """Classify a failure's root cause by comparing expected vs generated SQL.

    Accepts either a ``sql_context`` dict (with ``expected_sql`` / ``generated_sql``
    keys) or a full row dict (with ``request`` / ``response`` keys).
    Falls back to ``"other"`` when the SQL pair is missing or no pattern matches.
    """
    expected_sql = (ctx.get("expected_sql") or "").strip()
    generated_sql = (ctx.get("generated_sql") or "").strip()

    if not expected_sql:
        req = ctx.get("request") or {}
        if isinstance(req, str):
            try:
                req = json.loads(req)
            except (json.JSONDecodeError, TypeError):
                req = {}
        expected_sql = (req.get("expected_sql") or "").strip()
    if not generated_sql:
        resp = ctx.get("response") or {}
        if isinstance(resp, str):
            try:
                resp = json.loads(resp)
            except (json.JSONDecodeError, TypeError):
                resp = {}
        generated_sql = (resp.get("response") or "").strip()

    if not expected_sql or not generated_sql:
        return "other"

    exp_lower = expected_sql.lower()
    gen_lower = generated_sql.lower()

    exp_tables = _extract_sql_tables(expected_sql)
    gen_tables = _extract_sql_tables(generated_sql)
    if exp_tables and gen_tables and exp_tables != gen_tables:
        missing = exp_tables - gen_tables
        extra = gen_tables - exp_tables
        if missing or extra:
            return "wrong_table"

    exp_has_join = bool(re.search(r"\bJOIN\b", exp_lower))
    gen_has_join = bool(re.search(r"\bJOIN\b", gen_lower))
    if exp_has_join != gen_has_join:
        return "wrong_join"

    exp_has_where = bool(_SQL_WHERE.search(exp_lower))
    gen_has_where = bool(_SQL_WHERE.search(gen_lower))
    if exp_has_where and not gen_has_where:
        return "missing_filter"

    exp_has_tvf = bool(_SQL_TVF.search(exp_lower))
    gen_has_tvf = bool(_SQL_TVF.search(gen_lower))
    if exp_has_tvf != gen_has_tvf:
        return "tvf_parameter_error"

    exp_has_measure = bool(_SQL_MEASURE.search(exp_lower))
    gen_has_measure = bool(_SQL_MEASURE.search(gen_lower))
    if exp_has_measure != gen_has_measure:
        return "wrong_measure"

    exp_has_group = bool(_SQL_GROUP.search(exp_lower))
    gen_has_group = bool(_SQL_GROUP.search(gen_lower))
    if exp_has_group != gen_has_group:
        return "wrong_aggregation"

    if exp_tables == gen_tables and exp_tables:
        return "wrong_column"

    return "missing_instruction"


def cluster_failures(eval_results: dict, metadata_snapshot: dict) -> list[dict]:
    """Group evaluation failures into actionable clusters.

    Groups by ``(judge, asi_failure_type, blame_set_str)``.  Falls back to
    ``(judge, _extract_pattern(rationale), "")`` when ASI is absent.
    Returns clusters with >= 1 question so even single failures are actionable.
    """
    failures: list[dict] = []
    table = None

    results_obj = eval_results.get("eval_result")
    if results_obj is not None and hasattr(results_obj, "tables"):
        table = results_obj.tables.get("eval_results")
    elif results_obj is not None and hasattr(results_obj, "eval_results"):
        table = results_obj.eval_results

    if table is None:
        rows = (
            eval_results.get("eval_results")
            or eval_results.get("rows")
            or eval_results.get("table")
        )
        if isinstance(rows, list):
            table = rows

    if table is None:
        return []

    try:
        import pandas as pd

        if hasattr(table, "iterrows"):
            rows_iter = [row.to_dict() for _, row in table.iterrows()]
        else:
            rows_iter = table if isinstance(table, list) else []
    except ImportError:
        rows_iter = table if isinstance(table, list) else []

    for row in rows_iter:
        if not isinstance(row, dict):
            continue

        _req = row.get("request") or {}
        if isinstance(_req, str):
            try:
                _req = json.loads(_req)
            except (json.JSONDecodeError, TypeError):
                _req = {}
        _req_kwargs = _req.get("kwargs", {}) if isinstance(_req, dict) else {}
        _resp = row.get("response") or {}
        if isinstance(_resp, str):
            try:
                _resp = json.loads(_resp)
            except (json.JSONDecodeError, TypeError):
                _resp = {}

        question_id = (
            row.get("inputs/question_id")
            or row.get("inputs/question")
            or row.get("question_id")
            or _req_kwargs.get("question_id")
            or (_req.get("question") if isinstance(_req, dict) else None)
            or row.get("question", "unknown")
        )

        sql_ctx = {
            "question": _req.get("question", "") if isinstance(_req, dict) else "",
            "expected_sql": _req.get("expected_sql", "") if isinstance(_req, dict) else "",
            "generated_sql": _resp.get("response", "") if isinstance(_resp, dict) else "",
            "comparison": _resp.get("comparison", {}) if isinstance(_resp, dict) else {},
        }

        for col_name, val in list(row.items()):
            judge: str | None = None
            if col_name.startswith("feedback/") and col_name.endswith("/value"):
                judge = col_name.removeprefix("feedback/").removesuffix("/value")
            elif col_name.startswith("feedback/") and "/value" not in col_name:
                judge = col_name.removeprefix("feedback/")
            elif col_name.endswith("/value") and not col_name.startswith("feedback/"):
                judge = col_name.removesuffix("/value")
            if judge and "no" in str(val).lower():
                rationale = (
                    row.get(f"feedback/{judge}/rationale")
                    or row.get(f"{judge}/rationale")
                    or row.get(f"rationale/{judge}")
                    or row.get("rationale", "")
                )
                judge_meta = (
                    row.get(f"feedback/{judge}/metadata")
                    or row.get(f"{judge}/metadata")
                    or {}
                )
                if not isinstance(judge_meta, dict):
                    try:
                        judge_meta = json.loads(judge_meta) if isinstance(judge_meta, str) else {}
                    except (json.JSONDecodeError, TypeError):
                        judge_meta = {}
                if not judge_meta:
                    from genie_space_optimizer.optimization.evaluation import (
                        _parse_asi_from_rationale,
                    )
                    judge_meta = _parse_asi_from_rationale(rationale)
                asi_failure_type = (
                    judge_meta.get("failure_type")
                    or row.get(f"metadata/{judge}/failure_type")
                    or row.get("metadata/failure_type")
                )
                asi_blame_set = (
                    judge_meta.get("blame_set")
                    or row.get(f"metadata/{judge}/blame_set")
                    or row.get("metadata/blame_set")
                )
                asi_counterfactual = (
                    judge_meta.get("counterfactual_fix")
                    or row.get(f"metadata/{judge}/counterfactual_fix")
                    or row.get("metadata/counterfactual_fix")
                )
                asi_wrong_clause = (
                    judge_meta.get("wrong_clause")
                    or row.get(f"metadata/{judge}/wrong_clause")
                )
                failures.append(
                    {
                        "question_id": question_id,
                        "judge": judge,
                        "rationale": rationale,
                        "asi_failure_type": asi_failure_type,
                        "asi_blame_set": asi_blame_set,
                        "asi_counterfactual_fix": asi_counterfactual,
                        "asi_wrong_clause": asi_wrong_clause,
                        "sql_context": sql_ctx,
                    }
                )

    pattern_groups: dict[tuple, list] = defaultdict(list)
    for f in failures:
        if f.get("asi_failure_type"):
            blame = str(f.get("asi_blame_set", "")) if f.get("asi_blame_set") else ""
            key = (f["judge"], f["asi_failure_type"], blame)
        else:
            pattern = _extract_pattern(f["rationale"])
            if pattern == "other":
                pattern = _classify_sql_diff(f.get("sql_context", {}))
            key = (f["judge"], pattern, "")
        pattern_groups[key].append(f)

    clusters: list[dict] = []
    for group_key, items in pattern_groups.items():
        judge, pattern, blame = group_key[0], group_key[1], group_key[2] if len(group_key) > 2 else ""
        sample_asi_type = next(
            (i["asi_failure_type"] for i in items if i.get("asi_failure_type")), None
        )
        sql_contexts = [i["sql_context"] for i in items if i.get("sql_context")]
        entry = {
            "cluster_id": f"C{len(clusters) + 1:03d}",
            "root_cause": pattern,
            "question_ids": [i["question_id"] for i in items],
            "affected_judge": judge,
            "confidence": min(0.9, 0.5 + 0.1 * len(items)),
            "asi_failure_type": sample_asi_type,
            "asi_blame_set": blame or None,
            "asi_wrong_clause": next(
                (i["asi_wrong_clause"] for i in items if i.get("asi_wrong_clause")), None
            ),
            "asi_counterfactual_fixes": [
                i["asi_counterfactual_fix"]
                for i in items
                if i.get("asi_counterfactual_fix")
            ],
            "sql_contexts": sql_contexts[:5],
        }
        if len(items) >= 1:
            clusters.append(entry)

    clusters.sort(key=lambda c: len(c["question_ids"]), reverse=True)
    return clusters


# ═══════════════════════════════════════════════════════════════════════
# 2b. UC Type Enrichment & Join Discovery (Lever 4)
# ═══════════════════════════════════════════════════════════════════════


def enrich_metadata_with_uc_types(
    metadata_snapshot: dict,
    uc_columns: list[dict],
) -> None:
    """Merge UC ``data_type`` and ``comment`` into metadata_snapshot column_configs.

    Mutates *metadata_snapshot* in place.  Each UC row is matched by
    ``table_name`` (unqualified) + ``column_name`` against the tables in
    ``data_sources.tables[].column_configs``.  If a column_config already has
    ``data_type`` set it is left unchanged.
    """
    if not uc_columns:
        return

    uc_lookup: dict[tuple[str, str], dict] = {}
    for row in uc_columns:
        if not isinstance(row, dict):
            continue
        tbl = str(row.get("table_name") or row.get("table") or "").strip().lower()
        col = str(row.get("column_name") or row.get("column") or "").strip().lower()
        if tbl and col:
            uc_lookup[(tbl, col)] = row

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    enriched = 0
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = ident.rsplit(".", 1)[-1].lower() if ident else ""
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            if not isinstance(cc, dict):
                continue
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            uc_row = uc_lookup.get((short, col_name))
            if uc_row is None:
                continue
            if not cc.get("data_type"):
                dt = uc_row.get("data_type") or uc_row.get("type") or ""
                if dt:
                    cc["data_type"] = str(dt).upper()
                    enriched += 1
            if not cc.get("uc_comment"):
                comment = uc_row.get("comment") or ""
                if comment:
                    cc["uc_comment"] = str(comment)
    logger.info("UC type enrichment: updated %d column_configs", enriched)


_JOIN_KEY_SUFFIXES = ("_key", "_id", "_code", "_fk", "_ref", "_num", "_no")

_COMPATIBLE_TYPE_GROUPS: list[set[str]] = [
    {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "LONG", "SHORT"},
    {"FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"},
    {"STRING", "VARCHAR", "CHAR"},
    {"DATE"},
    {"TIMESTAMP", "TIMESTAMP_NTZ"},
    {"BOOLEAN"},
]


def _short_name(identifier: str) -> str:
    """Return the unqualified table name from a fully-qualified identifier."""
    return identifier.rsplit(".", 1)[-1] if "." in identifier else identifier


def _strip_suffix(col: str) -> str:
    """Strip known join-key suffixes to get the column stem."""
    lower = col.lower()
    for sfx in _JOIN_KEY_SUFFIXES:
        if lower.endswith(sfx):
            return lower[: -len(sfx)]
    return lower


def _is_fuzzy_match(col_a: str, col_b: str) -> bool:
    """Check whether two column names are fuzzy-matches.

    Returns True when:
    - One full name is a substring of the other
    - They share the same stem after stripping join-key suffixes
    - One *stem* is a substring of the other stem (handles ``prod`` vs ``product``)
    """
    la, lb = col_a.lower(), col_b.lower()
    if la == lb:
        return True
    if la in lb or lb in la:
        return True
    stem_a = _strip_suffix(la)
    stem_b = _strip_suffix(lb)
    if not stem_a or not stem_b:
        return False
    if stem_a == stem_b:
        return True
    if stem_a in stem_b or stem_b in stem_a:
        return True
    return False


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Return True if two UC data types are join-compatible.

    When either type is unknown (empty) we assume compatibility so
    that the LLM can make the final decision.
    """
    if not type_a or not type_b:
        return True
    a, b = type_a.upper().split("(")[0].strip(), type_b.upper().split("(")[0].strip()
    if a == b:
        return True
    for group in _COMPATIBLE_TYPE_GROUPS:
        if a in group and b in group:
            return True
    return False


def discover_join_candidates(metadata_snapshot: dict) -> list[dict]:
    """Discover potential join relationships and return **hints** for the LLM.

    Scans all table pairs for columns that look like join keys using:

    * Exact name matching on key-suffix columns
    * Fuzzy name matching (substring / shared stem)
    * Data-type compatibility filtering (when types are enriched)

    Existing join specs are excluded.  Returns a list of hint dicts
    (not final join specs) that feed into the LLM discovery prompt.

    Each hint has the shape::

        {
            "left_table": str,
            "right_table": str,
            "candidate_columns": [
                {"left_col": str, "right_col": str, "reason": str}
            ],
            "type_compatible": bool,
        }
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    join_specs = metadata_snapshot.get("join_specs", []) or ds.get("join_specs", [])

    existing_pairs: set[tuple[str, str]] = set()
    for spec in join_specs:
        if not isinstance(spec, dict):
            continue
        left_obj = spec.get("left", {})
        right_obj = spec.get("right", {})
        if isinstance(left_obj, dict) and isinstance(right_obj, dict):
            lt = left_obj.get("identifier", "")
            rt = right_obj.get("identifier", "")
        else:
            lt = spec.get("left_table_name", "")
            rt = spec.get("right_table_name", "")
        if lt and rt:
            existing_pairs.add((lt, rt))
            existing_pairs.add((rt, lt))

    # Build per-table column info: {identifier: [{name, data_type}, ...]}
    table_col_info: dict[str, list[dict[str, str]]] = {}
    for t in tables:
        if not isinstance(t, dict):
            continue
        ident = t.get("identifier", "") or t.get("name", "")
        if not ident:
            continue
        cols: list[dict[str, str]] = []
        for cc in t.get("column_configs", t.get("columns", [])):
            col = cc.get("column_name") or cc.get("name", "")
            if not col:
                continue
            dt = cc.get("data_type", "")
            cols.append({"name": col.lower(), "data_type": str(dt).upper() if dt else ""})
        if cols:
            table_col_info[ident] = cols

    # Only consider columns that look like join keys for the suffix matching
    # but also include all columns for fuzzy matching
    def _key_cols(cols: list[dict[str, str]]) -> list[dict[str, str]]:
        return [c for c in cols if any(c["name"].endswith(s) for s in _JOIN_KEY_SUFFIXES)]

    hints: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()
    idents = list(table_col_info.keys())

    for i, ident_a in enumerate(idents):
        for ident_b in idents[i + 1:]:
            if (ident_a, ident_b) in existing_pairs:
                continue

            _a, _b = sorted((ident_a, ident_b))
            pair_key = (_a, _b)
            if pair_key in seen_pairs:
                continue

            cols_a = table_col_info[ident_a]
            cols_b = table_col_info[ident_b]
            key_a = _key_cols(cols_a)
            key_b = _key_cols(cols_b)

            candidate_columns: list[dict[str, str]] = []
            all_types_compatible = True

            # 1) Exact name match on key-suffix columns
            names_b = {c["name"]: c for c in cols_b}
            for ca in key_a:
                cb = names_b.get(ca["name"])
                if cb:
                    compat = _types_compatible(ca["data_type"], cb["data_type"])
                    if not compat:
                        all_types_compatible = False
                    candidate_columns.append({
                        "left_col": ca["name"],
                        "right_col": cb["name"],
                        "reason": f"exact name match (suffix key){'' if compat else ' [TYPE MISMATCH]'}",
                    })

            # 2) Fuzzy name match on key-suffix columns (only if no exact match)
            exact_left = {c["left_col"] for c in candidate_columns}
            for ca in key_a:
                if ca["name"] in exact_left:
                    continue
                for cb in key_b:
                    if ca["name"] == cb["name"]:
                        continue
                    if _is_fuzzy_match(ca["name"], cb["name"]):
                        compat = _types_compatible(ca["data_type"], cb["data_type"])
                        if not compat:
                            all_types_compatible = False
                        candidate_columns.append({
                            "left_col": ca["name"],
                            "right_col": cb["name"],
                            "reason": f"fuzzy name match (stem/substring){'' if compat else ' [TYPE MISMATCH]'}",
                        })

            if not candidate_columns:
                continue

            seen_pairs.add(pair_key)
            hints.append({
                "left_table": ident_a,
                "right_table": ident_b,
                "candidate_columns": candidate_columns,
                "type_compatible": all_types_compatible,
            })

    logger.info(
        "Join discovery: found %d hint pairs (%d existing specs)",
        len(hints), len(existing_pairs) // 2,
    )
    return hints


# ═══════════════════════════════════════════════════════════════════════
# 3. ASI Extraction
# ═══════════════════════════════════════════════════════════════════════


def read_asi_from_uc(
    spark: Any,
    mlflow_run_id: str,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Query ``genie_eval_asi_results`` Delta table via Spark."""
    table = f"{catalog}.{schema}.genie_eval_asi_results"
    try:
        df = spark.sql(
            f"""
            SELECT run_id, iteration, question_id, judge, value,
                   failure_type, severity, confidence, blame_set,
                   counterfactual_fix, wrong_clause, expected_value,
                   actual_value, missing_metadata, ambiguity_detected
            FROM {table}
            WHERE run_id = '{mlflow_run_id}'
            ORDER BY question_id, judge
            """
        )
        rows: list[dict] = []
        for r in df.collect():
            row_dict = r.asDict()
            if row_dict.get("blame_set"):
                try:
                    row_dict["blame_set"] = json.loads(row_dict["blame_set"])
                except (json.JSONDecodeError, TypeError):
                    row_dict["blame_set"] = [row_dict["blame_set"]]
            rows.append(row_dict)
        return rows
    except Exception:
        logger.exception("read_asi_from_uc failed")
        return []


def _extract_asi_from_assessments(assessments: list) -> list[dict]:
    """Parse ASI metadata from MLflow assessments list."""
    feedbacks: list[dict] = []
    for a in assessments:
        if not isinstance(a, dict):
            continue
        meta = a.get("metadata", {})
        if not isinstance(meta, dict):
            continue
        feedbacks.append(
            {
                "value": a.get("value", ""),
                "judge": a.get("name", ""),
                "question_id": a.get("question_id", ""),
                "failure_type": meta.get("failure_type", ""),
                "blame_set": meta.get("blame_set", []),
                "counterfactual_fix": [meta.get("counterfactual_fix", "")],
                "confidence": float(meta.get("confidence", 0.5)),
                "asi_severity": meta.get("severity", ""),
                "asi_wrong_clause": meta.get("wrong_clause", ""),
                "asi_expected_value": meta.get("expected_value", ""),
                "asi_actual_value": meta.get("actual_value", ""),
                "asi_missing_metadata": meta.get("missing_metadata", ""),
                "asi_ambiguity_detected": meta.get("ambiguity_detected", False),
            }
        )
    return feedbacks


def _extract_judge_feedbacks_from_eval(
    eval_results: dict,
    catalog: str = "",
    schema: str = "",
    warehouse_id: str = "",
) -> list[dict]:
    """Extract judge feedback dicts from eval results using UC-first priority chain."""
    direct = eval_results.get("judge_feedbacks") or eval_results.get("feedbacks")
    if isinstance(direct, list) and direct:
        return direct

    rows = (
        eval_results.get("eval_results")
        or eval_results.get("rows")
        or eval_results.get("table")
    )
    if not isinstance(rows, list):
        return []

    feedbacks: list[dict] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        for col, val in list(row.items()):
            if col.endswith("/value") and str(val).lower() in ("no", "false"):
                judge = col.replace("/value", "")
                meta_col = f"{judge}/metadata"
                meta = row.get(meta_col, {})
                if isinstance(meta, dict) and meta.get("failure_type"):
                    feedbacks.append(
                        {
                            "value": val,
                            "judge": judge,
                            "failure_type": meta.get("failure_type", ""),
                            "blame_set": meta.get("blame_set", []),
                            "counterfactual_fix": [meta.get("counterfactual_fix", "")],
                            "confidence": float(meta.get("confidence", 0.7)),
                            "question_id": row.get(
                                "inputs/question_id",
                                (row.get("inputs", {}) or {}).get("question_id", f"q{i}"),
                            ),
                            "feedback_id": f"r{i}_{judge}",
                        }
                    )
                    continue

            if col.startswith("feedback/") and "no" in str(val).lower():
                judge = col.replace("feedback/", "")
                rationale = row.get(f"rationale/{judge}", row.get("rationale", ""))
                feedbacks.append(
                    {
                        "value": val,
                        "blame_set": _infer_blame_from_rationale(rationale),
                        "counterfactual_fix": [],
                        "feedback_id": f"r{i}_{judge}",
                        "question_id": row.get(
                            "inputs/question", row.get("question", f"q{i}")
                        ),
                        "confidence": 0.7,
                    }
                )
    return feedbacks


def _infer_blame_from_rationale(rationale: str, metadata_snapshot: dict | None = None) -> list[str]:
    """Infer blame_set from judge rationale for grouping."""
    r = (rationale or "").lower()
    blame: list[str] = []
    if "table" in r:
        blame.append("tables")
    if "column" in r:
        blame.append("column_metadata")
    if "join" in r:
        blame.append("joins")
    if "filter" in r:
        blame.append("filters")
    if "instruction" in r:
        blame.append("instructions")
    return blame if blame else ["_ungrouped"]


# ═══════════════════════════════════════════════════════════════════════
# 4. LLM-powered Proposal Generation
# ═══════════════════════════════════════════════════════════════════════


def _extract_metadata_for_blame(
    metadata_snapshot: dict, blame_set: Any
) -> str:
    """Extract relevant metadata sections for the blamed objects.

    Handles fully-qualified names (``catalog.schema.table``) by matching
    on the last dotted component as well as the full string.
    """
    if not blame_set or not metadata_snapshot:
        return "(no metadata available)"

    blame_items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
    blame_lower = set()
    for b in blame_items:
        bl = b.lower().strip()
        blame_lower.add(bl)
        if "." in bl:
            blame_lower.add(bl.rsplit(".", 1)[-1])

    sections: list[str] = []
    matched_tables: set[str] = set()

    for table in metadata_snapshot.get("tables", []):
        table_name = table.get("name") or table.get("identifier", "")
        tn_lower = table_name.lower()
        short_name = tn_lower.rsplit(".", 1)[-1] if "." in tn_lower else tn_lower
        if any(b in tn_lower or b == short_name for b in blame_lower):
            if table_name in matched_tables:
                continue
            matched_tables.add(table_name)
            sections.append(f"Table: {table_name}")
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                desc = col.get("description", "")
                if isinstance(desc, list):
                    desc = " ".join(desc)
                sections.append(f"  Column: {col_name} — {desc}")

    for b in blame_lower:
        for table in metadata_snapshot.get("tables", []):
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                if b == col_name.lower():
                    desc = col.get("description", "")
                    if isinstance(desc, list):
                        desc = " ".join(desc)
                    tname = table.get("name") or table.get("identifier", "")
                    sections.append(f"Column {tname}.{col_name}: {desc}")

    instructions = metadata_snapshot.get("general_instructions", "")
    if instructions and any("instruction" in b for b in blame_lower):
        sections.append(f"Instructions: {instructions[:500]}")

    return "\n".join(sections) if sections else "(blamed objects not found in metadata)"


def _format_full_schema_context(metadata_snapshot: dict) -> str:
    """Build a full schema summary of all tables, columns, descriptions, and synonyms.

    Gives the LLM complete visibility into the Genie Space structure so it can
    make informed decisions about which columns need descriptions vs. which
    should inherit from Unity Catalog, and which synonyms already exist.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    lines: list[str] = []
    for tbl in tables:
        identifier = tbl.get("identifier", "")
        tbl_desc = tbl.get("description", [])
        if isinstance(tbl_desc, list):
            tbl_desc = " ".join(tbl_desc)
        lines.append(f"### Table: {identifier}")
        if tbl_desc:
            lines.append(f"  Description: {tbl_desc}")
        for cc in tbl.get("column_configs", []):
            col_name = cc.get("column_name", "")
            data_type = cc.get("data_type", "")
            desc = cc.get("description", [])
            if isinstance(desc, list):
                desc = " ".join(desc) if desc else ""
            uc_comment = cc.get("uc_comment", "")
            if not desc and uc_comment:
                desc = uc_comment
            syns = cc.get("synonyms", [])
            type_part = f" ({data_type})" if data_type else ""
            desc_part = f" -- {desc}" if desc else " -- (from UC)"
            syn_part = f" | synonyms: {syns}" if syns else ""
            lines.append(f"  - `{col_name}`{type_part}{desc_part}{syn_part}")
    return "\n".join(lines) if lines else "(no schema available)"


def _describe_patch_type(patch_type: str) -> str:
    """Human-readable description of a patch type."""
    pt_info = PATCH_TYPES.get(patch_type)
    if pt_info:
        return (
            f"{patch_type}: scope={pt_info['scope']}, "
            f"risk={pt_info['risk_level']}, affects={pt_info['affects']}"
        )
    return patch_type


def _format_sql_diffs(cluster: dict) -> str:
    """Build a human-readable summary of SQL diffs for the LLM prompt."""
    sql_contexts = cluster.get("sql_contexts", [])
    if not sql_contexts:
        return "(no SQL context available)"
    lines: list[str] = []
    for idx, ctx in enumerate(sql_contexts[:3], 1):
        q = ctx.get("question", "")
        exp = ctx.get("expected_sql", "")
        gen = ctx.get("generated_sql", "")
        comp = ctx.get("comparison", {})
        lines.append(f"### Question {idx}: {q}")
        lines.append(f"**Expected SQL:**\n```sql\n{exp}\n```")
        lines.append(f"**Generated SQL:**\n```sql\n{gen}\n```")
        if isinstance(comp, dict) and comp.get("error"):
            lines.append(f"**Error:** {comp['error']}")
        elif isinstance(comp, dict) and not comp.get("match"):
            match_type = comp.get("match_type", "unknown")
            lines.append(f"**Mismatch type:** {match_type}")
        lines.append("")
    cf = cluster.get("asi_counterfactual_fixes", [])
    if cf:
        lines.append("**Suggested fixes from judges:**")
        for fix in cf[:3]:
            lines.append(f"- {fix}")
    return "\n".join(lines)


def _derive_blame_from_sql(cluster: dict) -> list[str] | None:
    """Derive blamed table/column references from SQL diffs when ASI blame_set is empty."""
    sql_contexts = cluster.get("sql_contexts", [])
    if not sql_contexts:
        return None
    blamed: set[str] = set()
    for ctx in sql_contexts[:3]:
        exp_tables = _extract_sql_tables(ctx.get("expected_sql", ""))
        gen_tables = _extract_sql_tables(ctx.get("generated_sql", ""))
        blamed.update(exp_tables | gen_tables)
    return sorted(blamed)[:5] if blamed else None


def _format_existing_example_sqls(metadata_snapshot: dict) -> str:
    """Format existing example_question_sqls for inclusion in the Lever 6 prompt."""
    example_sqls = metadata_snapshot.get("example_question_sqls", [])
    if not example_sqls:
        return "(none)"
    lines: list[str] = []
    for ex in example_sqls[:20]:
        if not isinstance(ex, dict):
            continue
        q = ex.get("question", "")
        if isinstance(q, list):
            q = q[0] if q else ""
        sql = ex.get("sql", "")
        if isinstance(sql, list):
            sql = sql[0] if sql else ""
        if not q:
            continue
        entry = f"- Q: {q}\n  SQL: {sql[:200]}"
        params = ex.get("parameters", [])
        if params:
            param_strs = [
                f"{p.get('name', '?')}:{p.get('type_hint', 'STRING')}"
                for p in params if isinstance(p, dict)
            ]
            entry += f"\n  Params: {', '.join(param_strs)}"
        guidance = ex.get("usage_guidance", [])
        if guidance:
            g = guidance[0] if isinstance(guidance, list) else str(guidance)
            entry += f"\n  Guidance: {g[:150]}"
        lines.append(entry)
    return "\n".join(lines) if lines else "(none)"


def _resolve_lever5_llm_result(
    llm_result: dict, original_patch_type: str
) -> tuple[str, dict]:
    """Interpret the instruction_type returned by the Lever 6 LLM and resolve
    the actual patch_type and extra fields to merge into the proposal.

    Returns ``(resolved_patch_type, extra_fields)``.
    """
    instruction_type = llm_result.get("instruction_type", "text_instruction")

    if instruction_type == "example_sql":
        return "add_example_sql", {
            "example_question": llm_result.get("example_question", ""),
            "example_sql": llm_result.get("example_sql", ""),
            "parameters": llm_result.get("parameters", []),
            "usage_guidance": llm_result.get("usage_guidance", ""),
        }

    if instruction_type == "sql_expression":
        logger.info(
            "Lever 6 LLM recommended sql_expression (handled by levers 1-5): "
            "table=%s column=%s — falling back to example_sql",
            llm_result.get("target_table", ""),
            llm_result.get("target_column", ""),
        )
        return original_patch_type, {}

    if original_patch_type == "add_example_sql":
        logger.warning(
            "Lever 6 LLM returned text_instruction for a routing failure "
            "(original_patch_type=add_example_sql). Example SQL is preferred "
            "for routing issues. Keeping text_instruction but marking as downgraded."
        )

    return "add_instruction", {
        "new_text": llm_result.get("instruction_text", ""),
        "downgraded_from_example_sql": original_patch_type == "add_example_sql",
    }


def _call_llm_for_proposal(
    cluster: dict,
    metadata_snapshot: dict,
    patch_type: str,
    lever: int,
) -> dict:
    """Call Databricks Claude Opus 4.6 to generate proposal text.

    Returns ``{"proposed_value": str, "rationale": str}``.
    For lever 5 the response may also contain ``instruction_type``,
    ``example_question``, ``example_sql``, ``target_table``, etc.
    """
    prompt_map = {
        1: LEVER_1_2_COLUMN_PROMPT,
        2: LEVER_1_2_COLUMN_PROMPT,
        4: LEVER_4_JOIN_SPEC_PROMPT,
        5: LEVER_5_INSTRUCTION_PROMPT,
    }
    prompt_template = prompt_map.get(lever, PROPOSAL_GENERATION_PROMPT)

    current_dict_count = sum(
        1
        for t in metadata_snapshot.get("tables", [])
        for c in t.get("column_configs", [])
        if c.get("enable_entity_matching")
    )

    sql_diffs = _format_sql_diffs(cluster)
    blame = cluster.get("asi_blame_set")
    if not blame:
        blame = _derive_blame_from_sql(cluster)

    existing_example_sqls = _format_existing_example_sqls(metadata_snapshot)

    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    _mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
    _funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])
    _join_specs = metadata_snapshot.get("join_specs", []) or ds.get("join_specs", [])

    format_kwargs: dict[str, Any] = {
        "failure_type": cluster.get("asi_failure_type", cluster.get("root_cause", "")),
        "blame_set": blame or "",
        "affected_questions": cluster.get("question_ids", []),
        "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
        "severity": "major",
        "current_metadata": _extract_metadata_for_blame(
            metadata_snapshot, blame
        ),
        "patch_type_description": _describe_patch_type(patch_type),
        "failures_context": json.dumps(cluster, default=str),
        "sql_diffs": sql_diffs,
        "current_join_specs": json.dumps(_join_specs, default=str),
        "table_relationships": json.dumps(
            [t.get("relationships", []) for t in _tables],
            default=str,
        ),
        "current_column_configs": json.dumps(
            metadata_snapshot.get("column_configs", {}), default=str
        ),
        "full_schema_context": _format_full_schema_context(metadata_snapshot),
        "string_column_count": metadata_snapshot.get("string_column_count", 0),
        "max_value_dictionary_cols": MAX_VALUE_DICTIONARY_COLUMNS,
        "current_dictionary_count": current_dict_count,
        "current_instructions": metadata_snapshot.get("general_instructions", ""),
        "existing_example_sqls": existing_example_sqls,
        "instruction_char_budget": max(
            0,
            24500 - len(metadata_snapshot.get("general_instructions", "")),
        ),
        "table_names": [
            t.get("name") or t.get("identifier", "")
            for t in _tables
        ],
        "mv_names": [
            m.get("name") or m.get("identifier", "")
            for m in _mvs
        ],
        "tvf_names": [
            f.get("name") or f.get("identifier", "")
            for f in _funcs
        ],
    }

    try:
        prompt = prompt_template.format(**format_kwargs)
    except KeyError:
        prompt = prompt_template.format_map(defaultdict(str, format_kwargs))

    _tmpl_name = {1: "LEVER_1_2_COLUMN", 2: "LEVER_1_2_COLUMN", 4: "LEVER_4_JOIN_SPEC", 5: "LEVER_5_INSTRUCTION"}.get(lever, "PROPOSAL_GENERATION")
    _W = 78
    _hdr = f"┌─── LLM Call [{_tmpl_name}] " + "─" * max(0, _W - 18 - len(_tmpl_name))
    _ftr = "└" + "─" * (_W - 1)

    print(
        f"\n{_hdr}\n"
        f"│ {'Patch type:':<24s} {patch_type}\n"
        f"│ {'Failure type:':<24s} {format_kwargs.get('failure_type', '?')}\n"
        f"│ {'Blame set:':<24s} {format_kwargs.get('blame_set', '?')}\n"
        f"│ {'Questions:':<24s} {len(format_kwargs.get('affected_questions', []))}\n"
        f"│ {'Prompt length:':<24s} {len(prompt):,} chars\n│"
    )

    import time

    from databricks.sdk import WorkspaceClient as _WC
    from genie_space_optimizer.optimization.evaluation import _extract_json

    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            w = _WC()
            response = w.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.SYSTEM,
                        content=(
                            "You are a JSON-only responder. Your ENTIRE response must be a single "
                            "valid JSON object. Do not include any analysis, markdown, commentary, "
                            "or explanation outside the JSON. Start your response with '{' and end "
                            "with '}'."
                        ),
                    ),
                    ChatMessage(role=ChatMessageRole.USER, content=prompt),
                ],
                temperature=LLM_TEMPERATURE,
            )
            choices = getattr(response, "choices", None) or []
            if not choices:
                raise ValueError("LLM response had no choices")
            first_choice = choices[0]
            message = getattr(first_choice, "message", None)
            content = getattr(message, "content", None)
            if not content:
                raise ValueError("LLM response content is empty")
            text = str(content).strip()
            parsed = _extract_json(text)
            print(
                f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} OK -- parsed JSON\n"
                f"│ {'Proposed value:':<24s} {str(parsed.get('proposed_value', ''))[:300]}\n"
                f"│ {'Rationale:':<24s} {str(parsed.get('rationale', ''))[:300]}\n"
                f"│ {'Result:':<24s} OK\n"
                f"{_ftr}"
            )
            return parsed
        except json.JSONDecodeError:
            if attempt < LLM_MAX_RETRIES - 1:
                print(f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} non-JSON (retrying...)")
                time.sleep(2**attempt)
                continue
            print(
                f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} non-JSON -- retries exhausted\n"
                f"│ Raw text (500 chars): {text[:500]}\n"
                f"│\n"
                f"│ {'Result:':<24s} FALLBACK (raw text kept as proposed_value)\n"
                f"{_ftr}"
            )
            return {"proposed_value": text, "rationale": "LLM response was not valid JSON after all retries"}
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                print(f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} error (retrying...)")
                time.sleep(2**attempt)
            else:
                logger.exception("LLM [%s] call failed after %d retries", _tmpl_name, LLM_MAX_RETRIES)
                print(
                    f"│ Attempt {attempt + 1}/{LLM_MAX_RETRIES}:{'':9s} error -- retries exhausted\n"
                    f"│ {'Result:':<24s} FAILED\n"
                    f"{_ftr}"
                )
                return {
                    "proposed_value": "",
                    "rationale": "LLM call failed",
                }
    return {
        "proposed_value": "",
        "rationale": "LLM call failed",
    }


def _format_discovery_hints(hints: list[dict]) -> str:
    """Format discovery hints into a human-readable string for the LLM prompt."""
    if not hints:
        return "(no heuristic hints)"
    lines: list[str] = []
    for idx, h in enumerate(hints, 1):
        lt = h.get("left_table", "")
        rt = h.get("right_table", "")
        compat = h.get("type_compatible", True)
        lines.append(f"### Hint {idx}: {lt} ↔ {rt}")
        if not compat:
            lines.append("  ⚠️  Some candidate columns have mismatched types")
        for cc in h.get("candidate_columns", []):
            lines.append(
                f"  - {cc.get('left_col', '?')} ↔ {cc.get('right_col', '?')} "
                f"({cc.get('reason', 'unknown')})"
            )
    return "\n".join(lines)


def _call_llm_for_join_discovery(
    metadata_snapshot: dict,
    hints: list[dict],
    w: WorkspaceClient | None = None,
) -> list[dict]:
    """Call the LLM with the discovery prompt to validate and refine join hints.

    Returns a list of ``{"join_spec": {...}, "rationale": str}`` dicts.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _join_specs = metadata_snapshot.get("join_specs", []) or ds.get("join_specs", [])

    format_kwargs = {
        "full_schema_context": _format_full_schema_context(metadata_snapshot),
        "current_join_specs": json.dumps(_join_specs, default=str),
        "discovery_hints": _format_discovery_hints(hints),
    }
    try:
        prompt = LEVER_4_JOIN_DISCOVERY_PROMPT.format(**format_kwargs)
    except KeyError:
        prompt = LEVER_4_JOIN_DISCOVERY_PROMPT.format_map(defaultdict(str, format_kwargs))

    logger.info(
        "\n"
        "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] INPUT ────────────────────────────────\n"
        "│ Hints: %d\n"
        "│ Existing join specs: %d\n"
        "│ Prompt length: %d chars\n"
        "└─────────────────────────────────────────────────────────────────────────",
        len(hints), len(_join_specs), len(prompt),
    )

    import time

    from databricks.sdk import WorkspaceClient as _WC

    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            wc = w or _WC()
            response = wc.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
                temperature=LLM_TEMPERATURE,
                max_tokens=2048,
            )
            choices = getattr(response, "choices", None) or []
            if not choices:
                raise ValueError("LLM response had no choices")
            first_choice = choices[0]
            message = getattr(first_choice, "message", None)
            content = getattr(message, "content", None)
            if not content:
                raise ValueError("LLM response content is empty")
            text = str(content).strip()
            if text.startswith("```"):
                text = re.sub(r"^```(?:json)?\s*", "", text)
                text = re.sub(r"\s*```$", "", text)
            result = json.loads(text)
            specs = result.get("join_specs", [])
            rationale = result.get("rationale", "")
            out = [
                {"join_spec": s, "rationale": rationale}
                for s in specs
                if isinstance(s, dict)
            ]
            logger.info(
                "\n"
                "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] RESPONSE ─────────────────────────\n"
                "│ Join specs returned: %d\n"
                "│ Rationale: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                len(out), str(rationale)[:300],
            )
            return out
        except json.JSONDecodeError:
            logger.warning(
                "\n"
                "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] RESPONSE (non-JSON) ──────────────\n"
                "│ Raw text: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                text[:500],
            )
            return []
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                logger.exception(
                    "\n"
                    "┌─── OPTIMIZER LLM [JOIN_DISCOVERY] ERROR ────────────────────────────\n"
                    "│ Prompt len: %d chars | Retries: %d\n"
                    "└─────────────────────────────────────────────────────────────────────────",
                    len(prompt), LLM_MAX_RETRIES,
                )
                return []
    return []


def validate_join_spec_types(
    join_spec: dict,
    metadata_snapshot: dict,
) -> tuple[bool, str]:
    """Validate that join columns have compatible data types.

    Parses the ``sql`` array to extract column names from the join condition,
    looks up their ``data_type`` in the enriched metadata, and checks
    compatibility.

    Returns ``(valid, reason)`` — if valid is False, *reason* explains why.
    """
    sql_parts = join_spec.get("sql", [])
    if not sql_parts:
        return True, "no sql to validate"

    condition = sql_parts[0] if isinstance(sql_parts, list) else str(sql_parts)

    left_obj = join_spec.get("left", {})
    right_obj = join_spec.get("right", {})
    left_alias = left_obj.get("alias", "") if isinstance(left_obj, dict) else ""
    right_alias = right_obj.get("alias", "") if isinstance(right_obj, dict) else ""
    left_ident = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
    right_ident = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""

    # Parse "= " join conditions: `left_alias`.`col` = `right_alias`.`col`
    pattern = r"`([^`]+)`\s*\.\s*`([^`]+)`\s*=\s*`([^`]+)`\s*\.\s*`([^`]+)`"
    match = re.search(pattern, condition)
    if not match:
        return True, "could not parse join condition columns"

    cond_left_alias, cond_left_col = match.group(1), match.group(2)
    cond_right_alias, cond_right_col = match.group(3), match.group(4)

    # Build column type lookup from metadata
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    col_types: dict[tuple[str, str], str] = {}  # (short_table, col_name) -> data_type
    for tbl in tables:
        if not isinstance(tbl, dict):
            continue
        ident = tbl.get("identifier", "") or tbl.get("name", "")
        short = _short_name(ident).lower()
        for cc in tbl.get("column_configs", tbl.get("columns", [])):
            col_name = (cc.get("column_name") or cc.get("name", "")).lower()
            dt = cc.get("data_type", "")
            if col_name and dt:
                col_types[(short, col_name)] = str(dt).upper()

    left_type = col_types.get((cond_left_alias.lower(), cond_left_col.lower()), "")
    right_type = col_types.get((cond_right_alias.lower(), cond_right_col.lower()), "")

    if not left_type or not right_type:
        return True, "type info unavailable, skipping validation"

    if _types_compatible(left_type, right_type):
        return True, f"types compatible: {left_type} ↔ {right_type}"

    return False, (
        f"incompatible types: {cond_left_alias}.{cond_left_col} ({left_type}) "
        f"↔ {cond_right_alias}.{cond_right_col} ({right_type})"
    )


def _is_generic_counterfactual(fix: str) -> bool:
    """Return True if the counterfactual_fix is too vague to drive optimization."""
    if not fix:
        return True
    lower = fix.strip().lower()
    if any(lower.startswith(p) for p in GENERIC_FIX_PREFIXES):
        has_specific_ref = any(
            tok in lower
            for tok in (".", "_", "column", "table ", "tvf", "function ")
            if len(tok) > 1
        )
        if not has_specific_ref:
            return True
    return False


def _describe_fix(cluster: dict) -> str:
    """Describe the fix for a cluster, preferring specific ASI fixes."""
    asi_fixes = [f for f in cluster.get("asi_counterfactual_fixes", []) if f]
    specific_fixes = [f for f in asi_fixes if not _is_generic_counterfactual(f)]
    if specific_fixes:
        return specific_fixes[0]
    if cluster.get("root_cause") == "repeatability_issue":
        dominant_asset = cluster.get("asi_blame_set") or cluster.get(
            "dominant_asset", "TABLE"
        )
        base = REPEATABILITY_FIX_BY_ASSET.get(
            dominant_asset, REPEATABILITY_FIX_BY_ASSET["TABLE"]
        )
        return f"{base} (affects {len(cluster['question_ids'])} questions)"
    wrong_clause = cluster.get("asi_wrong_clause") or ""
    blame = cluster.get("asi_blame_set") or ""
    if wrong_clause and blame:
        return (
            f"Fix {cluster['root_cause']}: wrong clause '{wrong_clause}' "
            f"in {blame} affecting {len(cluster['question_ids'])} questions."
        )
    return (
        f"Fix {cluster['root_cause']} affecting "
        f"{len(cluster['question_ids'])} questions. "
        f"Judge: {cluster['affected_judge']}."
    )


def _deduplicate_clusters(clusters: list[dict]) -> list[dict]:
    """Merge clusters with identical (root_cause, question_ids) into one representative.

    Keeps the cluster with the highest confidence and tracks all merged judge
    names so attribution is not lost.
    """
    seen: dict[tuple, dict] = {}
    for c in clusters:
        key = (c.get("root_cause", ""), tuple(sorted(c.get("question_ids", []))))
        if key not in seen or c.get("confidence", 0) > seen[key].get("confidence", 0):
            merged = dict(c)
            merged.setdefault("merged_judges", [])
            seen[key] = merged
        seen[key]["merged_judges"].append(c.get("affected_judge", ""))
    return list(seen.values())


_INSTRUCTION_CONTENT_PATTERNS = re.compile(
    r"(?i)(ROUTING\s*RULES|MUST\s+follow|MUST\s+use|TVF\s+ROUTING|"
    r"METRIC\s+VIEW|MEASURE\s*\(|GROUP\s+BY|ORDER\s+BY|WHERE\s+.*=|"
    r"SELECT\s+\*\s+FROM|CRITICAL\s+ROUTING|enable_format_assistance|"
    r"enable_entity_matching)",
)


def _detect_instruction_content_in_description(
    metadata_snapshot: dict,
) -> list[dict]:
    """Check if the Genie Space description contains instruction-like content.

    Returns a list of ``update_description`` proposals that strip the
    instruction content, keeping only the user-facing summary paragraph.
    """
    config = metadata_snapshot.get("config") or {}
    desc = config.get("description") or ""
    if isinstance(desc, list):
        desc = "\n".join(desc)
    if not desc or not _INSTRUCTION_CONTENT_PATTERNS.search(desc):
        return []

    paragraphs = desc.split("\n\n")
    summary_parts: list[str] = []
    for para in paragraphs:
        if _INSTRUCTION_CONTENT_PATTERNS.search(para):
            break
        summary_parts.append(para.strip())
    clean_desc = "\n\n".join(summary_parts).strip()
    if not clean_desc or clean_desc == desc.strip():
        return []

    logger.info(
        "Description contains instruction-like content (%d chars). "
        "Proposing cleanup to %d chars.",
        len(desc),
        len(clean_desc),
    )
    return [
        {
            "patch_type": "update_description",
            "scope": "genie_config",
            "target": "genie_space",
            "proposed_value": clean_desc,
            "old_value": desc,
            "rationale": (
                "Description contained LLM-facing routing rules and SQL patterns. "
                "Stripped to user-facing summary only."
            ),
            "lever": 5,
            "net_impact": 1,
            "question_ids": [],
            "asi": {
                "failure_type": "missing_instruction",
                "severity": "minor",
                "counterfactual_fixes": [],
                "ambiguity_detected": False,
            },
        }
    ]


def _validate_lever5_proposals(
    proposals: list[dict], metadata_snapshot: dict
) -> list[dict]:
    """Filter out empty, generic, or over-length Lever 5 proposals."""
    from genie_space_optimizer.common.config import MAX_INSTRUCTION_TEXT_CHARS

    _tables = metadata_snapshot.get("tables") or []
    _funcs = metadata_snapshot.get("functions") or []
    _mvs = metadata_snapshot.get("metric_views") or []
    known_assets: set[str] = set()
    for t in _tables:
        name = (t.get("name") or t.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    for f in _funcs:
        name = (f.get("name") or f.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    for m in _mvs:
        name = (m.get("name") or m.get("identifier", "")).lower()
        known_assets.add(name)
        known_assets.add(name.rsplit(".", 1)[-1])
    known_assets.discard("")

    valid: list[dict] = []
    for p in proposals:
        ptype = p.get("patch_type", "")
        if ptype not in ("add_instruction", "add_example_sql"):
            valid.append(p)
            continue

        if ptype == "add_instruction":
            text = (p.get("proposed_value") or p.get("new_text") or "").strip()
            if not text:
                logger.info("Rejecting empty add_instruction proposal")
                continue
            if len(text) > MAX_INSTRUCTION_TEXT_CHARS:
                logger.warning(
                    "Rejecting add_instruction proposal exceeding %d chars (%d chars)",
                    MAX_INSTRUCTION_TEXT_CHARS,
                    len(text),
                )
                continue
            text_lower = text.lower()
            if known_assets and not any(a in text_lower for a in known_assets):
                logger.warning(
                    "Rejecting generic add_instruction (no known asset referenced): %.100s...",
                    text,
                )
                continue

        if ptype == "add_example_sql":
            eq = (p.get("example_question") or "").strip()
            es = (p.get("example_sql") or "").strip()
            if not eq or not es:
                logger.info("Rejecting empty add_example_sql proposal")
                continue

        valid.append(p)

    rejected = len(proposals) - len(valid)
    if rejected:
        logger.info(
            "Lever 5 proposal validation: %d rejected, %d kept", rejected, len(valid)
        )
    return valid


def _deduplicate_proposals(proposals: list[dict]) -> list[dict]:
    """Remove proposals with identical proposed_value, keeping highest net_impact."""
    seen: dict[tuple, int] = {}
    out: list[dict] = []
    for p in proposals:
        val = (p.get("proposed_value") or "").strip()
        ptype = p.get("patch_type", "")
        if ptype in ("add_example_sql", "add_instruction") and val:
            key = (ptype, val)
            if key in seen:
                existing_idx = seen[key]
                if p.get("net_impact", 0) > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            seen[key] = len(out)
        out.append(p)
    return out


def generate_metadata_proposals(
    clusters: list[dict],
    metadata_snapshot: dict,
    target_lever: int | None = None,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    failed_levers: set[int] | None = None,
) -> list[dict]:
    """Generate metadata change proposals from failure clusters.

    For each cluster, maps to a lever, calls the LLM to generate a concrete
    ``proposed_value``, resolves scope, and scores by net_impact.

    When *failed_levers* is provided and *target_lever* is 5, clusters whose
    natural lever is in *failed_levers* are also included so that lever 5
    (instructions / example SQL) can act as a catch-all.
    """
    failed_levers = failed_levers or set()
    _pre_dedup = len(clusters)
    clusters = _deduplicate_clusters(clusters)
    if len(clusters) < _pre_dedup:
        logger.info(
            "Cluster dedup: %d -> %d (merged %d duplicates)",
            _pre_dedup, len(clusters), _pre_dedup - len(clusters),
        )
    proposals: list[dict] = []
    if target_lever == 5:
        desc_proposals = _detect_instruction_content_in_description(metadata_snapshot)
        proposals.extend(desc_proposals)
    for cluster in clusters:
        natural_lever = _map_to_lever(
            cluster["root_cause"],
            asi_failure_type=cluster.get("asi_failure_type"),
            blame_set=cluster.get("asi_blame_set"),
            judge=cluster.get("affected_judge"),
        )
        lever = natural_lever
        if target_lever is not None and lever != target_lever:
            if target_lever == 5 and natural_lever in failed_levers:
                lever = 5
            else:
                continue

        failure_type = cluster.get("asi_failure_type") or cluster.get("root_cause", "other")
        patch_type = _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get(
                (failure_type, 1), "add_instruction"
            ),
        )

        llm_result = _call_llm_for_proposal(cluster, metadata_snapshot, patch_type, lever)

        extra_fields: dict = {}
        if lever == 5:
            patch_type, extra_fields = _resolve_lever5_llm_result(
                llm_result, patch_type
            )

        if lever == 4 and isinstance(llm_result.get("join_spec"), dict):
            valid, reason = validate_join_spec_types(
                llm_result["join_spec"], metadata_snapshot
            )
            if not valid:
                logger.info(
                    "Reactive join proposal rejected (type mismatch): %s", reason
                )
                continue
            extra_fields["join_spec"] = llm_result["join_spec"]

        scope = _resolve_scope(lever, apply_mode)
        q_fixed = len(cluster["question_ids"])
        confidence = cluster["confidence"]
        total_objects = max(len(metadata_snapshot.get("tables", [])), 1)
        blast_radius = 1
        net_impact = q_fixed * confidence - 0.1 * (blast_radius / total_objects)
        rationale = llm_result.get("rationale", "")

        asi_block = {
            "failure_type": failure_type,
            "blame_set": cluster.get("asi_blame_set") or [],
            "severity": "major",
            "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
            "ambiguity_detected": cluster.get("root_cause") == "repeatability_issue",
        }

        if lever in (1, 2) and isinstance(llm_result.get("changes"), list):
            for change in llm_result["changes"]:
                if not isinstance(change, dict):
                    continue
                tbl = change.get("table", "")
                col = change.get("column", "")
                desc = change.get("description")
                syns = change.get("synonyms")
                if not tbl or not col:
                    continue
                change_desc = f"Update {tbl}.{col}"
                if desc:
                    change_desc += f" description={desc}"
                if syns:
                    change_desc += f" synonyms={syns}"
                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": cluster["cluster_id"],
                    "lever": lever,
                    "scope": scope,
                    "patch_type": patch_type,
                    "change_description": change_desc,
                    "proposed_value": desc[0] if isinstance(desc, list) and desc else "",
                    "rationale": rationale,
                    "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                    "confidence": confidence,
                    "questions_fixed": q_fixed,
                    "questions_at_risk": 0,
                    "net_impact": net_impact,
                    "asi": asi_block,
                    "table": tbl,
                    "column": col,
                    "column_description": desc,
                    "column_synonyms": syns,
                    **extra_fields,
                })
        else:
            proposed_value = (
                llm_result.get("proposed_value")
                or llm_result.get("instruction_text")
                or llm_result.get("example_question")
                or ""
            )

            proposal = {
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": cluster["cluster_id"],
                "lever": lever,
                "scope": scope,
                "patch_type": patch_type,
                "change_description": proposed_value or _describe_fix(cluster),
                "proposed_value": proposed_value,
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[5]),
                "confidence": confidence,
                "questions_fixed": q_fixed,
                "questions_at_risk": 0,
                "net_impact": net_impact,
                "asi": asi_block,
                **extra_fields,
            }
            proposals.append(proposal)

    if target_lever == 4:
        discovery_hints = discover_join_candidates(metadata_snapshot)
        if discovery_hints:
            discovery_specs = _call_llm_for_join_discovery(
                metadata_snapshot, discovery_hints, w=w,
            )
            for spec_result in discovery_specs:
                join_spec = spec_result.get("join_spec")
                if not isinstance(join_spec, dict):
                    continue

                valid, reason = validate_join_spec_types(join_spec, metadata_snapshot)
                if not valid:
                    logger.info("Discovery join rejected (type mismatch): %s", reason)
                    continue

                left_obj = join_spec.get("left", {})
                right_obj = join_spec.get("right", {})
                left_id = left_obj.get("identifier", "") if isinstance(left_obj, dict) else ""
                right_id = right_obj.get("identifier", "") if isinstance(right_obj, dict) else ""
                sql_parts = join_spec.get("sql", [])
                condition = sql_parts[0] if sql_parts else ""

                proposals.append({
                    "proposal_id": f"P{len(proposals) + 1:03d}",
                    "cluster_id": f"JOIN_DISC_{len(proposals) + 1:03d}",
                    "lever": 4,
                    "scope": "genie_config",
                    "patch_type": "add_join_spec",
                    "change_description": f"Add join: {condition}" if condition else f"Add join: {left_id} ↔ {right_id}",
                    "proposed_value": "",
                    "rationale": spec_result.get("rationale", "LLM-assisted join discovery"),
                    "join_spec": join_spec,
                    "dual_persistence": DUAL_PERSIST_PATHS.get(4, DUAL_PERSIST_PATHS[5]),
                    "confidence": 0.7,
                    "questions_fixed": 0,
                    "questions_at_risk": 0,
                    "net_impact": 0.35,
                    "asi": {
                        "failure_type": "missing_join_spec",
                        "blame_set": [],
                        "severity": "minor",
                        "counterfactual_fixes": [],
                        "ambiguity_detected": False,
                    },
                })

    proposals = _validate_lever5_proposals(proposals, metadata_snapshot)
    _pre_dedup_p = len(proposals)
    proposals = _deduplicate_proposals(proposals)
    if len(proposals) < _pre_dedup_p:
        logger.info(
            "Proposal dedup: %d -> %d (removed %d duplicates)",
            _pre_dedup_p, len(proposals), _pre_dedup_p - len(proposals),
        )
    proposals.sort(key=lambda p: p["net_impact"], reverse=True)
    return proposals


def propose_patch_set_from_asi(
    asi_rows: list[dict],
    metadata_snapshot: dict,
    lever: int | None = None,
) -> list[dict]:
    """Generate proposals directly from ASI records.

    Wraps ASI rows into cluster-like structures and delegates to
    ``generate_metadata_proposals``.
    """
    blame_groups: dict[tuple, list] = defaultdict(list)
    for row in asi_rows:
        if not isinstance(row, dict):
            continue
        ft = row.get("failure_type", row.get("asi_failure_type", "other"))
        bs = row.get("blame_set", [])
        if isinstance(bs, list):
            key = (ft, tuple(sorted(bs)))
        else:
            key = (ft, (str(bs),))
        blame_groups[key].append(row)

    clusters: list[dict] = []
    for (ft, bs_tuple), rows in blame_groups.items():
        if len(rows) < 1:
            continue
        clusters.append(
            {
                "cluster_id": f"ASI_C{len(clusters) + 1:03d}",
                "root_cause": ft,
                "question_ids": [
                    r.get("question_id", f"q{i}") for i, r in enumerate(rows)
                ],
                "affected_judge": rows[0].get("judge", "unknown"),
                "confidence": sum(
                    float(r.get("confidence", 0.5)) for r in rows
                )
                / max(len(rows), 1),
                "asi_failure_type": ft,
                "asi_blame_set": list(bs_tuple) if bs_tuple else None,
                "asi_counterfactual_fixes": [
                    r.get("counterfactual_fix", "")
                    for r in rows
                    if r.get("counterfactual_fix")
                ],
            }
        )

    return generate_metadata_proposals(
        clusters, metadata_snapshot, target_lever=lever
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Scoring (pure)
# ═══════════════════════════════════════════════════════════════════════


def score_patch_set(proposals: list[dict], metadata_snapshot: dict) -> float:
    """Score a patch set by expected impact.

    ``questions_blamed * avg_confidence - 0.1 * (blast_objects / total_objects)``
    """
    if not proposals:
        return 0.0

    total_objects = max(
        len(metadata_snapshot.get("tables", [])) if metadata_snapshot else 1, 1
    )

    all_targets: set[str] = set()
    questions_total = 0
    confidences: list[float] = []

    for p in proposals:
        target = p.get("target_object") or p.get("object_id") or p.get("target", "")
        if target:
            all_targets.add(target)
        questions_total += p.get("questions_fixed", 0)
        confidences.append(float(p.get("confidence", 0.5)))

    avg_confidence = sum(confidences) / max(len(confidences), 1)
    blast = len(all_targets)
    return questions_total * avg_confidence - 0.1 * (blast / total_objects)


# ═══════════════════════════════════════════════════════════════════════
# 6. Conflict Detection & Batching (pure)
# ═══════════════════════════════════════════════════════════════════════


def detect_conflicts_and_batch(proposals: list[dict]) -> list[list[dict]]:
    """Group proposals into conflict-free batches.

    Within each lever group, checks ``CONFLICT_RULES``.  Starts a new batch
    when a conflict is found.
    """
    conflict_set = set()
    for a, b in CONFLICT_RULES:
        conflict_set.add((a, b))
        conflict_set.add((b, a))

    batches: list[list[dict]] = []
    current_batch: list[dict] = []
    current_types: set[str] = set()

    for p in proposals:
        p_type = p.get("patch_type") or p.get("type", "")
        has_conflict = any((p_type, existing) in conflict_set for existing in current_types)
        if has_conflict:
            batches.append(current_batch)
            current_batch = [p]
            current_types = {p_type}
        else:
            current_batch.append(p)
            current_types.add(p_type)

    if current_batch:
        batches.append(current_batch)
    return batches


# ═══════════════════════════════════════════════════════════════════════
# 7. Regression Detection (pure)
# ═══════════════════════════════════════════════════════════════════════


def detect_regressions(
    current_scores: dict[str, float],
    previous_scores: dict[str, float],
    threshold: float = REGRESSION_THRESHOLD,
) -> list[dict]:
    """Detect if any metric dropped more than ``threshold`` percentage points."""
    regressions: list[dict] = []
    for key in previous_scores:
        prev_val = previous_scores.get(key, 0)
        curr_val = current_scores.get(key, 0)
        if curr_val < prev_val - threshold:
            regressions.append(
                {
                    "judge": key,
                    "previous": prev_val,
                    "current": curr_val,
                    "drop": prev_val - curr_val,
                }
            )
    return regressions


# ═══════════════════════════════════════════════════════════════════════
# 8. Validation (pure)
# ═══════════════════════════════════════════════════════════════════════


def validate_patch_set(
    patches: list[dict], metadata_snapshot: dict | None = None
) -> tuple[bool, list[str]]:
    """Validate a patch set before application.

    Checks: known types, conflict rules, blast radius (max 5 objects),
    and optionally validates targets exist in metadata_snapshot.
    """
    errors: list[str] = []
    valid_types = set(PATCH_TYPES.keys())

    for i, p in enumerate(patches):
        pt = p.get("type") if isinstance(p, dict) else None
        if pt and pt not in valid_types:
            errors.append(f"Patch {i}: unknown type '{pt}'")

    targets: set[str] = set()
    for p in patches:
        if not isinstance(p, dict):
            continue
        obj = p.get("target_object") or p.get("object_id") or p.get("target") or p.get("table")
        if obj:
            targets.add(obj)

    if len(targets) > MAX_PATCH_OBJECTS:
        errors.append(
            f"Too many target objects: {len(targets)} (max {MAX_PATCH_OBJECTS})"
        )

    types_in_set = {p.get("type") for p in patches if isinstance(p, dict)}
    for a, b in CONFLICT_RULES:
        if a in types_in_set and b in types_in_set:
            errors.append(f"Conflicting patch types: {a} and {b}")

    if metadata_snapshot:
        known_tables = {
            t.get("name") or t.get("identifier", "")
            for t in metadata_snapshot.get("tables", [])
        }
        known_columns: set[str] = set()
        for t in metadata_snapshot.get("tables", []):
            for c in t.get("columns", t.get("column_configs", [])):
                known_columns.add(c.get("name") or c.get("column_name", ""))

        for i, p in enumerate(patches):
            if not isinstance(p, dict):
                continue
            tgt_table = p.get("table", "")
            tgt_col = p.get("column", "")
            if tgt_table and known_tables and tgt_table not in known_tables:
                errors.append(f"Patch {i}: table '{tgt_table}' not found in metadata")
            if tgt_col and known_columns and tgt_col not in known_columns:
                errors.append(f"Patch {i}: column '{tgt_col}' not found in metadata")

    return (len(errors) == 0, errors)
