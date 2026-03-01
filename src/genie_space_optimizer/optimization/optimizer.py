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
import os
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
    LEVER_5_HOLISTIC_PROMPT,
    LEVER_5_INSTRUCTION_PROMPT,
    LEVER_NAMES,
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_TEMPERATURE,
    LOW_RISK_PATCHES,
    MAX_HOLISTIC_INSTRUCTION_CHARS,
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


def _row_qid(row: dict, *, fallback: str = "unknown") -> str:
    """Extract question_id from an eval-results row regardless of column layout.

    MLflow stores inputs in the ``request`` column (not ``inputs/…``), so we
    parse both layouts to robustly recover the QID.
    """
    direct = (
        row.get("inputs/question_id")
        or row.get("inputs/question")
        or row.get("question_id")
    )
    if direct:
        return str(direct)
    _req = row.get("request") or {}
    if isinstance(_req, str):
        try:
            _req = json.loads(_req)
        except (json.JSONDecodeError, TypeError):
            _req = {}
    if isinstance(_req, dict):
        _kw = _req.get("kwargs", {})
        qid = _kw.get("question_id") or _req.get("question_id") or _req.get("question")
        if qid:
            return str(qid)
    return row.get("question", fallback) or fallback


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
    ft = (asi_failure_type if asi_failure_type and asi_failure_type != "other" else None) or root_cause

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
        "missing_scd_filter": 2,
        "wrong_filter_condition": 2,
        "missing_temporal_filter": 2,
        "wrong_join_type": 5,
        "tvf_parameter_error": 3,
        "wrong_join": 4,
        "missing_join_spec": 4,
        "wrong_join_spec": 4,
        "asset_routing_error": 5,
        "missing_instruction": 5,
        "ambiguous_question": 5,
        "format_difference": 0,
        "extra_columns_only": 0,
        "select_star": 0,
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
    if "is_current" in r or ("scd" in r and ("filter" in r or "dimension" in r)):
        return "missing_scd_filter"
    if ("left join" in r or "inner join" in r) and ("join type" in r or "instead of" in r or "wrong join" in r):
        return "wrong_join_type"
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
    if "limit" in r and ("missing" in r or "without" in r):
        return "wrong_filter_condition"
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
_SQL_AGG = re.compile(r"\b(SUM|AVG|COUNT|MIN|MAX|STDDEV|VARIANCE)\s*\(", re.I)
_SQL_SELECT_STAR = re.compile(r"\bSELECT\s+\*\b", re.I)
_SQL_SCD_FILTER = re.compile(r"\b(is_current|is_active)\s*=\s*(true|1|'true')\b", re.I)
_SQL_JOIN_TYPE = re.compile(r"\b(LEFT|RIGHT|INNER|CROSS|FULL)\s+(OUTER\s+)?JOIN\b", re.I)
_SQL_WHERE_CONDITIONS = re.compile(r"\bWHERE\b\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|\bHAVING\b|;|\Z)", re.I | re.S)


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


_SQL_JOIN_ON = re.compile(
    r"\bJOIN\s+([\w.`]+)\s+(?:AS\s+)?(\w+)?\s*ON\s+(.+?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|;|\Z)",
    re.I | re.S,
)


def _extract_join_pairs(sql: str) -> set[tuple[str, str]]:
    """Extract (table, join_column) pairs from JOIN...ON clauses.

    Parses each ``JOIN <table> ... ON <condition>`` and extracts every
    ``alias.column`` reference from the ON clause, pairing the joined
    table with the column names used in the condition.
    """
    pairs: set[tuple[str, str]] = set()
    for m in _SQL_JOIN_ON.finditer(sql or ""):
        table = m.group(1).strip("`").lower()
        on_clause = m.group(3)
        cols = re.findall(r"[\w`]+\.`?([\w]+)`?", on_clause)
        for col in cols:
            pairs.add((table, col.lower()))
    return pairs


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
    exp_join_pairs = _extract_join_pairs(expected_sql)
    gen_join_pairs = _extract_join_pairs(generated_sql)
    exp_has_join = bool(re.search(r"\bJOIN\b", exp_lower))

    _DIM_PREFIXES = ("dim_", "lookup_", "ref_")
    missing_tables = exp_tables - gen_tables if exp_tables and gen_tables else set()

    # 1. Missing dimension JOIN — GT joins a dim/lookup/ref table that
    #    Genie omits entirely. This is fundamentally a join issue even if
    #    aggregation or filter differences also exist.
    if missing_tables:
        dim_tables = {t for t in missing_tables if any(t.startswith(p) or f".{p}" in t for p in _DIM_PREFIXES)}
        if dim_tables:
            return "wrong_join"

    # 2. Aggregation checks
    exp_aggs = set(_SQL_AGG.findall(exp_lower))
    gen_aggs = set(_SQL_AGG.findall(gen_lower))
    if exp_aggs and not gen_aggs:
        return "missing_aggregation"
    if exp_aggs != gen_aggs and exp_aggs and gen_aggs:
        return "wrong_aggregation"

    # 3. Filter checks
    exp_has_where = bool(_SQL_WHERE.search(exp_lower))
    gen_has_where = bool(_SQL_WHERE.search(gen_lower))
    if exp_has_where and not gen_has_where:
        return "missing_filter"

    # 3b. SCD filter — expected has is_current/is_active = true, generated omits it
    exp_has_scd = bool(_SQL_SCD_FILTER.search(exp_lower))
    gen_has_scd = bool(_SQL_SCD_FILTER.search(gen_lower))
    if exp_has_scd and not gen_has_scd:
        return "missing_scd_filter"

    # 3c. WHERE condition diff — both have WHERE but conditions differ
    if exp_has_where and gen_has_where:
        exp_conds = _SQL_WHERE_CONDITIONS.search(exp_lower)
        gen_conds = _SQL_WHERE_CONDITIONS.search(gen_lower)
        if exp_conds and gen_conds:
            exp_text = re.sub(r"\s+", " ", exp_conds.group(1).strip())
            gen_text = re.sub(r"\s+", " ", gen_conds.group(1).strip())
            if exp_text != gen_text:
                return "wrong_filter_condition"

    # 3d. Join type diff — same tables but different join types (LEFT vs INNER)
    exp_join_types = sorted(_SQL_JOIN_TYPE.findall(exp_lower))
    gen_join_types = sorted(_SQL_JOIN_TYPE.findall(gen_lower))
    if exp_join_types and gen_join_types and exp_join_types != gen_join_types:
        return "wrong_join_type"

    # 4. Wrong join column — both queries join the same tables but on
    #    different columns (e.g. destination_name vs destination_key).
    if exp_join_pairs and gen_join_pairs and exp_join_pairs != gen_join_pairs:
        return "wrong_join_spec"

    # 5. Missing join — GT has a JOIN that Genie doesn't (regardless of
    #    whether Genie has other JOINs).
    if exp_has_join and missing_tables:
        return "wrong_join"

    # 6. SELECT *, TVF, MEASURE, GROUP BY checks
    gen_has_star = bool(_SQL_SELECT_STAR.search(gen_lower))
    exp_has_star = bool(_SQL_SELECT_STAR.search(exp_lower))
    if gen_has_star and not exp_has_star:
        return "select_star"

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

    # 7. Table-set differs (non-dimension tables)
    if exp_tables and gen_tables and exp_tables != gen_tables:
        extra_tables = gen_tables - exp_tables
        date_dims = {t for t in missing_tables if "dim_date" in t or "dim_time" in t}
        if missing_tables == date_dims and not extra_tables:
            return "format_difference"
        if missing_tables or extra_tables:
            return "wrong_table"

    # 8. Same tables, different columns
    if exp_tables == gen_tables and exp_tables:
        exp_select = re.findall(r"\bSELECT\b(.+?)\bFROM\b", exp_lower, re.S)
        gen_select = re.findall(r"\bSELECT\b(.+?)\bFROM\b", gen_lower, re.S)
        if exp_select and gen_select:
            exp_cols = [c.strip() for c in exp_select[0].split(",")]
            gen_cols = [c.strip() for c in gen_select[0].split(",")]
            if len(exp_cols) > len(gen_cols) and len(gen_cols) >= 1:
                return "extra_columns_only"
        return "wrong_column"

    return "missing_instruction"


def cluster_failures(
    eval_results: dict,
    metadata_snapshot: dict,
    *,
    spark: Any = None,
    run_id: str = "",
    catalog: str = "",
    schema: str = "",
) -> list[dict]:
    """Group evaluation failures into actionable clusters.

    Groups by ``(judge, asi_failure_type, blame_set_str)``.  Falls back to
    ``(judge, _extract_pattern(rationale), "")`` when ASI is absent.
    Returns clusters with >= 1 question so even single failures are actionable.

    When ``spark/run_id/catalog/schema`` are provided, enriches with stored
    ASI data from ``genie_eval_asi_results`` Delta table.
    """
    uc_asi_map: dict[tuple[str, str], dict] = {}
    if spark and run_id and catalog and schema:
        try:
            uc_asi = read_asi_from_uc(spark, run_id, catalog, schema)
            for a in uc_asi:
                key = (a.get("question_id", ""), a.get("judge", ""))
                uc_asi_map[key] = a
            if uc_asi_map:
                logger.info("Enriched clustering with %d UC ASI records", len(uc_asi_map))
        except Exception:
            logger.debug("UC ASI enrichment failed", exc_info=True)

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

        question_id = _row_qid(row)

        sql_ctx = {
            "question": _req.get("question", "") if isinstance(_req, dict) else "",
            "expected_sql": _req.get("expected_sql", "") if isinstance(_req, dict) else "",
            "generated_sql": _resp.get("response", "") if isinstance(_resp, dict) else "",
            "comparison": _resp.get("comparison", {}) if isinstance(_resp, dict) else {},
        }

        _NON_JUDGE_SUFFIXES = ("/rationale", "/source", "/metadata", "/error")
        for col_name, val in list(row.items()):
            judge: str | None = None
            if col_name.startswith("feedback/") and col_name.endswith("/value"):
                judge = col_name.removeprefix("feedback/").removesuffix("/value")
            elif col_name.startswith("feedback/") and not any(col_name.endswith(s) for s in _NON_JUDGE_SUFFIXES):
                bare = col_name.removeprefix("feedback/")
                if "/" not in bare:
                    judge = bare
            elif col_name.endswith("/value") and not col_name.startswith("feedback/"):
                judge = col_name.removesuffix("/value")
            if judge and "no" in str(val).lower():
                from genie_space_optimizer.optimization.evaluation import (
                    _parse_asi_from_rationale,
                )
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

                if not asi_failure_type and uc_asi_map:
                    uc_asi_entry = uc_asi_map.get((question_id, judge))
                    if uc_asi_entry:
                        asi_failure_type = asi_failure_type or uc_asi_entry.get("failure_type")
                        asi_blame_set = asi_blame_set or uc_asi_entry.get("blame_set")
                        asi_counterfactual = asi_counterfactual or uc_asi_entry.get("counterfactual_fix")
                        asi_wrong_clause = asi_wrong_clause or uc_asi_entry.get("wrong_clause")

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

    question_profiles: dict[str, dict] = {}
    for f in failures:
        qid = f["question_id"]
        if qid not in question_profiles:
            question_profiles[qid] = {
                "judges": set(),
                "root_causes": [],
                "blame_sets": [],
                "counterfactual_fixes": [],
                "wrong_clauses": [],
                "sql_context": f.get("sql_context", {}),
                "failures": [],
            }
        profile = question_profiles[qid]
        profile["judges"].add(f["judge"])
        profile["failures"].append(f)

        asi_ft = f.get("asi_failure_type")
        if asi_ft and asi_ft != "other":
            root = asi_ft
            resolution_method = "asi_metadata"
        else:
            pattern = _extract_pattern(f["rationale"])
            if pattern != "other":
                root = pattern
                resolution_method = "rationale_pattern"
            else:
                root = _classify_sql_diff(f.get("sql_context", {}))
                resolution_method = "sql_diff"
        f["_resolved_root_cause"] = root
        f["_resolution_method"] = resolution_method
        profile["root_causes"].append(root)

        if f.get("asi_blame_set"):
            blame_str = str(f["asi_blame_set"])
            if blame_str not in profile["blame_sets"]:
                profile["blame_sets"].append(blame_str)
        if f.get("asi_counterfactual_fix"):
            profile["counterfactual_fixes"].append(f["asi_counterfactual_fix"])
        if f.get("asi_wrong_clause"):
            profile["wrong_clauses"].append(f["asi_wrong_clause"])

    for qid, profile in question_profiles.items():
        cause_counts = Counter(profile["root_causes"])
        profile["dominant_root_cause"] = cause_counts.most_common(1)[0][0] if cause_counts else "other"

    # ── 8a. Per-Question ASI Extraction Trace ───────────────────────────
    _cluster_debug = os.environ.get("CLUSTER_DEBUG", "1").lower() not in ("0", "false", "no")
    if _cluster_debug and question_profiles:
        lines = ["\n== ASI EXTRACTION TRACE ======================================================"]
        for qid, profile in question_profiles.items():
            lines.append(f"\n--- Q: {qid} " + "-" * max(1, 60 - len(qid)))
            for f in profile["failures"]:
                judge = f["judge"]
                verdict = "FAIL"
                asi_ft = f.get("asi_failure_type")
                blame = f.get("asi_blame_set")
                cfix = f.get("asi_counterfactual_fix")
                wclause = f.get("asi_wrong_clause")
                resolved = f.get("_resolved_root_cause", "other")
                method = f.get("_resolution_method", "unknown")
                lines.append(f"|  Judge: {judge:<24s}|  Verdict: {verdict}")
                has_asi = bool(asi_ft)
                lines.append(f"|    ASI metadata found:      {'YES' if has_asi else 'NO'}")
                if has_asi:
                    lines.append(f"|      failure_type (raw):    {asi_ft}")
                    if blame:
                        lines.append(f"|      blame_set:             {blame}")
                    if cfix:
                        lines.append(f"|      counterfactual_fix:    \"{str(cfix)[:120]}\"")
                    if wclause:
                        lines.append(f"|      wrong_clause:          {wclause}")
                lines.append(f"|    Final root cause:        {resolved}  (via {method})")
            lines.append(f"|  Dominant root cause:       {profile['dominant_root_cause']}")
            blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else "(none)"
            lines.append(f"|  Cluster group key:         ({profile['dominant_root_cause']}, \"{blame_key}\")")
        lines.append("-" * 78)
        print("\n".join(lines))

    cluster_groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for qid, profile in question_profiles.items():
        blame_key = "|".join(sorted(profile["blame_sets"])) if profile["blame_sets"] else ""
        group_key = (profile["dominant_root_cause"], blame_key)
        cluster_groups[group_key].append(qid)

    clusters: list[dict] = []
    for (root_cause, blame_str), qids in cluster_groups.items():
        all_judges: set[str] = set()
        all_counterfactuals: list[str] = []
        all_wrong_clauses: list[str] = []
        sql_contexts: list[dict] = []
        sample_asi_type: str | None = None

        question_traces: list[dict] = []
        for qid in qids:
            profile = question_profiles[qid]
            all_judges.update(profile["judges"])
            all_counterfactuals.extend(profile["counterfactual_fixes"])
            all_wrong_clauses.extend(profile["wrong_clauses"])
            if profile["sql_context"]:
                sql_contexts.append(profile["sql_context"])
            if not sample_asi_type:
                for f in profile["failures"]:
                    if f.get("asi_failure_type"):
                        sample_asi_type = f["asi_failure_type"]
                        break
            q_text = profile["sql_context"].get("question", "") if profile["sql_context"] else ""
            judge_traces = []
            for f in profile["failures"]:
                judge_traces.append({
                    "judge": f["judge"],
                    "verdict": "FAIL",
                    "asi_failure_type_raw": f.get("asi_failure_type"),
                    "resolved_root_cause": f.get("_resolved_root_cause", "other"),
                    "resolution_method": f.get("_resolution_method", "unknown"),
                    "blame_set": f.get("asi_blame_set"),
                    "counterfactual_fix": f.get("asi_counterfactual_fix"),
                    "wrong_clause": f.get("asi_wrong_clause"),
                    "rationale_snippet": (f.get("rationale") or "")[:500],
                })
            question_traces.append({
                "question_id": qid,
                "question_text": q_text[:200],
                "failed_judges": judge_traces,
            })

        unique_qids = sorted(set(qids))
        entry = {
            "cluster_id": f"C{len(clusters) + 1:03d}",
            "root_cause": root_cause,
            "question_ids": unique_qids,
            "affected_judges": sorted(all_judges),
            "affected_judge": sorted(all_judges)[0] if all_judges else "unknown",
            "confidence": min(0.9, 0.5 + 0.1 * len(unique_qids)),
            "asi_failure_type": sample_asi_type,
            "asi_blame_set": blame_str or None,
            "asi_wrong_clause": next((wc for wc in all_wrong_clauses if wc), None),
            "asi_counterfactual_fixes": list(dict.fromkeys(cf for cf in all_counterfactuals if cf)),
            "sql_contexts": sql_contexts[:5],
            "question_traces": question_traces,
        }
        clusters.append(entry)

    clusters.sort(key=lambda c: len(c["question_ids"]), reverse=True)

    # ── 8b. Cluster Formation Summary ────────────────────────────────────
    if _cluster_debug and clusters:
        total_failures = sum(len(p["failures"]) for p in question_profiles.values())
        total_judges = len({f["judge"] for p in question_profiles.values() for f in p["failures"]})
        lines = ["\n== CLUSTER FORMATION ========================================================="]
        lines.append(f"|  Total failure entries:     {total_failures} (across {len(question_profiles)} questions, {total_judges} judges)")
        lines.append(f"|  Question profiles:         {len(question_profiles)}")
        lines.append(f"|  Cluster groups formed:     {len(clusters)}")
        for c in clusters:
            cid = c["cluster_id"]
            rc = c["root_cause"]
            blame = c.get("asi_blame_set") or "(none)"
            qids = c["question_ids"]
            lines.append(f"|    {cid} ({rc}, blame=\"{blame}\"): {len(qids)} question(s) {qids}")
        lines.append("=" * 78)
        print("\n".join(lines))

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
                            "question_id": _row_qid(row, fallback=f"q{i}"),
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


def _format_eval_summary(clusters: list[dict]) -> str:
    """Produce a compact summary of all evaluation clusters for the holistic prompt."""
    if not clusters:
        return "No failure clusters from evaluation (all questions passed)."

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    soft = [c for c in clusters if c.get("signal_type") == "soft"]
    total_questions = sum(len(c.get("question_ids", [])) for c in clusters)
    root_causes: dict[str, int] = {}
    judges: dict[str, int] = {}
    for c in clusters:
        rc = c.get("root_cause", "unknown")
        root_causes[rc] = root_causes.get(rc, 0) + len(c.get("question_ids", []))
        judge = c.get("affected_judge", "unknown")
        judges[judge] = judges.get(judge, 0) + len(c.get("question_ids", []))

    lines = [
        f"Total clusters: {len(clusters)} (hard failures: {len(hard)}, soft signals: {len(soft)})",
        f"Total affected questions: {total_questions}",
        "",
        "Failures by root cause:",
    ]
    for rc, count in sorted(root_causes.items(), key=lambda x: -x[1]):
        lines.append(f"  - {rc}: {count} questions")
    lines.append("")
    lines.append("Failures by judge:")
    for judge, count in sorted(judges.items(), key=lambda x: -x[1]):
        lines.append(f"  - {judge}: {count} questions")
    return "\n".join(lines)


def _format_lever_summary(lever_changes: list[dict] | None) -> str:
    """Format what levers 1-4 did for the holistic lever 5 prompt."""
    if not lever_changes:
        return "(No changes applied by earlier levers in this iteration.)"

    lines: list[str] = []
    for lc in lever_changes:
        lever_name = lc.get("lever_name", f"Lever {lc.get('lever', '?')}")
        delta = lc.get("accuracy_delta", 0)
        patches = lc.get("patches", [])
        delta_str = f"+{delta:.1f}%" if delta >= 0 else f"{delta:.1f}%"
        lines.append(f"### {lever_name} (accuracy change: {delta_str})")
        for p in patches[:10]:
            change = p.get("change", "")
            ptype = p.get("patch_type", "")
            lines.append(f"  - [{ptype}] {change}")
        if len(patches) > 10:
            lines.append(f"  ... and {len(patches) - 10} more patches")
        lines.append("")
    return "\n".join(lines) if lines else "(No changes applied.)"


def _format_cluster_briefs(clusters: list[dict], top_n: int = 5) -> str:
    """Format cluster data for the holistic prompt.

    Hard-failure clusters (top N) get full SQL diffs; remaining get one-line
    summaries.  Soft-signal clusters (correct-but-suboptimal) are rendered
    under a separate header so the LLM can weight them appropriately.
    """
    if not clusters:
        return "(No failure clusters.)"

    hard = [c for c in clusters if c.get("signal_type") != "soft"]
    soft = [c for c in clusters if c.get("signal_type") == "soft"]

    sorted_hard = sorted(hard, key=lambda c: len(c.get("question_ids", [])), reverse=True)
    lines: list[str] = []

    for idx, cluster in enumerate(sorted_hard[:top_n], 1):
        rc = cluster.get("root_cause", "unknown")
        q_ids = cluster.get("question_ids", [])
        judge = cluster.get("affected_judge", "unknown")
        blame = cluster.get("asi_blame_set") or []
        lines.append(f"### Cluster {idx}: {rc} ({len(q_ids)} questions, judge: {judge})")
        if blame:
            lines.append(f"Blamed objects: {', '.join(str(b) for b in blame[:5])}")
        lines.append(_format_sql_diffs(cluster))
        lines.append("")

    remaining_hard = sorted_hard[top_n:]
    if remaining_hard:
        lines.append(f"### Additional hard-failure clusters ({len(remaining_hard)} more):")
        for cluster in remaining_hard:
            rc = cluster.get("root_cause", "unknown")
            q_ids = cluster.get("question_ids", [])
            judge = cluster.get("affected_judge", "unknown")
            blame = cluster.get("asi_blame_set") or []
            blame_str = f" blamed=[{', '.join(str(b) for b in blame[:3])}]" if blame else ""
            lines.append(
                f"  - {rc}: {len(q_ids)} questions (judge: {judge}){blame_str}"
            )

    if soft:
        sorted_soft = sorted(soft, key=lambda c: len(c.get("question_ids", [])), reverse=True)
        lines.append("")
        lines.append("### Correct-but-Suboptimal Patterns (arbiter: correct, individual judges: failed)")
        lines.append("These queries returned correct results but used suboptimal approaches.")
        lines.append("Use these to inform best-practice guidance, NOT to fix failures.")
        lines.append("")
        for idx, cluster in enumerate(sorted_soft[:top_n], 1):
            rc = cluster.get("root_cause", "unknown")
            q_ids = cluster.get("question_ids", [])
            judge = cluster.get("affected_judge", "unknown")
            blame = cluster.get("asi_blame_set") or []
            lines.append(f"#### Soft {idx}: {rc} ({len(q_ids)} questions, judge: {judge})")
            if blame:
                lines.append(f"Blamed objects: {', '.join(str(b) for b in blame[:5])}")
            lines.append(_format_sql_diffs(cluster))
            lines.append("")
        remaining_soft = sorted_soft[top_n:]
        if remaining_soft:
            lines.append(f"#### Additional soft-signal clusters ({len(remaining_soft)} more):")
            for cluster in remaining_soft:
                rc = cluster.get("root_cause", "unknown")
                q_ids = cluster.get("question_ids", [])
                judge = cluster.get("affected_judge", "unknown")
                blame = cluster.get("asi_blame_set") or []
                blame_str = f" blamed=[{', '.join(str(b) for b in blame[:3])}]" if blame else ""
                lines.append(
                    f"  - {rc}: {len(q_ids)} questions (judge: {judge}){blame_str}"
                )

    return "\n".join(lines)


_SQL_PATTERN_ROOT_CAUSES = frozenset({
    "wrong_table", "wrong_join", "missing_filter", "missing_aggregation",
    "wrong_aggregation", "wrong_measure", "select_star", "tvf_parameter_error",
})


def _resolve_lever5_llm_result(
    llm_result: dict, original_patch_type: str, cluster: dict | None = None,
) -> tuple[str, dict]:
    """Interpret the instruction_type returned by the Lever 6 LLM and resolve
    the actual patch_type and extra fields to merge into the proposal.

    When the cluster's root_cause is a clear SQL-pattern issue (e.g.
    ``wrong_join``, ``missing_filter``) and the cluster has SQL context with
    a valid expected SQL, force ``add_example_sql`` to provide a concrete
    pattern rather than a verbose text instruction.

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

    if cluster and cluster.get("root_cause") in _SQL_PATTERN_ROOT_CAUSES:
        sql_ctxs = cluster.get("sql_contexts", [])
        representative = next(
            (sc for sc in sql_ctxs if sc.get("expected_sql") and sc.get("question")),
            None,
        )
        if representative:
            logger.info(
                "Forcing add_example_sql for SQL-pattern root cause '%s' "
                "(LLM returned text_instruction)",
                cluster["root_cause"],
            )
            return "add_example_sql", {
                "example_question": representative["question"],
                "example_sql": representative["expected_sql"],
                "parameters": [],
                "usage_guidance": llm_result.get("rationale", ""),
                "forced_from_sql_pattern": True,
            }

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

    _cid = cluster.get("cluster_id", "?")
    _root = cluster.get("root_cause", "?")
    _q_traces = cluster.get("question_traces", [])
    _ctxts = cluster.get("sql_contexts", [])

    _judge_lines = []
    for qt in _q_traces[:5]:
        for jt in qt.get("failed_judges", []):
            snip = (jt.get("rationale_snippet") or "")[:120].replace("\n", " ")
            _judge_lines.append(f"│   {qt['question_id'][:40]} / {jt['judge']}: \"{snip}\"")

    _sql_diff_lines = []
    if _ctxts:
        ctx = _ctxts[0]
        exp_snip = (ctx.get("expected_sql") or "")[:200].replace("\n", " ")
        gen_snip = (ctx.get("generated_sql") or "")[:200].replace("\n", " ")
        if exp_snip or gen_snip:
            _sql_diff_lines.append(f"│   Expected:  {exp_snip}")
            _sql_diff_lines.append(f"│   Generated: {gen_snip}")

    _cfix_lines = []
    for cf in cluster.get("asi_counterfactual_fixes", [])[:3]:
        _cfix_lines.append(f"│   \"{str(cf)[:120]}\"")

    _extra = (
        f"│ {'Cluster:':<24s} {_cid}\n"
        f"│ {'Root cause:':<24s} {_root}\n"
    )
    if _judge_lines:
        _extra += "│\n│ --- Judge Feedback Driving This Patch ---\n" + "\n".join(_judge_lines) + "\n"
    if _sql_diff_lines:
        _extra += "│\n│ --- SQL Diff (sample) ---\n" + "\n".join(_sql_diff_lines) + "\n"
    if _cfix_lines:
        _extra += "│\n│ --- Counterfactual Fixes ---\n" + "\n".join(_cfix_lines) + "\n"

    print(
        f"\n{_hdr}\n"
        f"│ {'Patch type:':<24s} {patch_type}\n"
        f"│ {'Failure type:':<24s} {format_kwargs.get('failure_type', '?')}\n"
        f"│ {'Blame set:':<24s} {format_kwargs.get('blame_set', '?')}\n"
        f"│ {'Questions:':<24s} {len(format_kwargs.get('affected_questions', []))}\n"
        f"{_extra}"
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


def _call_llm_for_holistic_instructions(
    all_clusters: list[dict],
    metadata_snapshot: dict,
    lever_changes: list[dict] | None = None,
    w: WorkspaceClient | None = None,
) -> dict:
    """Single LLM call to synthesize ALL evaluation learnings into holistic instructions.

    Returns ``{"instruction_text": str, "example_sql_proposals": list, "rationale": str}``.
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    _tables = metadata_snapshot.get("tables", []) or ds.get("tables", [])
    _mvs = metadata_snapshot.get("metric_views", []) or ds.get("metric_views", [])
    _funcs = metadata_snapshot.get("functions", []) or ds.get("functions", [])

    config = metadata_snapshot.get("config") or {}
    space_desc = config.get("description") or ""
    if isinstance(space_desc, list):
        space_desc = "\n".join(space_desc)
    if not space_desc:
        space_desc = "(No description set for this Genie Space.)"

    current_instructions = metadata_snapshot.get("general_instructions", "")
    existing_example_sqls = _format_existing_example_sqls(metadata_snapshot)

    format_kwargs: dict[str, Any] = {
        "space_description": space_desc,
        "eval_summary": _format_eval_summary(all_clusters),
        "cluster_briefs": _format_cluster_briefs(all_clusters, top_n=5),
        "lever_summary": _format_lever_summary(lever_changes),
        "current_instructions": current_instructions or "(No current instructions.)",
        "existing_example_sqls": existing_example_sqls,
        "instruction_char_budget": max(0, 24500 - 500),
        "table_names": [t.get("name") or t.get("identifier", "") for t in _tables],
        "mv_names": [m.get("name") or m.get("identifier", "") for m in _mvs],
        "tvf_names": [f.get("name") or f.get("identifier", "") for f in _funcs],
    }

    try:
        prompt = LEVER_5_HOLISTIC_PROMPT.format(**format_kwargs)
    except KeyError:
        prompt = LEVER_5_HOLISTIC_PROMPT.format_map(defaultdict(str, format_kwargs))

    _W = 78
    _hdr = "┌─── LLM Call [LEVER_5_HOLISTIC] " + "─" * max(0, _W - 32)
    _ftr = "└" + "─" * (_W - 1)

    logger.info(
        "\n%s\n│ Clusters: %d\n│ Lever changes: %d\n│ Prompt length: %d chars\n%s",
        _hdr, len(all_clusters), len(lever_changes or []), len(prompt), _ftr,
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
                max_tokens=4096,
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

            instruction_text = result.get("instruction_text", "")
            example_proposals = result.get("example_sql_proposals", [])
            rationale = result.get("rationale", "")

            if instruction_text and len(instruction_text) > MAX_HOLISTIC_INSTRUCTION_CHARS:
                logger.warning(
                    "Holistic instruction text exceeds %d chars (%d), truncating",
                    MAX_HOLISTIC_INSTRUCTION_CHARS, len(instruction_text),
                )
                instruction_text = instruction_text[:MAX_HOLISTIC_INSTRUCTION_CHARS]

            logger.info(
                "\n┌─── LLM Response [LEVER_5_HOLISTIC] ──────────────────────────────────\n"
                "│ Instruction text: %d chars\n"
                "│ Example SQL proposals: %d\n"
                "│ Rationale: %s\n"
                "└─────────────────────────────────────────────────────────────────────────",
                len(instruction_text), len(example_proposals), str(rationale)[:300],
            )
            return {
                "instruction_text": instruction_text,
                "example_sql_proposals": example_proposals if isinstance(example_proposals, list) else [],
                "rationale": rationale,
            }
        except json.JSONDecodeError:
            logger.warning(
                "Holistic LLM response was not valid JSON (attempt %d): %.500s",
                attempt + 1, text,
            )
            if attempt >= LLM_MAX_RETRIES - 1:
                return {"instruction_text": "", "example_sql_proposals": [], "rationale": "JSON parse failed"}
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                logger.exception(
                    "Holistic LLM call failed after %d retries (prompt len: %d)",
                    LLM_MAX_RETRIES, len(prompt),
                )
                return {"instruction_text": "", "example_sql_proposals": [], "rationale": "LLM call failed"}
    return {"instruction_text": "", "example_sql_proposals": [], "rationale": "All retries exhausted"}


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
        if ptype not in ("add_instruction", "add_example_sql", "rewrite_instruction"):
            valid.append(p)
            continue

        if ptype == "rewrite_instruction":
            text = (p.get("proposed_value") or "").strip()
            if not text:
                logger.info("Rejecting empty rewrite_instruction proposal")
                continue
            if len(text) > MAX_HOLISTIC_INSTRUCTION_CHARS:
                logger.warning(
                    "Rejecting rewrite_instruction exceeding %d chars (%d chars)",
                    MAX_HOLISTIC_INSTRUCTION_CHARS, len(text),
                )
                continue
            text_lower = text.lower()
            if known_assets and not any(a in text_lower for a in known_assets):
                logger.warning(
                    "Rejecting generic rewrite_instruction (no known asset referenced): %.100s...",
                    text,
                )
                continue
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


def _ngram_similarity(a: str, b: str, n: int = 3) -> float:
    """Compute Jaccard similarity over character n-grams."""
    if not a or not b:
        return 0.0
    a_lower, b_lower = a.lower(), b.lower()
    a_ngrams = {a_lower[i : i + n] for i in range(len(a_lower) - n + 1)}
    b_ngrams = {b_lower[i : i + n] for i in range(len(b_lower) - n + 1)}
    if not a_ngrams or not b_ngrams:
        return 0.0
    return len(a_ngrams & b_ngrams) / len(a_ngrams | b_ngrams)


def _filter_no_op_proposals(proposals: list[dict], metadata_snapshot: dict) -> list[dict]:
    """Remove proposals that don't meaningfully change the current metadata.

    For ``update_column_description`` / ``update_description``: drops the
    proposal if the proposed value is essentially identical to the existing
    column description (after normalizing whitespace).
    """
    ds = metadata_snapshot.get("data_sources", {})
    if not isinstance(ds, dict):
        ds = {}
    tables = ds.get("tables", []) or metadata_snapshot.get("tables", [])

    existing_descs: dict[tuple[str, str], str] = {}
    for tbl in tables:
        tbl_name = (tbl.get("name") or tbl.get("identifier") or "").lower()
        short_name = tbl_name.rsplit(".", 1)[-1] if "." in tbl_name else tbl_name
        for col in tbl.get("columns", tbl.get("column_configs", [])):
            col_name = (col.get("name") or col.get("column_name") or "").lower()
            raw_desc = col.get("description") or col.get("comment") or ""
            if isinstance(raw_desc, list):
                raw_desc = " ".join(str(x) for x in raw_desc)
            desc = re.sub(r"\s+", " ", str(raw_desc).strip().lower())
            if desc:
                existing_descs[(tbl_name, col_name)] = desc
                existing_descs[(short_name, col_name)] = desc

    kept: list[dict] = []
    dropped = 0
    for p in proposals:
        ptype = p.get("patch_type", "")
        if ptype in ("update_column_description", "update_description", "add_column_synonym"):
            tbl = (p.get("table") or p.get("target_table") or "").lower()
            col = (p.get("column") or p.get("target_column") or "").lower()
            proposed = re.sub(r"\s+", " ", (p.get("proposed_value") or "").strip().lower())
            short_tbl = tbl.rsplit(".", 1)[-1] if "." in tbl else tbl
            current = existing_descs.get((tbl, col)) or existing_descs.get((short_tbl, col)) or ""
            if current and proposed and _ngram_similarity(current, proposed) > 0.97:
                dropped += 1
                continue
        kept.append(p)

    if dropped:
        logger.info(
            "Filtered %d no-op proposals (description unchanged from current metadata)",
            dropped,
        )
    return kept


def _deduplicate_proposals(proposals: list[dict]) -> list[dict]:
    """Remove duplicate proposals using type-aware deduplication.

    - ``update_column_description`` / ``add_column_synonym``: dedup by (table, column).
    - ``add_instruction``: merge near-duplicates (ngram similarity > 0.7).
    - ``add_example_sql``: dedup by normalized SQL text.
    - Others: dedup by exact (patch_type, proposed_value).
    """
    out: list[dict] = []

    col_desc_best: dict[tuple[str, str], int] = {}
    instruction_entries: list[tuple[int, dict]] = []
    example_sql_seen: dict[str, int] = {}
    exact_seen: dict[tuple[str, str], int] = {}

    for p in proposals:
        ptype = p.get("patch_type", "")
        val = (p.get("proposed_value") or "").strip()
        impact = p.get("net_impact", 0)

        if ptype in ("update_column_description", "update_description", "add_column_synonym"):
            tbl = p.get("table", p.get("target_table", ""))
            col = p.get("column", p.get("target_column", ""))
            key = (tbl, col)
            if key in col_desc_best:
                existing_idx = col_desc_best[key]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            col_desc_best[key] = len(out)
            out.append(p)

        elif ptype == "add_instruction" and val:
            merged = False
            for existing_idx, existing_p in instruction_entries:
                existing_val = (existing_p.get("proposed_value") or "").strip()
                if _ngram_similarity(val, existing_val) > 0.7:
                    if impact > out[existing_idx].get("net_impact", 0):
                        out[existing_idx] = p
                        instruction_entries = [
                            (ei, ep) if ei != existing_idx else (ei, p)
                            for ei, ep in instruction_entries
                        ]
                    merged = True
                    break
            if not merged:
                instruction_entries.append((len(out), p))
                out.append(p)

        elif ptype == "add_example_sql" and val:
            normalized = re.sub(r"\s+", " ", val.lower()).strip()
            if normalized in example_sql_seen:
                existing_idx = example_sql_seen[normalized]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            example_sql_seen[normalized] = len(out)
            out.append(p)

        else:
            key = (ptype, val)
            if key in exact_seen:
                existing_idx = exact_seen[key]
                if impact > out[existing_idx].get("net_impact", 0):
                    out[existing_idx] = p
                continue
            exact_seen[key] = len(out)
            out.append(p)

    return out


def _merge_overlapping_instructions(proposals: list[dict]) -> list[dict]:
    """Merge ``add_instruction`` proposals that share >70% keyword overlap.

    Keeps all non-instruction proposals intact. For instructions, groups those
    with high overlap and concatenates into a single combined instruction.
    """
    instructions: list[dict] = []
    others: list[dict] = []
    for p in proposals:
        if p.get("patch_type") == "add_instruction":
            instructions.append(p)
        else:
            others.append(p)

    if len(instructions) <= 1:
        return proposals

    def _keywords(text: str) -> set[str]:
        return {w.lower() for w in re.findall(r"\b\w{3,}\b", text)}

    merged: list[dict] = []
    used: set[int] = set()

    for i, p1 in enumerate(instructions):
        if i in used:
            continue
        group = [p1]
        kw1 = _keywords(p1.get("proposed_value", ""))
        for j, p2 in enumerate(instructions):
            if j <= i or j in used:
                continue
            kw2 = _keywords(p2.get("proposed_value", ""))
            if kw1 and kw2:
                overlap = len(kw1 & kw2) / len(kw1 | kw2)
                if overlap > 0.7:
                    group.append(p2)
                    used.add(j)
        used.add(i)

        if len(group) == 1:
            merged.append(group[0])
        else:
            best = max(group, key=lambda g: g.get("net_impact", 0))
            total_questions = sum(g.get("questions_fixed", 0) for g in group)
            best = dict(best)
            best["questions_fixed"] = total_questions
            best["merged_count"] = len(group)
            merged.append(best)
            logger.info(
                "Merged %d overlapping instructions into one (kept best net_impact=%.2f)",
                len(group), best.get("net_impact", 0),
            )

    return others + merged


_LEVER_NAMES = {1: "Tables & Columns", 2: "Metric Views", 3: "Table-Valued Functions", 4: "Join Specifications", 5: "Genie Space Instructions"}


def _build_provenance(cluster: dict, lever: int, patch_type: str) -> dict:
    """Build a provenance dict from a cluster's question_traces."""
    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "root_cause": cluster.get("root_cause", "other"),
        "originating_questions": cluster.get("question_traces", []),
        "lever": lever,
        "lever_name": _LEVER_NAMES.get(lever, f"Lever {lever}"),
        "patch_type": patch_type,
    }


def generate_metadata_proposals(
    clusters: list[dict],
    metadata_snapshot: dict,
    target_lever: int | None = None,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    failed_levers: set[int] | None = None,
    lever_changes: list[dict] | None = None,
) -> list[dict]:
    """Generate metadata change proposals from failure clusters.

    For each cluster, maps to a lever, calls the LLM to generate a concrete
    ``proposed_value``, resolves scope, and scores by net_impact.

    When *target_lever* is 5, uses **holistic mode**: a single LLM call
    synthesizes ALL evaluation learnings into a coherent instruction document
    (``rewrite_instruction``) plus targeted example SQL proposals. The
    *lever_changes* parameter provides context about what levers 1-4 already
    fixed in this iteration.

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

    # ── Holistic path for lever 5 ─────────────────────────────────────
    if target_lever == 5:
        all_lever5_clusters: list[dict] = []
        for cluster in clusters:
            natural_lever = _map_to_lever(
                cluster["root_cause"],
                asi_failure_type=cluster.get("asi_failure_type"),
                blame_set=cluster.get("asi_blame_set"),
                judge=cluster.get("affected_judge"),
            )
            if natural_lever == 5 or natural_lever in failed_levers:
                all_lever5_clusters.append(cluster)

        holistic_result = _call_llm_for_holistic_instructions(
            all_lever5_clusters if all_lever5_clusters else clusters,
            metadata_snapshot,
            lever_changes=lever_changes,
            w=w,
        )

        instruction_text = holistic_result.get("instruction_text", "")
        example_proposals = holistic_result.get("example_sql_proposals", [])
        rationale = holistic_result.get("rationale", "")

        from genie_space_optimizer.optimization.applier import _get_general_instructions

        current_instructions = _get_general_instructions(
            metadata_snapshot.get("config") or metadata_snapshot
        )

        if instruction_text:
            total_q = sum(len(c.get("question_ids", [])) for c in all_lever5_clusters)
            holistic_traces = []
            for c in (all_lever5_clusters or clusters):
                holistic_traces.extend(c.get("question_traces", []))
            proposals.append({
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": "HOLISTIC_L5",
                "lever": 5,
                "scope": "genie_config",
                "patch_type": "rewrite_instruction",
                "change_description": f"Holistic instruction rewrite ({len(instruction_text)} chars)",
                "proposed_value": instruction_text,
                "old_value": current_instructions,
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                "confidence": 0.8,
                "questions_fixed": total_q,
                "questions_at_risk": 0,
                "net_impact": max(total_q * 0.8, 1.0),
                "asi": {
                    "failure_type": "missing_instruction",
                    "blame_set": [],
                    "severity": "major",
                    "counterfactual_fixes": [],
                    "ambiguity_detected": False,
                },
                "provenance": {
                    "cluster_id": "HOLISTIC_L5",
                    "root_cause": "missing_instruction",
                    "originating_questions": holistic_traces,
                    "lever": 5,
                    "lever_name": "Genie Space Instructions",
                    "patch_type": "rewrite_instruction",
                },
            })

        for idx, ex in enumerate(example_proposals):
            if not isinstance(ex, dict):
                continue
            eq = (ex.get("example_question") or "").strip()
            es = (ex.get("example_sql") or "").strip()
            if not eq or not es:
                continue
            proposals.append({
                "proposal_id": f"P{len(proposals) + 1:03d}",
                "cluster_id": f"HOLISTIC_EX_{idx + 1:03d}",
                "lever": 5,
                "scope": "genie_config",
                "patch_type": "add_example_sql",
                "change_description": f"Example SQL: {eq[:80]}",
                "proposed_value": eq,
                "example_question": eq,
                "example_sql": es,
                "parameters": ex.get("parameters", []),
                "usage_guidance": ex.get("usage_guidance", ""),
                "rationale": rationale,
                "dual_persistence": DUAL_PERSIST_PATHS.get(5, DUAL_PERSIST_PATHS[5]),
                "confidence": 0.75,
                "questions_fixed": 1,
                "questions_at_risk": 0,
                "net_impact": 0.75,
                "asi": {
                    "failure_type": "asset_routing_error",
                    "blame_set": [],
                    "severity": "major",
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

    # ── Standard per-cluster path (levers 1-4) ───────────────────────
    _MAX_CLUSTERS_PER_LEVER = 3
    eligible_clusters: list[tuple[dict, int]] = []
    for cluster in clusters:
        natural_lever = _map_to_lever(
            cluster["root_cause"],
            asi_failure_type=cluster.get("asi_failure_type"),
            blame_set=cluster.get("asi_blame_set"),
            judge=cluster.get("affected_judge"),
        )
        lever = natural_lever
        if target_lever is not None and lever != target_lever:
            continue
        eligible_clusters.append((cluster, lever))

    eligible_clusters.sort(key=lambda x: len(x[0]["question_ids"]), reverse=True)
    if len(eligible_clusters) > _MAX_CLUSTERS_PER_LEVER:
        logger.info(
            "Capping clusters for lever %s: %d -> %d (keeping top by question count)",
            target_lever, len(eligible_clusters), _MAX_CLUSTERS_PER_LEVER,
        )
        eligible_clusters = eligible_clusters[:_MAX_CLUSTERS_PER_LEVER]

    for cluster, lever in eligible_clusters:

        failure_type = cluster.get("asi_failure_type") or cluster.get("root_cause", "other")
        patch_type = _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get(
                (failure_type, 1), "add_instruction"
            ),
        )

        llm_result = _call_llm_for_proposal(cluster, metadata_snapshot, patch_type, lever)

        extra_fields: dict = {}

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
                    "provenance": _build_provenance(cluster, lever, patch_type),
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
                "provenance": _build_provenance(cluster, lever, patch_type),
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
    proposals = _merge_overlapping_instructions(proposals)
    proposals = _filter_no_op_proposals(proposals, metadata_snapshot)
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
