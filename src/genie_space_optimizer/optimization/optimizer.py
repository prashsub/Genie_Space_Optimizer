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

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    CONFLICT_RULES,
    FAILURE_TAXONOMY,
    GENERIC_FIX_PREFIXES,
    LEVER_4_JOIN_SPEC_PROMPT,
    LEVER_5_DISCOVERY_PROMPT,
    LEVER_6_INSTRUCTION_PROMPT,
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
    6: {
        "api": "PATCH /api/2.0/genie/spaces/{space_id}",
        "repo": "src/genie/{domain}_genie_export.json",
    },
}


# ═══════════════════════════════════════════════════════════════════════
# 1. Failure-to-Lever Mapping (pure)
# ═══════════════════════════════════════════════════════════════════════


def _map_to_lever(
    root_cause: str,
    asi_failure_type: str | None = None,
    blame_set: list | str | None = None,
) -> int:
    """Map a failure root cause to its primary control lever (1-6).

    ASI ``failure_type`` takes precedence when present since it comes
    directly from the FAILURE_TAXONOMY and is more precise.
    """
    ft = asi_failure_type or root_cause

    if ft == "repeatability_issue":
        bs = str(blame_set).upper() if blame_set else ""
        return 6 if "TVF" in bs else 1

    mapping = {
        "wrong_column": 1,
        "wrong_table": 1,
        "description_mismatch": 1,
        "wrong_aggregation": 2,
        "wrong_measure": 2,
        "missing_filter": 2,
        "missing_temporal_filter": 2,
        "tvf_parameter_error": 3,
        "wrong_join": 4,
        "missing_join_spec": 4,
        "wrong_join_spec": 4,
        "missing_synonym": 5,
        "missing_format_assistance": 5,
        "missing_entity_matching": 5,
        "asset_routing_error": 6,
        "missing_instruction": 6,
        "ambiguous_question": 6,
    }

    if asi_failure_type and asi_failure_type in mapping:
        return mapping[asi_failure_type]
    return mapping.get(root_cause, 6)


def _resolve_scope(lever: int, apply_mode: str = APPLY_MODE) -> str:
    """Determine where a patch is applied based on lever and apply_mode.

    Levers 4-6 are always ``genie_config`` (Genie Space native structures).
    Levers 1-3 are governed by ``apply_mode``.
    """
    if lever in (4, 5, 6):
        return "genie_config"
    return apply_mode


# ═══════════════════════════════════════════════════════════════════════
# 2. Failure Clustering (pure)
# ═══════════════════════════════════════════════════════════════════════


def _extract_pattern(rationale: str) -> str:
    """Extract a generalizable pattern from a judge rationale string."""
    r = (rationale or "").lower()
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
    return "other"


def cluster_failures(eval_results: dict, metadata_snapshot: dict) -> list[dict]:
    """Group evaluation failures into actionable clusters.

    Groups by ``(judge, asi_failure_type, blame_set_str)``.  Falls back to
    ``(judge, _extract_pattern(rationale), "")`` when ASI is absent.
    Only returns clusters with >= 2 questions; singletons go to long-tail.
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
        for col_name, val in list(row.items()):
            judge: str | None = None
            if col_name.startswith("feedback/"):
                judge = col_name.replace("feedback/", "")
            elif col_name.endswith("/value"):
                judge = col_name.replace("/value", "")
            if judge and "no" in str(val).lower():
                rationale = (
                    row.get(f"{judge}/rationale")
                    or row.get(f"rationale/{judge}")
                    or row.get("rationale", "")
                )
                question_id = (
                    row.get("inputs/question_id")
                    or row.get("inputs/question")
                    or row.get("question_id")
                    or row.get("question", "unknown")
                )
                judge_meta = row.get(f"{judge}/metadata", {})
                if not isinstance(judge_meta, dict):
                    judge_meta = {}
                asi_failure_type = (
                    row.get(f"metadata/{judge}/failure_type")
                    or judge_meta.get("failure_type")
                    or row.get("metadata/failure_type")
                )
                asi_blame_set = (
                    row.get(f"metadata/{judge}/blame_set")
                    or judge_meta.get("blame_set")
                    or row.get("metadata/blame_set")
                )
                asi_counterfactual = (
                    row.get(f"metadata/{judge}/counterfactual_fix")
                    or judge_meta.get("counterfactual_fix")
                    or row.get("metadata/counterfactual_fix")
                )
                failures.append(
                    {
                        "question_id": question_id,
                        "judge": judge,
                        "rationale": rationale,
                        "asi_failure_type": asi_failure_type,
                        "asi_blame_set": asi_blame_set,
                        "asi_counterfactual_fix": asi_counterfactual,
                    }
                )

    pattern_groups: dict[tuple, list] = defaultdict(list)
    for f in failures:
        if f.get("asi_failure_type"):
            blame = str(f.get("asi_blame_set", "")) if f.get("asi_blame_set") else ""
            key = (f["judge"], f["asi_failure_type"], blame)
        else:
            key = (f["judge"], _extract_pattern(f["rationale"]), "")
        pattern_groups[key].append(f)

    clusters: list[dict] = []
    for group_key, items in pattern_groups.items():
        judge, pattern, blame = group_key[0], group_key[1], group_key[2] if len(group_key) > 2 else ""
        sample_asi_type = next(
            (i["asi_failure_type"] for i in items if i.get("asi_failure_type")), None
        )
        entry = {
            "cluster_id": f"C{len(clusters) + 1:03d}",
            "root_cause": pattern,
            "question_ids": [i["question_id"] for i in items],
            "affected_judge": judge,
            "confidence": min(0.9, 0.5 + 0.1 * len(items)),
            "asi_failure_type": sample_asi_type,
            "asi_blame_set": blame or None,
            "asi_counterfactual_fixes": [
                i["asi_counterfactual_fix"]
                for i in items
                if i.get("asi_counterfactual_fix")
            ],
        }
        if len(items) >= 2:
            clusters.append(entry)

    clusters.sort(key=lambda c: len(c["question_ids"]), reverse=True)
    return clusters


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
    """Extract relevant metadata sections for the blamed objects."""
    if not blame_set or not metadata_snapshot:
        return "(no metadata available)"

    blame_items = blame_set if isinstance(blame_set, list) else [str(blame_set)]
    sections: list[str] = []

    for table in metadata_snapshot.get("tables", []):
        table_name = table.get("name") or table.get("identifier", "")
        if any(b.lower() in table_name.lower() for b in blame_items):
            sections.append(f"Table: {table_name}")
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                desc = col.get("description", "")
                if isinstance(desc, list):
                    desc = " ".join(desc)
                sections.append(f"  Column: {col_name} — {desc}")

    for col_item in blame_items:
        for table in metadata_snapshot.get("tables", []):
            for col in table.get("columns", table.get("column_configs", [])):
                col_name = col.get("name") or col.get("column_name", "")
                if col_item.lower() == col_name.lower():
                    desc = col.get("description", "")
                    if isinstance(desc, list):
                        desc = " ".join(desc)
                    tname = table.get("name") or table.get("identifier", "")
                    sections.append(f"Column {tname}.{col_name}: {desc}")

    instructions = metadata_snapshot.get("general_instructions", "")
    if instructions and any("instruction" in b.lower() for b in blame_items):
        sections.append(f"Instructions: {instructions[:500]}")

    return "\n".join(sections) if sections else "(blamed objects not found in metadata)"


def _describe_patch_type(patch_type: str) -> str:
    """Human-readable description of a patch type."""
    pt_info = PATCH_TYPES.get(patch_type)
    if pt_info:
        return (
            f"{patch_type}: scope={pt_info['scope']}, "
            f"risk={pt_info['risk_level']}, affects={pt_info['affects']}"
        )
    return patch_type


def _call_llm_for_proposal(
    cluster: dict,
    metadata_snapshot: dict,
    patch_type: str,
    lever: int,
) -> dict:
    """Call Databricks Claude Opus 4.6 to generate proposal text.

    Returns ``{"proposed_value": str, "rationale": str}``.
    """
    prompt_map = {
        4: LEVER_4_JOIN_SPEC_PROMPT,
        5: LEVER_5_DISCOVERY_PROMPT,
        6: LEVER_6_INSTRUCTION_PROMPT,
    }
    prompt_template = prompt_map.get(lever, PROPOSAL_GENERATION_PROMPT)

    current_dict_count = sum(
        1
        for t in metadata_snapshot.get("tables", [])
        for c in t.get("column_configs", [])
        if c.get("build_value_dictionary")
    )

    format_kwargs: dict[str, Any] = {
        "failure_type": cluster.get("asi_failure_type", cluster.get("root_cause", "")),
        "blame_set": cluster.get("asi_blame_set", ""),
        "affected_questions": cluster.get("question_ids", []),
        "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
        "severity": "major",
        "current_metadata": _extract_metadata_for_blame(
            metadata_snapshot, cluster.get("asi_blame_set")
        ),
        "patch_type_description": _describe_patch_type(patch_type),
        "failures_context": json.dumps(cluster, default=str),
        "current_join_specs": json.dumps(
            metadata_snapshot.get("join_specs", []), default=str
        ),
        "table_relationships": json.dumps(
            [
                t.get("relationships", [])
                for t in metadata_snapshot.get("tables", [])
            ],
            default=str,
        ),
        "current_column_configs": json.dumps(
            metadata_snapshot.get("column_configs", {}), default=str
        ),
        "string_column_count": metadata_snapshot.get("string_column_count", 0),
        "max_value_dictionary_cols": MAX_VALUE_DICTIONARY_COLUMNS,
        "current_dictionary_count": current_dict_count,
        "current_instructions": metadata_snapshot.get("general_instructions", ""),
        "table_names": [
            t.get("name") or t.get("identifier", "")
            for t in metadata_snapshot.get("tables", [])
        ],
        "mv_names": [
            m.get("name") or m.get("identifier", "")
            for m in metadata_snapshot.get("metric_views", [])
        ],
        "tvf_names": [
            f.get("name") or f.get("identifier", "")
            for f in metadata_snapshot.get("functions", [])
        ],
    }

    try:
        prompt = prompt_template.format(**format_kwargs)
    except KeyError:
        prompt = prompt_template.format_map(defaultdict(str, format_kwargs))

    import time

    from databricks.sdk import WorkspaceClient as _WC

    for attempt in range(LLM_MAX_RETRIES):
        try:
            w = _WC()
            response = w.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=1024,
            )
            text = response.choices[0].message.content
            text = text.strip()
            if text.startswith("```"):
                text = re.sub(r"^```(?:json)?\s*", "", text)
                text = re.sub(r"\s*```$", "", text)
            return json.loads(text)
        except json.JSONDecodeError:
            return {"proposed_value": text, "rationale": "LLM response was not valid JSON"}
        except Exception:
            if attempt < LLM_MAX_RETRIES - 1:
                time.sleep(2**attempt)
            else:
                logger.exception("LLM call failed after %d retries", LLM_MAX_RETRIES)
                return {
                    "proposed_value": "",
                    "rationale": "LLM call failed",
                }


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


def generate_metadata_proposals(
    clusters: list[dict],
    metadata_snapshot: dict,
    target_lever: int | None = None,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
) -> list[dict]:
    """Generate metadata change proposals from failure clusters.

    For each cluster, maps to a lever, calls the LLM to generate a concrete
    ``proposed_value``, resolves scope, and scores by net_impact.
    """
    proposals: list[dict] = []
    for cluster in clusters:
        lever = _map_to_lever(
            cluster["root_cause"],
            asi_failure_type=cluster.get("asi_failure_type"),
            blame_set=cluster.get("asi_blame_set"),
        )
        if target_lever is not None and lever != target_lever:
            continue

        failure_type = cluster.get("asi_failure_type") or cluster.get("root_cause", "other")
        patch_type = _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get(
                (failure_type, 1), "add_instruction"
            ),
        )

        llm_result = _call_llm_for_proposal(cluster, metadata_snapshot, patch_type, lever)
        proposed_value = llm_result.get("proposed_value") or llm_result.get(
            "instruction_text", ""
        )
        rationale = llm_result.get("rationale", "")

        scope = _resolve_scope(lever, apply_mode)
        q_fixed = len(cluster["question_ids"])
        confidence = cluster["confidence"]
        total_objects = max(len(metadata_snapshot.get("tables", [])), 1)
        blast_radius = 1
        net_impact = q_fixed * confidence - 0.1 * (blast_radius / total_objects)

        proposal = {
            "proposal_id": f"P{len(proposals) + 1:03d}",
            "cluster_id": cluster["cluster_id"],
            "lever": lever,
            "scope": scope,
            "patch_type": patch_type,
            "change_description": proposed_value or _describe_fix(cluster),
            "proposed_value": proposed_value,
            "rationale": rationale,
            "dual_persistence": DUAL_PERSIST_PATHS.get(lever, DUAL_PERSIST_PATHS[6]),
            "confidence": confidence,
            "questions_fixed": q_fixed,
            "questions_at_risk": 0,
            "net_impact": net_impact,
            "asi": {
                "failure_type": failure_type,
                "blame_set": cluster.get("asi_blame_set") or [],
                "severity": "major",
                "counterfactual_fixes": cluster.get("asi_counterfactual_fixes", []),
                "ambiguity_detected": cluster.get("root_cause") == "repeatability_issue",
            },
        }
        proposals.append(proposal)

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
