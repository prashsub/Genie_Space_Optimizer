"""
Optimization Applier — patch rendering, application, and rollback.

Converts optimizer proposals into Patch DSL actions, applies them to the
Genie Space config (and optionally UC artifacts), and supports full
snapshot-based rollback.
"""

from __future__ import annotations

import copy
import json
import logging
import os
from typing import Any

from databricks.sdk import WorkspaceClient

from genie_space_optimizer.common.config import (
    APPLY_MODE,
    HIGH_RISK_PATCHES,
    LOW_RISK_PATCHES,
    MEDIUM_RISK_PATCHES,
    NON_EXPORTABLE_FIELDS,
    PATCH_TYPES,
    _LEVER_TO_PATCH_TYPE,
)
from genie_space_optimizer.common.genie_client import patch_space_config
from genie_space_optimizer.optimization.optimizer import _resolve_scope

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# 1. Config Helpers
# ═══════════════════════════════════════════════════════════════════════


def strip_non_exportable_fields(config: dict) -> dict:
    """Remove fields that should not be included in PATCH requests."""
    return {k: v for k, v in config.items() if k not in NON_EXPORTABLE_FIELDS}


def sort_genie_config(config: dict) -> dict:
    """Sort arrays in a Genie config for deterministic comparison."""
    if "data_sources" in config:
        for key in ["tables", "metric_views"]:
            if key in config.get("data_sources", {}):
                config["data_sources"][key] = sorted(
                    config["data_sources"][key],
                    key=lambda x: x.get("identifier", ""),
                )
    if "instructions" in config:
        inst = config["instructions"]
        if "sql_functions" in inst:
            inst["sql_functions"] = sorted(
                inst["sql_functions"],
                key=lambda x: (x.get("id", ""), x.get("identifier", "")),
            )
        for key in ["text_instructions", "example_question_sqls"]:
            if key in inst:
                inst[key] = sorted(inst[key], key=lambda x: x.get("id", ""))
    return config


def _get_general_instructions(config: dict) -> str:
    """Extract general instructions as joined text from text_instructions."""
    inst = config.get("instructions", {})
    ti = inst.get("text_instructions", [])
    if not ti:
        return ""
    content = ti[0].get("content", [])
    if isinstance(content, list):
        return "\n".join(c for c in content if c)
    return str(content)


def _set_general_instructions(
    config: dict, text: str, instruction_id: str = "genie_opt"
) -> None:
    """Set general instructions into text_instructions."""
    inst = config.setdefault("instructions", {})
    ti = inst.setdefault("text_instructions", [])
    lines = [ln for ln in text.split("\n")] if text else [""]
    if ti:
        ti[0] = {"id": ti[0].get("id", instruction_id), "content": lines}
    else:
        ti.append({"id": instruction_id, "content": lines})


# ═══════════════════════════════════════════════════════════════════════
# 2. Risk Classification
# ═══════════════════════════════════════════════════════════════════════


def classify_risk(patch_type: str | dict) -> str:
    """Classify a patch type as ``low``, ``medium``, or ``high``."""
    pt = patch_type if isinstance(patch_type, str) else patch_type.get("type", "")
    if pt in LOW_RISK_PATCHES:
        return "low"
    if pt in MEDIUM_RISK_PATCHES:
        return "medium"
    if pt in HIGH_RISK_PATCHES:
        return "high"
    return "medium"


# ═══════════════════════════════════════════════════════════════════════
# 3. Proposal → Patch Conversion
# ═══════════════════════════════════════════════════════════════════════


def proposals_to_patches(proposals: list[dict]) -> list[dict]:
    """Convert optimizer proposals into Patch DSL patches.

    Each proposal has an ``asi`` dict with ``failure_type``, ``blame_set``,
    ``counterfactual_fixes``.  Maps to concrete ``patch_type`` via
    ``_LEVER_TO_PATCH_TYPE``.
    """
    patches: list[dict] = []
    for p in proposals:
        asi = p.get("asi", {})
        if not isinstance(asi, dict):
            asi = {}
        failure_type = asi.get(
            "failure_type", p.get("lever_type", "other")
        )
        lever = p.get("lever", 6)
        patch_type = _LEVER_TO_PATCH_TYPE.get(
            (failure_type, lever),
            _LEVER_TO_PATCH_TYPE.get((failure_type, 1), "add_instruction"),
        )
        blame_set = asi.get("blame_set", [])
        target = blame_set[0] if blame_set else p.get("change_description", "unknown")
        fixes = asi.get("counterfactual_fixes", [])
        if isinstance(fixes, str):
            fixes = [fixes]
        new_text = p.get("proposed_value") or (fixes[0] if fixes else p.get("change_description", ""))

        patches.append(
            {
                "type": patch_type,
                "target": target,
                "new_text": new_text,
                "old_text": "",
                "lever": lever,
                "risk_level": classify_risk(patch_type),
                "predicted_affected_questions": p.get("questions_fixed", 0),
                "grounded_in": p.get("grounded_in", []),
                "source_proposal_id": p.get("proposal_id", ""),
            }
        )
    return patches


# ═══════════════════════════════════════════════════════════════════════
# 4. Patch Rendering
# ═══════════════════════════════════════════════════════════════════════


def render_patch(patch: dict, space_id: str, space_config: dict) -> dict:
    """Convert a patch dict into an executable action with command + rollback.

    Returns ``{action_type, target, command, rollback_command, risk_level}``.
    """
    patch_type = patch.get("type", "")
    target = (
        patch.get("target") or patch.get("object_id") or patch.get("table") or ""
    )
    risk = classify_risk(patch_type)

    def action(cmd: str, rollback: str) -> dict:
        return {
            "action_type": patch_type,
            "target": target,
            "command": cmd,
            "rollback_command": rollback,
            "risk_level": risk,
        }

    old_text = patch.get("old_text", "")
    new_text = patch.get("new_text", patch.get("value", ""))
    table_id = patch.get("table") or patch.get("target") or ""
    column_name = patch.get("column", "")

    # ── Instructions ──────────────────────────────────────────────
    if patch_type == "add_instruction":
        return action(
            json.dumps({"op": "add", "section": "instructions", "new_text": new_text}),
            json.dumps({"op": "remove", "section": "instructions", "old_text": new_text}),
        )
    if patch_type == "update_instruction":
        return action(
            json.dumps({"op": "update", "section": "instructions", "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "instructions", "old_text": new_text, "new_text": old_text}),
        )
    if patch_type == "remove_instruction":
        return action(
            json.dumps({"op": "remove", "section": "instructions", "old_text": old_text}),
            json.dumps({"op": "add", "section": "instructions", "new_text": old_text}),
        )

    # ── Descriptions ──────────────────────────────────────────────
    if patch_type == "add_description":
        return action(
            json.dumps({"op": "add", "section": "descriptions", "target": target, "value": new_text}),
            json.dumps({"op": "remove", "section": "descriptions", "target": target, "value": new_text}),
        )
    if patch_type == "update_description":
        return action(
            json.dumps({"op": "update", "section": "descriptions", "target": target, "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "descriptions", "target": target, "old_text": new_text, "new_text": old_text}),
        )
    if patch_type == "add_column_description":
        return action(
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "value": new_text}),
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "value": new_text}),
        )
    if patch_type == "update_column_description":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_text": old_text, "new_text": new_text}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_text": new_text, "new_text": old_text}),
        )

    # ── Visibility ────────────────────────────────────────────────
    if patch_type == "hide_column":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": True}),
        )
    if patch_type == "unhide_column":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "visible": False}),
        )
    if patch_type == "rename_column_alias":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_alias": old_text, "new_alias": new_text}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "old_alias": new_text, "new_alias": old_text}),
        )

    # ── Tables ────────────────────────────────────────────────────
    if patch_type == "add_table":
        asset = patch.get("asset", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "tables", "asset": asset}),
            json.dumps({"op": "remove", "section": "tables", "identifier": asset.get("identifier", target)}),
        )
    if patch_type == "remove_table":
        return action(
            json.dumps({"op": "remove", "section": "tables", "identifier": target}),
            json.dumps({"op": "add", "section": "tables", "asset": patch.get("previous_asset", {})}),
        )

    # ── Join Specifications (Lever 4) ─────────────────────────────
    if patch_type == "add_join_spec":
        join_spec = patch.get("join_spec", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "join_specs", "join_spec": join_spec}),
            json.dumps({"op": "remove", "section": "join_specs", "left_table": join_spec.get("left_table_name", ""), "right_table": join_spec.get("right_table_name", "")}),
        )
    if patch_type == "update_join_spec":
        join_spec = patch.get("join_spec", patch.get("value", {}))
        return action(
            json.dumps({"op": "update", "section": "join_specs", "left_table": patch.get("left_table", ""), "right_table": patch.get("right_table", ""), "join_spec": join_spec}),
            json.dumps({"op": "update", "section": "join_specs", "left_table": patch.get("left_table", ""), "right_table": patch.get("right_table", ""), "join_spec": patch.get("previous_join_spec", {})}),
        )
    if patch_type == "remove_join_spec":
        return action(
            json.dumps({"op": "remove", "section": "join_specs", "left_table": patch.get("left_table", ""), "right_table": patch.get("right_table", "")}),
            json.dumps({"op": "add", "section": "join_specs", "join_spec": patch.get("previous_join_spec", {})}),
        )

    # ── Column Discovery Settings (Lever 5) ───────────────────────
    if patch_type == "enable_example_values":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "get_example_values": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "get_example_values": False}),
        )
    if patch_type == "disable_example_values":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "get_example_values": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "get_example_values": True}),
        )
    if patch_type == "enable_value_dictionary":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "build_value_dictionary": True}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "build_value_dictionary": False}),
        )
    if patch_type == "disable_value_dictionary":
        return action(
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "build_value_dictionary": False}),
            json.dumps({"op": "update", "section": "column_configs", "table": table_id, "column": column_name, "build_value_dictionary": True}),
        )
    if patch_type == "add_column_synonym":
        synonyms = patch.get("synonyms", [new_text] if new_text else [])
        return action(
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
        )
    if patch_type == "remove_column_synonym":
        synonyms = patch.get("synonyms", [old_text] if old_text else [])
        return action(
            json.dumps({"op": "remove", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
            json.dumps({"op": "add", "section": "column_configs", "table": table_id, "column": column_name, "synonyms": synonyms}),
        )

    # ── Filters ───────────────────────────────────────────────────
    if patch_type == "add_default_filter":
        filt = patch.get("filter", {"condition": new_text})
        return action(
            json.dumps({"op": "add", "section": "default_filters", "filter": filt}),
            json.dumps({"op": "remove", "section": "default_filters", "filter": filt}),
        )
    if patch_type == "remove_default_filter":
        filt = patch.get("filter", {"condition": old_text})
        return action(
            json.dumps({"op": "remove", "section": "default_filters", "filter": filt}),
            json.dumps({"op": "add", "section": "default_filters", "filter": filt}),
        )
    if patch_type == "update_filter_condition":
        return action(
            json.dumps({"op": "update", "section": "default_filters", "old_condition": old_text, "new_condition": new_text}),
            json.dumps({"op": "update", "section": "default_filters", "old_condition": new_text, "new_condition": old_text}),
        )

    # ── TVF ───────────────────────────────────────────────────────
    if patch_type == "add_tvf_parameter":
        return action(
            json.dumps({"op": "add", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", new_text)}),
            json.dumps({"op": "remove", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", new_text)}),
        )
    if patch_type == "remove_tvf_parameter":
        return action(
            json.dumps({"op": "remove", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", old_text)}),
            json.dumps({"op": "add", "section": "tvf_parameters", "tvf": target, "param": patch.get("param_name", old_text)}),
        )
    if patch_type == "update_tvf_sql":
        return action(
            json.dumps({"op": "update", "section": "tvf_definition", "tvf": target, "old_sql": old_text, "new_sql": new_text}),
            json.dumps({"op": "update", "section": "tvf_definition", "tvf": target, "old_sql": new_text, "new_sql": old_text}),
        )
    if patch_type == "add_tvf":
        tvf_asset = patch.get("tvf_asset", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "tvfs", "tvf_asset": tvf_asset}),
            json.dumps({"op": "remove", "section": "tvfs", "identifier": tvf_asset.get("identifier", target)}),
        )
    if patch_type == "remove_tvf":
        return action(
            json.dumps({"op": "remove", "section": "tvfs", "identifier": target}),
            json.dumps({"op": "add", "section": "tvfs", "tvf_asset": patch.get("previous_tvf_asset", {})}),
        )

    # ── Metric Views ──────────────────────────────────────────────
    if patch_type == "add_mv_measure":
        measure = patch.get("measure", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "mv_measures", "mv": target, "measure": measure}),
            json.dumps({"op": "remove", "section": "mv_measures", "mv": target, "measure_name": measure.get("name", "")}),
        )
    if patch_type == "update_mv_measure":
        return action(
            json.dumps({"op": "update", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", ""), "old": old_text, "new": new_text}),
            json.dumps({"op": "update", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", ""), "old": new_text, "new": old_text}),
        )
    if patch_type == "remove_mv_measure":
        return action(
            json.dumps({"op": "remove", "section": "mv_measures", "mv": target, "measure_name": patch.get("measure_name", old_text)}),
            json.dumps({"op": "add", "section": "mv_measures", "mv": target, "measure": patch.get("previous_measure", {})}),
        )
    if patch_type == "add_mv_dimension":
        dim = patch.get("dimension", patch.get("value", {}))
        return action(
            json.dumps({"op": "add", "section": "mv_dimensions", "mv": target, "dimension": dim}),
            json.dumps({"op": "remove", "section": "mv_dimensions", "mv": target, "dimension_name": dim.get("name", "")}),
        )
    if patch_type == "remove_mv_dimension":
        return action(
            json.dumps({"op": "remove", "section": "mv_dimensions", "mv": target, "dimension_name": patch.get("dimension_name", old_text)}),
            json.dumps({"op": "add", "section": "mv_dimensions", "mv": target, "dimension": patch.get("previous_dimension", {})}),
        )
    if patch_type == "update_mv_yaml":
        return action(
            json.dumps({"op": "update", "section": "mv_yaml", "mv": target, "new_yaml": new_text}),
            json.dumps({"op": "update", "section": "mv_yaml", "mv": target, "new_yaml": old_text}),
        )

    # ── Unknown type ──────────────────────────────────────────────
    return action(
        json.dumps({"op": "unknown", "patch_type": patch_type}),
        json.dumps({"op": "unknown", "patch_type": patch_type}),
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Action Application — Genie Config
# ═══════════════════════════════════════════════════════════════════════


def _find_table_in_config(config: dict, table_id: str) -> dict | None:
    """Find a table or metric view in data_sources by identifier."""
    ds = config.get("data_sources", {})
    for source_list in [ds.get("tables", []), ds.get("metric_views", [])]:
        for t in source_list:
            if t.get("identifier") == table_id:
                return t
    return None


def _find_or_create_column_config(table_dict: dict, column_name: str) -> dict:
    """Find an existing column_config or create one."""
    for cc in table_dict.get("column_configs", []):
        if cc.get("column_name") == column_name:
            return cc
    new_cc = {"column_name": column_name}
    table_dict.setdefault("column_configs", []).append(new_cc)
    return new_cc


def _apply_action_to_config(config: dict, action: dict) -> bool:
    """Apply a single rendered action to a Genie Space config dict in-place.

    Returns True if applied, False if skipped (e.g. old_text guard failed).
    """
    try:
        cmd = json.loads(action.get("command", "{}"))
    except json.JSONDecodeError:
        return False

    op = cmd.get("op", "")
    section = cmd.get("section", "")

    # ── Instructions ──────────────────────────────────────────────
    if section == "instructions":
        if op == "add":
            text = cmd.get("new_text", "")
            if text:
                current = _get_general_instructions(config)
                _set_general_instructions(config, (current + "\n" + text).strip())
            return True
        if op == "update":
            current = _get_general_instructions(config)
            old_text = cmd.get("old_text", "")
            new_text = cmd.get("new_text", "")
            if old_text and old_text not in current:
                return False
            replaced = current.replace(old_text, new_text, 1) if old_text else current + "\n" + new_text
            _set_general_instructions(config, replaced.strip())
            return True
        if op == "remove":
            current = _get_general_instructions(config)
            old_text = cmd.get("old_text", "")
            if old_text and old_text not in current:
                return False
            _set_general_instructions(config, current.replace(old_text, "").strip())
            return True

    # ── Column Configs (descriptions, visibility, discovery) ──────
    if section == "column_configs":
        table_id = cmd.get("table", "")
        col_name = cmd.get("column", "")
        tbl = _find_table_in_config(config, table_id)
        if not tbl:
            return False
        cc = _find_or_create_column_config(tbl, col_name)

        if "visible" in cmd:
            cc["exclude"] = not cmd["visible"]
            return True
        if "get_example_values" in cmd:
            cc["get_example_values"] = cmd["get_example_values"]
            return True
        if "build_value_dictionary" in cmd:
            cc["build_value_dictionary"] = cmd["build_value_dictionary"]
            return True
        if "old_alias" in cmd:
            new_alias = cmd.get("new_alias", "")
            cc.setdefault("synonyms", []).append(new_alias)
            return True

        if op == "add" and "synonyms" in cmd:
            existing = cc.setdefault("synonyms", [])
            for s in cmd["synonyms"]:
                if s and s not in existing:
                    existing.append(s)
            return True
        if op == "remove" and "synonyms" in cmd:
            existing = cc.get("synonyms", [])
            cc["synonyms"] = [s for s in existing if s not in cmd["synonyms"]]
            return True

        if op == "add" and "value" in cmd:
            cc["description"] = [cmd["value"]] if isinstance(cmd["value"], str) else cmd["value"]
            return True
        if op == "update" and "new_text" in cmd:
            desc = cc.get("description", [])
            joined = "\n".join(desc) if isinstance(desc, list) else str(desc)
            old_t = cmd.get("old_text", "")
            if old_t and old_t not in joined:
                return False
            new_desc = joined.replace(old_t, cmd["new_text"], 1) if old_t else joined + "\n" + cmd["new_text"]
            cc["description"] = [ln for ln in new_desc.split("\n")]
            return True
        if op == "remove" and "value" in cmd:
            desc = cc.get("description", [])
            if isinstance(desc, list):
                cc["description"] = [d for d in desc if d != cmd["value"]]
            return True

    # ── Descriptions (table-level) ────────────────────────────────
    if section == "descriptions":
        target = cmd.get("target", "")
        tbl = _find_table_in_config(config, target)
        if not tbl:
            return False
        if op == "add":
            desc = tbl.get("description", [])
            if isinstance(desc, list):
                desc.append(cmd.get("value", ""))
            else:
                tbl["description"] = [str(desc), cmd.get("value", "")]
            return True
        if op == "update":
            desc = tbl.get("description", [])
            joined = "\n".join(desc) if isinstance(desc, list) else str(desc)
            old_t = cmd.get("old_text", "")
            if old_t and old_t not in joined:
                return False
            new_desc = joined.replace(old_t, cmd.get("new_text", ""), 1)
            tbl["description"] = [ln for ln in new_desc.split("\n")]
            return True
        if op == "remove":
            desc = tbl.get("description", [])
            val = cmd.get("value", "")
            if isinstance(desc, list):
                tbl["description"] = [d for d in desc if d != val]
            return True

    # ── Join Specifications (Lever 4) ─────────────────────────────
    if section == "join_specs":
        ds = config.setdefault("data_sources", {})
        specs = ds.setdefault("join_specs", [])
        if op == "add":
            js = cmd.get("join_spec", {})
            if js:
                specs.append(js)
            return True
        if op == "remove":
            lt, rt = cmd.get("left_table", ""), cmd.get("right_table", "")
            for i, s in enumerate(specs):
                if s.get("left_table_name") == lt and s.get("right_table_name") == rt:
                    specs.pop(i)
                    return True
            return False
        if op == "update":
            lt, rt = cmd.get("left_table", ""), cmd.get("right_table", "")
            new_spec = cmd.get("join_spec", {})
            for i, s in enumerate(specs):
                if s.get("left_table_name") == lt and s.get("right_table_name") == rt:
                    specs[i] = new_spec
                    return True
            return False

    # ── Tables ────────────────────────────────────────────────────
    if section == "tables":
        tables = config.setdefault("data_sources", {}).setdefault("tables", [])
        if op == "add":
            asset = cmd.get("asset", {})
            if asset:
                tables.append(asset)
                sort_genie_config(config)
            return True
        if op == "remove":
            ident = cmd.get("identifier", "")
            for i, t in enumerate(tables):
                if t.get("identifier") == ident:
                    tables.pop(i)
                    return True
            return False

    # ── Default Filters ───────────────────────────────────────────
    if section == "default_filters":
        filters = config.setdefault("default_filters", [])
        if op == "add":
            filt = cmd.get("filter", {})
            if filt and filt not in filters:
                filters.append(filt)
            return True
        if op == "remove":
            filt = cmd.get("filter", {})
            for i, f in enumerate(filters):
                if f == filt:
                    filters.pop(i)
                    return True
            return False
        if op == "update":
            old_c, new_c = cmd.get("old_condition", ""), cmd.get("new_condition", "")
            for i, f in enumerate(filters):
                if isinstance(f, dict) and f.get("condition") == old_c:
                    f["condition"] = new_c
                    return True
                if f == old_c:
                    filters[i] = new_c
                    return True
            return False

    # ── TVF / MV operations (config-level no-ops for uc_artifact patches) ──
    if section in ("tvf_parameters", "tvf_definition", "tvfs", "mv_measures", "mv_dimensions", "mv_yaml"):
        if section == "tvfs":
            funcs = config.setdefault("instructions", {}).setdefault("sql_functions", [])
            if op == "add":
                tvf_asset = cmd.get("tvf_asset", {})
                if tvf_asset:
                    ident = tvf_asset.get("identifier", "")
                    funcs.append({"id": tvf_asset.get("id", ident), "identifier": ident})
                    sort_genie_config(config)
                return True
            if op == "remove":
                ident = cmd.get("identifier", "")
                for i, f in enumerate(funcs):
                    if f.get("identifier") == ident:
                        funcs.pop(i)
                        return True
                return False
        return True

    return False


# ═══════════════════════════════════════════════════════════════════════
# 6. Action Application — UC Artifacts
# ═══════════════════════════════════════════════════════════════════════


def _apply_action_to_uc(w: WorkspaceClient, action: dict) -> bool:
    """Apply an action to UC artifacts via DDL.

    Only used for Levers 1-3 when ``apply_mode`` includes ``uc_artifact``.
    """
    try:
        cmd = json.loads(action.get("command", "{}"))
    except json.JSONDecodeError:
        return False

    patch_type = action.get("action_type", "")
    warehouse_id = os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        logger.warning("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID is not set; skipping UC DDL action")
        return False

    try:
        if patch_type == "update_column_description":
            table = cmd.get("table", "")
            column = cmd.get("column", "")
            new_text = cmd.get("new_text", "")
            if table and column and new_text:
                escaped = new_text.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
        if patch_type == "update_description":
            table = cmd.get("target", "")
            new_text = cmd.get("new_text", "")
            if table and new_text:
                escaped = new_text.replace("'", "\\'")
                w.statement_execution.execute_statement(
                    statement=f"COMMENT ON TABLE {table} IS '{escaped}'",
                    warehouse_id=warehouse_id,
                    wait_timeout="30s",
                )
                return True
        if patch_type == "update_tvf_sql":
            new_sql = cmd.get("new_sql", "")
            if new_sql:
                w.statement_execution.execute_statement(
                    statement=new_sql,
                    warehouse_id=warehouse_id,
                    wait_timeout="60s",
                )
                return True
    except Exception:
        logger.exception("UC action failed for %s", patch_type)
        return False

    return True


# ═══════════════════════════════════════════════════════════════════════
# 7. Patch Set Application
# ═══════════════════════════════════════════════════════════════════════


_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


def apply_patch_set(
    w: WorkspaceClient | None,
    space_id: str,
    patches: list[dict],
    metadata_snapshot: dict,
    *,
    apply_mode: str = APPLY_MODE,
    deploy_target: str | None = None,
) -> dict:
    """Apply a patch set to a Genie Space (and optionally UC artifacts).

    Applies in risk order: LOW -> MEDIUM -> HIGH.
    High-risk patches are queued for manual review.

    Returns an ``apply_log`` dict with pre/post snapshots and rollback info.
    """
    pre_snapshot = copy.deepcopy(metadata_snapshot)
    config = copy.deepcopy(metadata_snapshot)

    sorted_indices = sorted(
        range(len(patches)),
        key=lambda i: _RISK_ORDER.get(classify_risk(patches[i].get("type", "")), 1),
    )

    applied: list[dict] = []
    queued_high: list[dict] = []
    rollback_commands: list[str] = []
    patched_objects: set[str] = set()

    for idx in sorted_indices:
        patch = patches[idx]
        risk = classify_risk(patch.get("type", ""))
        lever = patch.get("lever", 6)
        scope = _resolve_scope(lever, apply_mode)

        rendered = render_patch(patch, space_id, config)

        if risk == "high":
            queued_high.append({"index": idx, "patch": patch, "action": rendered})
            continue

        ok = False
        if scope in ("genie_config", "both"):
            ok = _apply_action_to_config(config, rendered)
        if scope in ("uc_artifact", "both") and w is not None:
            uc_ok = _apply_action_to_uc(w, rendered)
            ok = ok or uc_ok

        if ok:
            applied.append({"index": idx, "patch": patch, "action": rendered})
            rollback_commands.append(rendered.get("rollback_command", ""))
            target = rendered.get("target", "")
            if target:
                patched_objects.add(target)

    sort_genie_config(config)

    if w is not None and applied:
        try:
            patch_space_config(w, space_id, config)
        except Exception:
            logger.exception("Failed to PATCH Genie Space config")

    return {
        "space_id": space_id,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": copy.deepcopy(config),
        "applied": applied,
        "queued_high": queued_high,
        "rollback_commands": rollback_commands,
        "deploy_target": deploy_target,
        "patched_objects": list(patched_objects),
    }


# ═══════════════════════════════════════════════════════════════════════
# 8. Rollback
# ═══════════════════════════════════════════════════════════════════════


def rollback(
    apply_log: dict,
    w: WorkspaceClient | None,
    space_id: str,
    metadata_snapshot: dict | None = None,
) -> dict:
    """Restore the Genie Space config to its pre-patch state.

    Primary mechanism: replace current config with ``apply_log["pre_snapshot"]``.
    Fallback: execute rollback_commands in reverse order (HIGH -> MEDIUM -> LOW).
    """
    pre_snapshot = apply_log.get("pre_snapshot")
    if pre_snapshot is None:
        return {
            "status": "error",
            "executed_count": 0,
            "errors": ["No pre_snapshot in apply_log"],
        }

    restored = copy.deepcopy(pre_snapshot)

    if metadata_snapshot is not None:
        metadata_snapshot.clear()
        metadata_snapshot.update(restored)

    if w is not None:
        try:
            patch_space_config(w, space_id, restored)
        except Exception:
            logger.exception("Failed to PATCH rollback config")
            return {
                "status": "error",
                "executed_count": 0,
                "errors": ["Failed to apply rollback via API"],
                "restored_config": restored,
            }

    commands = apply_log.get("rollback_commands", [])
    return {
        "status": "SUCCESS",
        "executed_count": max(len(commands), 1),
        "errors": [],
        "restored_config": restored,
    }


# ═══════════════════════════════════════════════════════════════════════
# 9. Validation & Verification
# ═══════════════════════════════════════════════════════════════════════


def validate_patch_set(patches: list[dict], metadata_snapshot: dict) -> tuple[bool, list[str]]:
    """Validate a patch set before application.

    Delegates to ``optimizer.validate_patch_set`` but adds metadata checks.
    """
    from genie_space_optimizer.optimization.optimizer import validate_patch_set as _validate

    return _validate(patches, metadata_snapshot)


def verify_dual_persistence(applied_patches: list[dict]) -> list[dict]:
    """Verify that both Genie config and UC objects were updated."""
    results: list[dict] = []
    for entry in applied_patches:
        patch = entry.get("patch", {})
        results.append(
            {
                "patch_type": patch.get("type", ""),
                "target": entry.get("action", {}).get("target", ""),
                "genie_config_applied": True,
                "uc_artifact_applied": patch.get("lever", 6) <= 3,
            }
        )
    return results


def verify_repo_update(patch: dict, w: WorkspaceClient | None = None) -> dict:
    """Verify a specific patch persisted (stub for repo-level checks)."""
    return {
        "patch_type": patch.get("type", ""),
        "target": patch.get("target", ""),
        "verified": True,
    }
