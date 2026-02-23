"""Unit tests for genie_space_optimizer.optimization.applier — pure functions."""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.applier import (
    _apply_action_to_config,
    classify_risk,
    render_patch,
    validate_patch_set,
)


class TestClassifyRisk:
    def test_low_risk(self):
        assert classify_risk("add_description") == "low"
        assert classify_risk("add_column_description") == "low"

    def test_medium_risk(self):
        assert classify_risk("update_instruction") == "medium"

    def test_dict_input(self):
        assert classify_risk({"type": "add_description"}) == "low"

    def test_unknown_defaults_to_medium(self):
        assert classify_risk("totally_unknown") == "medium"


class TestRenderPatch:
    def test_add_instruction(self):
        patch = {"type": "add_instruction", "new_text": "Always use UTC."}
        result = render_patch(patch, "space-1", {})
        assert result["action_type"] == "add_instruction"
        cmd = json.loads(result["command"])
        assert cmd["op"] == "add"
        assert cmd["section"] == "instructions"
        assert cmd["new_text"] == "Always use UTC."

        rollback = json.loads(result["rollback_command"])
        assert rollback["op"] == "remove"

    def test_update_instruction(self):
        patch = {"type": "update_instruction", "old_text": "old", "new_text": "new"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "update"
        assert cmd["old_text"] == "old"
        assert cmd["new_text"] == "new"

    def test_add_description(self):
        patch = {"type": "add_description", "target": "orders", "new_text": "Order data table"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "add"
        assert cmd["section"] == "descriptions"

    def test_hide_column(self):
        patch = {"type": "hide_column", "table": "orders", "column": "internal_id"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["visible"] is False

    def test_add_join_spec(self):
        join_spec = {"left_table_name": "orders", "right_table_name": "customers"}
        patch = {"type": "add_join_spec", "join_spec": join_spec}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "add"
        assert cmd["section"] == "join_specs"

    def test_unknown_type_returns_unknown_op(self):
        patch = {"type": "future_patch_type", "target": "x"}
        result = render_patch(patch, "space-1", {})
        cmd = json.loads(result["command"])
        assert cmd["op"] == "unknown"

    def test_render_has_risk_level(self):
        patch = {"type": "add_instruction", "new_text": "test"}
        result = render_patch(patch, "space-1", {})
        assert "risk_level" in result


class TestApplyActionToConfig:
    def _make_config(self):
        return {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "catalog.schema.orders",
                        "description": ["Order data"],
                        "column_configs": [
                            {"column_name": "order_id", "description": ["PK"]},
                            {"column_name": "amount", "description": ["Amount"]},
                        ],
                    }
                ]
            },
            "instructions": {
                "text_instructions": [
                    {"id": "instr_1", "content": ["Filter by active orders."]}
                ],
                "example_question_sqls": [],
            },
        }

    def test_add_instruction(self):
        config = self._make_config()
        action = {
            "command": json.dumps({"op": "add", "section": "instructions", "new_text": "Use UTC."}),
        }
        assert _apply_action_to_config(config, action) is True
        instructions = config["instructions"]["text_instructions"][0]["content"]
        assert any("UTC" in line for line in instructions)

    def test_update_instruction_with_guard(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "update",
                "section": "instructions",
                "old_text": "Filter by active orders.",
                "new_text": "Filter by status = active.",
            }),
        }
        assert _apply_action_to_config(config, action) is True

    def test_update_instruction_guard_fails(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "update",
                "section": "instructions",
                "old_text": "This text does not exist",
                "new_text": "replacement",
            }),
        }
        assert _apply_action_to_config(config, action) is False

    def test_add_column_description(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "add",
                "section": "column_configs",
                "table": "catalog.schema.orders",
                "column": "amount",
                "value": "Monetary value in USD",
            }),
        }
        assert _apply_action_to_config(config, action) is True

    def test_column_config_table_not_found(self):
        config = self._make_config()
        action = {
            "command": json.dumps({
                "op": "add",
                "section": "column_configs",
                "table": "nonexistent_table",
                "column": "col",
                "value": "desc",
            }),
        }
        assert _apply_action_to_config(config, action) is False

    def test_add_join_spec(self):
        config = self._make_config()
        js = {"left_table_name": "orders", "right_table_name": "items", "join_type": "inner"}
        action = {
            "command": json.dumps({"op": "add", "section": "join_specs", "join_spec": js}),
        }
        assert _apply_action_to_config(config, action) is True
        assert len(config["data_sources"]["join_specs"]) == 1

    def test_invalid_json_command(self):
        config = self._make_config()
        action = {"command": "not valid json"}
        assert _apply_action_to_config(config, action) is False


class TestValidatePatchSet:
    def test_delegates_to_optimizer(self):
        valid, errors = validate_patch_set([], {})
        assert valid is True
        assert errors == []
