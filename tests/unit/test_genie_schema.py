"""Unit tests for genie_space_optimizer.common.genie_schema — config validation."""

from __future__ import annotations

import json
import os

import pytest

from genie_space_optimizer.common.genie_schema import (
    ColumnConfig,
    ExampleQuestionSql,
    JoinSpec,
    SerializedSpace,
    TextInstruction,
    validate_serialized_space,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _minimal_valid_config() -> dict:
    """Smallest config that passes validation."""
    return {"data_sources": {"tables": []}}


def _reference_config() -> dict | None:
    """Load the reference config from docs/ if available."""
    path = os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "genie_space_config.json"
    )
    if not os.path.exists(path):
        return None
    with open(path) as f:
        full = json.load(f)
    return full.get("serialized_space", full)


# ═══════════════════════════════════════════════════════════════════════
# validate_serialized_space — full config
# ═══════════════════════════════════════════════════════════════════════


class TestValidateSerializedSpace:
    def test_minimal_valid(self):
        ok, errors = validate_serialized_space(_minimal_valid_config())
        assert ok is True
        assert errors == []

    def test_reference_config_passes(self):
        ref = _reference_config()
        if ref is None:
            pytest.skip("Reference config not found at docs/genie_space_config.json")
        ok, errors = validate_serialized_space(ref)
        assert ok is True, f"Reference config failed: {errors}"

    def test_not_a_dict(self):
        ok, errors = validate_serialized_space("a string")
        assert ok is False
        assert any("dict" in e.lower() or "str" in e.lower() for e in errors)

    def test_missing_data_sources(self):
        ok, errors = validate_serialized_space({"version": 2})
        assert ok is False
        assert any("data_sources" in e for e in errors)

    def test_extra_unknown_fields_allowed(self):
        config = _minimal_valid_config()
        config["unknown_future_field"] = True
        config["another_field"] = {"nested": "value"}
        ok, errors = validate_serialized_space(config)
        assert ok is True

    def test_empty_data_sources(self):
        ok, errors = validate_serialized_space({"data_sources": {}})
        assert ok is True

    def test_full_structure_with_all_sections(self):
        config = {
            "version": 2,
            "config": {
                "sample_questions": [
                    {"id": "q1", "question": ["How much revenue?"]}
                ]
            },
            "data_sources": {
                "tables": [
                    {
                        "identifier": "catalog.schema.table",
                        "column_configs": [
                            {
                                "column_name": "revenue",
                                "description": ["Total revenue"],
                                "synonyms": ["sales"],
                                "enable_format_assistance": True,
                            }
                        ],
                    }
                ],
                "metric_views": [
                    {"identifier": "catalog.schema.mv1", "column_configs": []}
                ],
            },
            "instructions": {
                "text_instructions": [
                    {"id": "ti1", "content": ["Use net revenue."]}
                ],
                "example_question_sqls": [
                    {
                        "id": "eq1",
                        "question": ["Top 10 stores?"],
                        "sql": ["SELECT * FROM t LIMIT 10"],
                        "parameters": [
                            {
                                "name": "limit_val",
                                "type_hint": "INTEGER",
                                "default_value": {"values": ["10"]},
                            }
                        ],
                        "usage_guidance": ["Use for top-N queries"],
                    }
                ],
                "sql_functions": [
                    {"id": "sf1", "identifier": "catalog.schema.get_trend"}
                ],
                "join_specs": [
                    {
                        "id": "js1",
                        "left": {"identifier": "catalog.schema.fact", "alias": "fact"},
                        "right": {"identifier": "catalog.schema.dim", "alias": "dim"},
                        "sql": ["`fact`.`key` = `dim`.`key`"],
                    }
                ],
            },
        }
        ok, errors = validate_serialized_space(config)
        assert ok is True, f"Full config failed: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# Column configs
# ═══════════════════════════════════════════════════════════════════════


class TestColumnConfig:
    def test_valid(self):
        cc = ColumnConfig(column_name="revenue")
        assert cc.column_name == "revenue"

    def test_missing_column_name(self):
        with pytest.raises(Exception):
            ColumnConfig.model_validate({"description": ["test"]})

    def test_description_str_coerced_to_list(self):
        cc = ColumnConfig.model_validate(
            {"column_name": "x", "description": "plain string"}
        )
        assert cc.description == ["plain string"]

    def test_synonyms_str_coerced_to_list(self):
        cc = ColumnConfig.model_validate(
            {"column_name": "x", "synonyms": "alias"}
        )
        assert cc.synonyms == ["alias"]

    def test_boolean_flags(self):
        cc = ColumnConfig.model_validate(
            {
                "column_name": "x",
                "enable_format_assistance": True,
                "enable_entity_matching": False,
                "exclude": True,
            }
        )
        assert cc.enable_format_assistance is True
        assert cc.enable_entity_matching is False
        assert cc.exclude is True


# ═══════════════════════════════════════════════════════════════════════
# Join specs
# ═══════════════════════════════════════════════════════════════════════


class TestJoinSpec:
    def test_valid(self):
        js = JoinSpec.model_validate(
            {
                "left": {"identifier": "a.b.c", "alias": "c"},
                "right": {"identifier": "a.b.d", "alias": "d"},
                "sql": ["`c`.`id` = `d`.`id`"],
            }
        )
        assert js.left.identifier == "a.b.c"

    def test_missing_left(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "right": {"identifier": "a.b.d", "alias": "d"},
                    "sql": ["`c`.`id` = `d`.`id`"],
                }
            )

    def test_missing_right(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "left": {"identifier": "a.b.c", "alias": "c"},
                    "sql": ["`c`.`id` = `d`.`id`"],
                }
            )

    def test_missing_sql(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "left": {"identifier": "a.b.c", "alias": "c"},
                    "right": {"identifier": "a.b.d", "alias": "d"},
                }
            )

    def test_sql_str_coerced_to_list(self):
        js = JoinSpec.model_validate(
            {
                "left": {"identifier": "a.b.c", "alias": "c"},
                "right": {"identifier": "a.b.d", "alias": "d"},
                "sql": "c.id = d.id",
            }
        )
        assert js.sql == ["c.id = d.id"]

    def test_id_optional(self):
        js = JoinSpec.model_validate(
            {
                "left": {"identifier": "a.b.c", "alias": "c"},
                "right": {"identifier": "a.b.d", "alias": "d"},
                "sql": ["c.id = d.id"],
            }
        )
        assert js.id is None

    def test_left_missing_alias(self):
        with pytest.raises(Exception):
            JoinSpec.model_validate(
                {
                    "left": {"identifier": "a.b.c"},
                    "right": {"identifier": "a.b.d", "alias": "d"},
                    "sql": ["c.id = d.id"],
                }
            )


# ═══════════════════════════════════════════════════════════════════════
# Example question SQLs
# ═══════════════════════════════════════════════════════════════════════


class TestExampleQuestionSql:
    def test_valid_minimal(self):
        eq = ExampleQuestionSql.model_validate(
            {"id": "eq1", "question": ["Top stores?"], "sql": ["SELECT * FROM t"]}
        )
        assert eq.id == "eq1"
        assert eq.parameters is None

    def test_missing_id(self):
        with pytest.raises(Exception):
            ExampleQuestionSql.model_validate(
                {"question": ["Q?"], "sql": ["SELECT 1"]}
            )

    def test_missing_question(self):
        with pytest.raises(Exception):
            ExampleQuestionSql.model_validate(
                {"id": "eq1", "sql": ["SELECT 1"]}
            )

    def test_missing_sql(self):
        with pytest.raises(Exception):
            ExampleQuestionSql.model_validate(
                {"id": "eq1", "question": ["Q?"]}
            )

    def test_str_coercion(self):
        eq = ExampleQuestionSql.model_validate(
            {"id": "eq1", "question": "bare string", "sql": "SELECT 1"}
        )
        assert eq.question == ["bare string"]
        assert eq.sql == ["SELECT 1"]

    def test_with_parameters(self):
        eq = ExampleQuestionSql.model_validate(
            {
                "id": "eq1",
                "question": ["Q?"],
                "sql": ["SELECT * WHERE x = :v"],
                "parameters": [
                    {
                        "name": "v",
                        "type_hint": "INTEGER",
                        "default_value": {"values": ["10"]},
                    }
                ],
            }
        )
        assert len(eq.parameters) == 1
        assert eq.parameters[0].name == "v"
        assert eq.parameters[0].default_value.values == ["10"]

    def test_usage_guidance_str_coerced(self):
        eq = ExampleQuestionSql.model_validate(
            {
                "id": "eq1",
                "question": ["Q?"],
                "sql": ["SELECT 1"],
                "usage_guidance": "when to use this",
            }
        )
        assert eq.usage_guidance == ["when to use this"]


# ═══════════════════════════════════════════════════════════════════════
# Text instructions
# ═══════════════════════════════════════════════════════════════════════


class TestTextInstruction:
    def test_valid(self):
        ti = TextInstruction.model_validate(
            {"id": "ti1", "content": ["Instruction text"]}
        )
        assert ti.content == ["Instruction text"]

    def test_missing_id(self):
        with pytest.raises(Exception):
            TextInstruction.model_validate({"content": ["text"]})

    def test_missing_content(self):
        with pytest.raises(Exception):
            TextInstruction.model_validate({"id": "ti1"})

    def test_content_str_coerced(self):
        ti = TextInstruction.model_validate(
            {"id": "ti1", "content": "plain string"}
        )
        assert ti.content == ["plain string"]


# ═══════════════════════════════════════════════════════════════════════
# Type mismatches caught at config level
# ═══════════════════════════════════════════════════════════════════════


class TestTypeMismatches:
    def test_column_config_wrong_column_name_type(self):
        ok, errors = validate_serialized_space(
            {
                "data_sources": {
                    "tables": [
                        {
                            "identifier": "t",
                            "column_configs": [{"column_name": 123}],
                        }
                    ]
                }
            }
        )
        assert ok is False

    def test_table_missing_identifier(self):
        ok, errors = validate_serialized_space(
            {"data_sources": {"tables": [{"column_configs": []}]}}
        )
        assert ok is False

    def test_join_spec_left_not_dict(self):
        ok, errors = validate_serialized_space(
            {
                "data_sources": {"tables": []},
                "instructions": {
                    "join_specs": [
                        {"left": "not_a_dict", "right": {"identifier": "x", "alias": "x"}, "sql": ["a"]}
                    ]
                },
            }
        )
        assert ok is False

    def test_sql_function_missing_identifier(self):
        ok, errors = validate_serialized_space(
            {
                "data_sources": {"tables": []},
                "instructions": {"sql_functions": [{"id": "sf1"}]},
            }
        )
        assert ok is False
