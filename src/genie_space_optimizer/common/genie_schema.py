"""
Pydantic v2 models for the Genie Space ``serialized_space`` structure.

Used to validate config payloads **before** PATCH requests so that
malformed data never reaches the Databricks API.  All models use
``extra="allow"`` for forward-compatibility with new API fields.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

logger = logging.getLogger(__name__)


# ── Leaf-level models ─────────────────────────────────────────────────


class ColumnConfig(BaseModel):
    """A single column configuration within a table or metric view."""

    model_config = ConfigDict(extra="allow")

    column_name: str
    description: list[str] | None = None
    synonyms: list[str] | None = None
    enable_format_assistance: bool | None = None
    enable_entity_matching: bool | None = None
    exclude: bool | None = None
    get_example_values: bool | None = None
    build_value_dictionary: bool | None = None

    @field_validator("description", "synonyms", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        """Accept a bare string and wrap it in a list."""
        if isinstance(v, str):
            return [v]
        return v


class TableDataSource(BaseModel):
    """A table or metric view entry in ``data_sources``."""

    model_config = ConfigDict(extra="allow")

    identifier: str
    column_configs: list[ColumnConfig] | None = None
    description: list[str] | None = None

    @field_validator("description", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class JoinSpecSide(BaseModel):
    """One side (left / right) of a join specification."""

    model_config = ConfigDict(extra="allow")

    identifier: str
    alias: str


class JoinSpec(BaseModel):
    """A join specification linking two data sources."""

    model_config = ConfigDict(extra="allow")

    left: JoinSpecSide
    right: JoinSpecSide
    sql: list[str]
    id: str | None = None

    @field_validator("sql", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SampleQuestion(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    question: list[str]

    @field_validator("question", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


# ── Instructions sub-models ──────────────────────────────────────────


class TextInstruction(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    content: list[str]

    @field_validator("content", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class ParameterDefaultValue(BaseModel):
    model_config = ConfigDict(extra="allow")

    values: list[str]

    @field_validator("values", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class ExampleSqlParameter(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    type_hint: str | None = None
    default_value: ParameterDefaultValue | None = None

    @field_validator("default_value", mode="before")
    @classmethod
    def _coerce_default_value(cls, v: Any) -> Any:
        if isinstance(v, str):
            return {"values": [v]}
        return v


class ExampleQuestionSql(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    question: list[str]
    sql: list[str]
    parameters: list[ExampleSqlParameter] | None = None
    usage_guidance: list[str] | None = None

    @field_validator("question", "sql", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v

    @field_validator("usage_guidance", mode="before")
    @classmethod
    def _coerce_guidance(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlFunction(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    identifier: str


# ── Composite models ─────────────────────────────────────────────────


class Instructions(BaseModel):
    model_config = ConfigDict(extra="allow")

    text_instructions: list[TextInstruction] | None = None
    example_question_sqls: list[ExampleQuestionSql] | None = None
    sql_functions: list[SqlFunction] | None = None
    join_specs: list[JoinSpec] | None = None


class DataSources(BaseModel):
    model_config = ConfigDict(extra="allow")

    tables: list[TableDataSource] | None = None
    metric_views: list[TableDataSource] | None = None


class SpaceConfig(BaseModel):
    """The ``config`` block inside ``serialized_space``."""

    model_config = ConfigDict(extra="allow")

    sample_questions: list[SampleQuestion] | None = None


class SerializedSpace(BaseModel):
    """Top-level model for the ``serialized_space`` payload."""

    model_config = ConfigDict(extra="allow")

    version: int | None = None
    config: SpaceConfig | None = None
    data_sources: DataSources | None = None
    instructions: Instructions | None = None

    @model_validator(mode="after")
    def _check_has_data_sources(self) -> "SerializedSpace":
        if self.data_sources is None:
            raise ValueError("serialized_space must contain 'data_sources'")
        return self


# ── Public validation entry point ────────────────────────────────────


def validate_serialized_space(config: dict) -> tuple[bool, list[str]]:
    """Validate a config dict against the Genie ``serialized_space`` schema.

    Returns ``(True, [])`` on success or ``(False, [error, ...])`` on failure.
    Validation is lenient: unknown fields are allowed, bare strings are
    auto-coerced to ``list[str]`` where the API expects arrays.
    """
    errors: list[str] = []

    if not isinstance(config, dict):
        return False, [f"Expected dict, got {type(config).__name__}"]

    try:
        SerializedSpace.model_validate(config)
    except Exception as exc:
        for line in str(exc).split("\n"):
            line = line.strip()
            if line:
                errors.append(line)

    if errors:
        logger.warning("Genie config validation errors: %s", errors)
        return False, errors

    return True, []
