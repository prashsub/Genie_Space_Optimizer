"""
Pydantic v2 models for the Genie Space ``serialized_space`` structure.

Used to validate config payloads **before** PATCH requests so that
malformed data never reaches the Databricks API.  All models use
``extra="allow"`` for forward-compatibility with new API fields.

Validation modes
~~~~~~~~~~~~~~~~
* **lenient** (``strict=False``, default) — structural / type checks only.
  Safe for configs read from the API that may contain legacy data.
* **strict** (``strict=True``) — additionally enforces all official
  Databricks API rules: 32-char hex IDs, sort order, uniqueness
  constraints, size limits, and structural requirements.
  Used in ``patch_space_config()`` before sending to the API.

Reference: https://docs.databricks.com/aws/en/genie/conversation-api#validation-rules-for-serialized_space
"""

from __future__ import annotations

import datetime
import logging
import random
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

logger = logging.getLogger(__name__)

_HEX32_RE = re.compile(r"[0-9a-f]{32}")
_THREE_LEVEL_RE = re.compile(r"^[^.]+\.[^.]+\.[^.]+$")

MAX_STRING_LENGTH = 25_000
MAX_ARRAY_SIZE = 10_000
MAX_INSTRUCTION_SLOTS = 100


# ── ID generation utility ────────────────────────────────────────────


def generate_genie_id() -> str:
    """Generate a valid 32-char lowercase hex ID (time-ordered UUID).

    Follows the Databricks recommendation for Genie space IDs:
    time-ordered so that IDs generated in sequence sort alphabetically.
    """
    epoch = datetime.datetime(1582, 10, 15)
    t = int((datetime.datetime.now() - epoch).total_seconds() * 1e7)
    hi = (t & 0xFFFF_FFFF_FFFF_0000) | (1 << 12) | ((t & 0xFFFF) >> 4)
    lo = random.getrandbits(62) | 0x8000_0000_0000_0000
    return f"{hi:016x}{lo:016x}"


def ensure_join_spec_fields(spec: dict) -> dict:
    """Ensure a join spec dict has all required fields (alias, id).

    Mutates and returns the spec for convenience. Derives ``alias`` from
    the last segment of ``identifier`` when missing, and generates a new
    ``id`` when absent.
    """
    for side_key in ("left", "right"):
        side = spec.get(side_key)
        if isinstance(side, dict) and "identifier" in side and not side.get("alias"):
            side["alias"] = side["identifier"].rsplit(".", 1)[-1]
    if not spec.get("id"):
        spec["id"] = generate_genie_id()
    return spec


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
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "comment", "instruction", mode="before")
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
    description: list[str] | None = None
    default_value: ParameterDefaultValue | None = None

    @field_validator("description", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v

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


# ── SQL Snippet sub-models ───────────────────────────────────────────


class SqlSnippetFilter(BaseModel):
    """A reusable SQL filter snippet."""

    model_config = ConfigDict(extra="allow")

    id: str
    sql: list[str]
    display_name: str | None = None
    synonyms: list[str] | None = None
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "synonyms", "comment", "instruction", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlSnippetExpression(BaseModel):
    """A reusable SQL expression snippet."""

    model_config = ConfigDict(extra="allow")

    id: str
    alias: str
    sql: list[str]
    display_name: str | None = None
    synonyms: list[str] | None = None
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "synonyms", "comment", "instruction", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlSnippetMeasure(BaseModel):
    """A reusable SQL measure snippet."""

    model_config = ConfigDict(extra="allow")

    id: str
    alias: str
    sql: list[str]
    display_name: str | None = None
    synonyms: list[str] | None = None
    comment: list[str] | None = None
    instruction: list[str] | None = None

    @field_validator("sql", "synonyms", "comment", "instruction", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class SqlSnippets(BaseModel):
    """Container for reusable SQL snippets (filters, expressions, measures)."""

    model_config = ConfigDict(extra="allow")

    filters: list[SqlSnippetFilter] | None = None
    expressions: list[SqlSnippetExpression] | None = None
    measures: list[SqlSnippetMeasure] | None = None


# ── Benchmark sub-models ─────────────────────────────────────────────


class BenchmarkAnswer(BaseModel):
    """A ground-truth answer for a benchmark question (typically SQL)."""

    model_config = ConfigDict(extra="allow")

    format: str
    content: list[str]

    @field_validator("content", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class BenchmarkQuestion(BaseModel):
    """A single benchmark question entry in the Genie Space config."""

    model_config = ConfigDict(extra="allow")

    id: str | None = None
    question: list[str]
    answer: list[BenchmarkAnswer] | None = None

    @field_validator("question", mode="before")
    @classmethod
    def _coerce_str_to_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class Benchmarks(BaseModel):
    """The ``benchmarks`` block inside ``serialized_space``."""

    model_config = ConfigDict(extra="allow")

    questions: list[BenchmarkQuestion] | None = None


# ── Composite models ─────────────────────────────────────────────────


class Instructions(BaseModel):
    model_config = ConfigDict(extra="allow")

    text_instructions: list[TextInstruction] | None = None
    example_question_sqls: list[ExampleQuestionSql] | None = None
    sql_functions: list[SqlFunction] | None = None
    join_specs: list[JoinSpec] | None = None
    sql_snippets: SqlSnippets | None = None


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
    benchmarks: Benchmarks | None = None

    @model_validator(mode="after")
    def _check_has_data_sources(self) -> "SerializedSpace":
        if self.data_sources is None:
            raise ValueError("serialized_space must contain 'data_sources'")
        return self


# ── Strict validation helpers ────────────────────────────────────────


def _validate_id_format(id_value: str, location: str, errors: list[str]) -> None:
    """Check that an ID is a 32-char lowercase hex string."""
    if not _HEX32_RE.fullmatch(id_value):
        errors.append(
            f"{location}: ID '{id_value}' is not a valid 32-char lowercase hex string"
        )


def _check_sorted(
    items: list[Any],
    key_fn: Any,
    collection_name: str,
    errors: list[str],
) -> None:
    """Verify a list is sorted by the given key function."""
    keys = [key_fn(item) for item in items]
    for i in range(len(keys) - 1):
        if keys[i] > keys[i + 1]:
            errors.append(
                f"{collection_name} is not sorted: "
                f"'{keys[i]}' comes before '{keys[i + 1]}'"
            )
            return


def _check_string_lengths(obj: Any, path: str, errors: list[str]) -> None:
    """Recursively check that no individual string exceeds MAX_STRING_LENGTH."""
    if isinstance(obj, str):
        if len(obj) > MAX_STRING_LENGTH:
            errors.append(
                f"{path}: string length {len(obj)} exceeds limit {MAX_STRING_LENGTH}"
            )
    elif isinstance(obj, list):
        if len(obj) > MAX_ARRAY_SIZE:
            errors.append(
                f"{path}: array size {len(obj)} exceeds limit {MAX_ARRAY_SIZE}"
            )
        for i, item in enumerate(obj):
            _check_string_lengths(item, f"{path}[{i}]", errors)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _check_string_lengths(v, f"{path}.{k}", errors)


def count_instruction_slots(config: dict) -> int:
    """Count the number of Genie API instruction slots consumed by *config*.

    Per Databricks docs the limit is 100 slots total:
    - Each ``example_question_sqls`` entry = 1 slot
    - Each ``sql_functions`` entry = 1 slot
    - The entire ``text_instructions`` block = 1 slot (regardless of length)

    ``join_specs`` and ``sql_snippets`` do **not** count.
    """
    instructions = config.get("instructions") or {}
    text_count = min(len(instructions.get("text_instructions") or []), 1)
    example_count = len(instructions.get("example_question_sqls") or [])
    function_count = len(instructions.get("sql_functions") or [])
    return text_count + example_count + function_count


def _strict_validate(config: dict) -> list[str]:
    """Run all strict validation rules against a parsed config dict.

    Returns a list of error messages (empty = valid).
    """
    errors: list[str] = []

    # ── Version ──────────────────────────────────────────────────
    version = config.get("version")
    if version is None:
        errors.append("version: required field is missing")
    elif version not in (1, 2):
        errors.append(f"version: must be 1 or 2, got {version}")

    # ── Collect and validate IDs ─────────────────────────────────
    sample_q_ids: list[str] = []
    cfg = config.get("config") or {}
    for sq in cfg.get("sample_questions") or []:
        sid = sq.get("id", "")
        sample_q_ids.append(sid)
        _validate_id_format(sid, "config.sample_questions", errors)

    benchmark_q_ids: list[str] = []
    bm = config.get("benchmarks") or {}
    for bq in bm.get("questions") or []:
        bid = bq.get("id", "")
        if not bid:
            errors.append("benchmarks.questions: id is required")
        else:
            benchmark_q_ids.append(bid)
            _validate_id_format(bid, "benchmarks.questions", errors)

    instruction_ids: list[str] = []
    inst = config.get("instructions") or {}

    for ti in inst.get("text_instructions") or []:
        tid = ti.get("id", "")
        instruction_ids.append(tid)
        _validate_id_format(tid, "instructions.text_instructions", errors)

    for eq in inst.get("example_question_sqls") or []:
        eid = eq.get("id", "")
        instruction_ids.append(eid)
        _validate_id_format(eid, "instructions.example_question_sqls", errors)

    for sf in inst.get("sql_functions") or []:
        sfid = sf.get("id", "")
        instruction_ids.append(sfid)
        _validate_id_format(sfid, "instructions.sql_functions", errors)

    for js in inst.get("join_specs") or []:
        jsid = js.get("id", "")
        if not jsid:
            errors.append("instructions.join_specs: id is required")
        else:
            instruction_ids.append(jsid)
            _validate_id_format(jsid, "instructions.join_specs", errors)

    snippets = inst.get("sql_snippets") or {}
    for snippet_type in ("filters", "expressions", "measures"):
        for item in snippets.get(snippet_type) or []:
            snip_id = item.get("id", "")
            instruction_ids.append(snip_id)
            _validate_id_format(
                snip_id, f"instructions.sql_snippets.{snippet_type}", errors
            )
            sql_val = item.get("sql", [])
            if not sql_val or (isinstance(sql_val, list) and all(not s for s in sql_val)):
                errors.append(
                    f"instructions.sql_snippets.{snippet_type}: sql must not be empty"
                )

    # ── Uniqueness: question IDs ─────────────────────────────────
    all_question_ids = sample_q_ids + benchmark_q_ids
    seen_q: set[str] = set()
    for qid in all_question_ids:
        if qid in seen_q:
            errors.append(f"Duplicate question ID across sample_questions/benchmarks: '{qid}'")
        seen_q.add(qid)

    # ── Uniqueness: instruction IDs ──────────────────────────────
    seen_i: set[str] = set()
    for iid in instruction_ids:
        if iid in seen_i:
            errors.append(f"Duplicate instruction ID: '{iid}'")
        seen_i.add(iid)

    # ── Uniqueness: (table_identifier, column_name) ──────────────
    ds = config.get("data_sources") or {}
    col_tuples: set[tuple[str, str]] = set()
    for source_key in ("tables", "metric_views"):
        for tbl in ds.get(source_key) or []:
            tbl_id = tbl.get("identifier", "")
            for cc in tbl.get("column_configs") or []:
                col_name = cc.get("column_name", "")
                key = (tbl_id, col_name)
                if key in col_tuples:
                    errors.append(
                        f"Duplicate column config: ({tbl_id}, {col_name})"
                    )
                col_tuples.add(key)

    # ── Sorting ──────────────────────────────────────────────────
    for source_key in ("tables", "metric_views"):
        items = ds.get(source_key) or []
        _check_sorted(
            items,
            lambda x: x.get("identifier", ""),
            f"data_sources.{source_key}",
            errors,
        )
        for tbl in items:
            ccs = tbl.get("column_configs") or []
            if ccs:
                tbl_id = tbl.get("identifier", "")
                _check_sorted(
                    ccs,
                    lambda x: x.get("column_name", ""),
                    f"data_sources.{source_key}[{tbl_id}].column_configs",
                    errors,
                )

    sqs = cfg.get("sample_questions") or []
    _check_sorted(sqs, lambda x: x.get("id", ""), "config.sample_questions", errors)

    for key in ("text_instructions", "example_question_sqls", "join_specs"):
        items = inst.get(key) or []
        _check_sorted(items, lambda x: x.get("id", ""), f"instructions.{key}", errors)

    sf_items = inst.get("sql_functions") or []
    _check_sorted(
        sf_items,
        lambda x: (x.get("id", ""), x.get("identifier", "")),
        "instructions.sql_functions",
        errors,
    )

    for snippet_type in ("filters", "expressions", "measures"):
        items = snippets.get(snippet_type) or []
        _check_sorted(
            items,
            lambda x: x.get("id", ""),
            f"instructions.sql_snippets.{snippet_type}",
            errors,
        )

    bm_questions = bm.get("questions") or []
    _check_sorted(
        bm_questions, lambda x: x.get("id", ""), "benchmarks.questions", errors
    )

    # ── Text instructions limit ──────────────────────────────────
    ti_list = inst.get("text_instructions") or []
    if len(ti_list) > 1:
        errors.append(
            f"instructions.text_instructions: at most 1 allowed, got {len(ti_list)}"
        )

    # ── Instruction slot budget (100 total) ──────────────────────
    slot_count = count_instruction_slots(config)
    if slot_count > MAX_INSTRUCTION_SLOTS:
        _text_ct = min(len(ti_list), 1)
        _eq_ct = len(inst.get("example_question_sqls") or [])
        _fn_ct = len(inst.get("sql_functions") or [])
        errors.append(
            f"Instruction slot budget exceeded: {slot_count}/{MAX_INSTRUCTION_SLOTS} "
            f"(example_sqls={_eq_ct}, sql_functions={_fn_ct}, "
            f"text_instructions={_text_ct})"
        )

    # ── Table identifier format ──────────────────────────────────
    for source_key in ("tables", "metric_views"):
        for tbl in ds.get(source_key) or []:
            ident = tbl.get("identifier", "")
            if not _THREE_LEVEL_RE.match(ident):
                errors.append(
                    f"data_sources.{source_key}: identifier '{ident}' "
                    "must use three-level namespace (catalog.schema.table)"
                )

    # ── Benchmark answer validation ──────────────────────────────
    for bq in bm.get("questions") or []:
        answers = bq.get("answer") or []
        q_text = (bq.get("question") or ["?"])[0][:40]
        if len(answers) != 1:
            errors.append(
                f"benchmarks.questions['{q_text}...']: "
                f"must have exactly 1 answer, got {len(answers)}"
            )
        elif answers[0].get("format") != "SQL":
            errors.append(
                f"benchmarks.questions['{q_text}...']: "
                f"answer format must be 'SQL', got '{answers[0].get('format')}'"
            )

    # ── Size / length limits ─────────────────────────────────────
    _check_string_lengths(config, "root", errors)

    return errors


# ── Public validation entry point ────────────────────────────────────


def validate_serialized_space(
    config: dict,
    *,
    strict: bool = False,
) -> tuple[bool, list[str]]:
    """Validate a config dict against the Genie ``serialized_space`` schema.

    Parameters
    ----------
    config:
        The parsed ``serialized_space`` dict.
    strict:
        When True, enforces all official Databricks API rules (ID format,
        sorting, uniqueness, size limits).  When False (default), only
        structural / type checks are performed.

    Returns ``(True, [])`` on success or ``(False, [error, ...])`` on failure.
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

    if strict:
        errors.extend(_strict_validate(config))

    if errors:
        logger.warning("Genie config validation errors: %s", errors)
        return False, errors

    return True, []
