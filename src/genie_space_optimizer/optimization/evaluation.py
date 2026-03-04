"""
Evaluation engine — predict function, shared helpers, MLflow integration,
and benchmark generation.

The central module for the quality measurement system. Provides:
  - ``make_predict_fn()``: factory closure binding workspace/spark context
  - Shared helpers used by all 8 scorers
  - ``run_evaluation()``: wraps ``mlflow.genai.evaluate()``
  - ``generate_benchmarks()``: LLM-powered benchmark creation
  - ``load_benchmarks_from_dataset()``: read from UC eval dataset
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import traceback
from difflib import get_close_matches
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Union

import mlflow
import pandas as pd
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from mlflow.entities import AssessmentSource, Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.config import (
    ASI_SCHEMA,
    BENCHMARK_CATEGORIES,
    BENCHMARK_CORRECTION_PROMPT,
    BENCHMARK_COVERAGE_GAP_PROMPT,
    BENCHMARK_GENERATION_PROMPT,
    BENCHMARK_PROMPTS,
    CODE_SOURCE_ID,
    COVERAGE_GAP_SOFT_CAP_FACTOR,
    DEFAULT_THRESHOLDS,
    FAILURE_TAXONOMY,
    INSTRUCTION_PROMPT_ALIAS,
    INSTRUCTION_PROMPT_NAME_TEMPLATE,
    JUDGE_PROMPTS,
    LEVER_PROMPTS,
    LLM_ENDPOINT,
    LLM_MAX_RETRIES,
    LLM_SOURCE_ID_TEMPLATE,
    LLM_TEMPERATURE,
    MLFLOW_THRESHOLDS,
    MODEL_NAME_TEMPLATE,
    PROMPT_ALIAS,
    PROMPT_NAME_TEMPLATE,
    RATE_LIMIT_SECONDS,
    RUN_NAME_TEMPLATE,
    TARGET_BENCHMARK_COUNT,
    TEMPLATE_VARIABLES,
    format_mlflow_template,
)
from genie_space_optimizer.common.genie_client import (
    detect_asset_type,
    fetch_genie_result_df,
    resolve_sql,
    run_genie_query,
    sanitize_sql,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

CODE_SOURCE = AssessmentSource(source_type="CODE", source_id=CODE_SOURCE_ID)
LLM_SOURCE = AssessmentSource(
    source_type="LLM_JUDGE",
    source_id=format_mlflow_template(LLM_SOURCE_ID_TEMPLATE, endpoint=LLM_ENDPOINT),
)

_SCORER_FEEDBACK_CACHE: dict[tuple[str, str], dict] = {}

_REGISTERED_PROMPT_NAMES: dict[str, str] = {}


def _cache_scorer_feedback(
    question_id: str, judge_name: str, rationale: str, metadata: dict | None = None
) -> None:
    """Store scorer feedback for later merge into rows_for_output.

    Called by scorers via ``format_asi_markdown`` so that rationale and
    metadata survive even when MLflow's eval_results table drops them.
    """
    _SCORER_FEEDBACK_CACHE[(question_id, judge_name)] = {
        "rationale": rationale,
        "metadata": metadata or {},
    }


def _drain_scorer_feedback_cache() -> dict[str, dict[str, dict]]:
    """Return and clear all cached feedback, keyed by question_id then judge."""
    by_question: dict[str, dict[str, dict]] = {}
    for (qid, judge), data in _SCORER_FEEDBACK_CACHE.items():
        by_question.setdefault(qid, {})[judge] = data
    _SCORER_FEEDBACK_CACHE.clear()
    return by_question


EVAL_SCOPES = {"full", "slice", "p0", "held_out"}
EVAL_DEBUG = os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_DEBUG", "true").lower() in {"1", "true", "yes", "on"}
EVAL_MAX_ATTEMPTS = int(os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS", "4"))
EVAL_RETRY_SLEEP_SECONDS = int(os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_RETRY_SLEEP_SECONDS", "10"))
EVAL_SINGLE_WORKER_FALLBACK = os.getenv("GENIE_SPACE_OPTIMIZER_EVAL_RETRY_WORKERS", "1")
STRICT_PROMPT_REGISTRATION = (
    os.getenv("GENIE_SPACE_OPTIMIZER_STRICT_PROMPT_REGISTRATION", "true").lower()
    in {"1", "true", "yes", "on"}
)
FAIL_ON_INFRA_EVAL_ERRORS = (
    os.getenv("GENIE_SPACE_OPTIMIZER_FAIL_ON_INFRA_EVAL_ERRORS", "true").lower()
    in {"1", "true", "yes", "on"}
)


# ── Shared Helpers ──────────────────────────────────────────────────────

_CMP_BULKY_KEYS = frozenset({"gt_sample", "genie_sample", "gt_signature", "genie_signature"})


def slim_comparison(cmp: dict) -> dict:
    """Return a lightweight copy of a comparison dict for use in assessments.

    Strips bulky keys (result samples, signatures) to keep MLflow
    trace/assessment payloads well within size limits.
    """
    return {k: v for k, v in cmp.items() if k not in _CMP_BULKY_KEYS}


def build_temporal_note(cmp: dict) -> str:
    """Build a prompt note explaining temporal date rewriting, if applicable."""
    tr = cmp.get("temporal_rewrite")
    if not tr:
        return ""
    return (
        "\nTEMPORAL CONTEXT: The question uses a relative time reference "
        f"('{tr['keyword']}'). The GT SQL dates were auto-adjusted from "
        f"{tr['original_dates']} to {tr['rewritten_dates']} to match the "
        "current date. If there are still minor date differences between "
        "GT and Genie, evaluate whether Genie's date interpretation is "
        "reasonable for the temporal reference in the question.\n"
    )


def _extract_response_text(outputs: Union[dict, Any]) -> str:
    """Extract response text from mlflow.genai.evaluate() serialized format."""
    if isinstance(outputs, str):
        return outputs
    if isinstance(outputs, dict):
        if "response" in outputs:
            return outputs["response"]
        if "output" in outputs:
            output_list = outputs["output"]
            if output_list and len(output_list) > 0:
                item = output_list[0]
                if "content" in item and item["content"]:
                    return item["content"][0].get("text", "")
    return ""


def _extract_json(content: str) -> dict:
    """Extract a JSON object from LLM response text that may contain non-JSON wrapping.

    Handles common LLM output patterns:
    - Pure JSON
    - JSON wrapped in markdown code fences
    - JSON preceded/followed by prose ("Here are my suggestions: {...}")
    - Multiple JSON objects (takes first)
    """
    content = content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        content = content.rsplit("```", 1)[0].strip()

    _saved_err: json.JSONDecodeError | None = None

    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        _saved_err = exc

    if _saved_err is not None and hasattr(_saved_err, "pos") and _saved_err.msg.startswith("Extra data"):
        try:
            return json.loads(content[: _saved_err.pos])
        except json.JSONDecodeError:
            pass

    match = re.search(r"\{.*\}", content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise _saved_err


def get_registered_prompt_name(judge_name: str) -> str:
    """Return the registered prompt name for a judge/lever, or empty string."""
    return _REGISTERED_PROMPT_NAMES.get(judge_name, "")


def _link_prompt_to_trace(prompt_name: str) -> None:
    """Load a registered prompt inside the current trace to link it.

    MLflow automatically associates ``load_prompt()`` calls with the
    active trace, making the prompt version visible in the Linked Prompts
    tab of the trace UI.  Failures are silently ignored so scoring continues.
    """
    if not prompt_name:
        return
    try:
        mlflow.genai.load_prompt(f"prompts:/{prompt_name}@{PROMPT_ALIAS}")
    except Exception:
        try:
            mlflow.genai.load_prompt(f"prompts:/{prompt_name}@latest")
        except Exception:
            logger.debug("Could not load prompt '%s' for trace linking", prompt_name)


def _call_llm_for_scoring(
    w: WorkspaceClient,
    prompt: str,
    max_retries: int = LLM_MAX_RETRIES,
    prompt_name: str = "",
) -> dict:
    """Call LLM using Databricks SDK with retry + exponential backoff.

    If *prompt_name* is provided, loads the registered prompt first to
    link it to the current MLflow trace (visible in Linked Prompts tab).
    """
    _link_prompt_to_trace(prompt_name)

    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = w.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
                temperature=LLM_TEMPERATURE,
            )
            choices = getattr(response, "choices", None) or []
            if not choices:
                raise ValueError(f"Empty LLM response choices on attempt {attempt + 1}")
            first_choice = choices[0]
            message = getattr(first_choice, "message", None)
            content = getattr(message, "content", None)
            if not content or not content.strip():
                raise ValueError(f"Empty LLM response on attempt {attempt + 1}")
            return _extract_json(content)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
    raise last_err  # type: ignore[misc]


def _rewrite_measure_refs(
    sql: str,
    metric_view_measures: dict[str, set[str]],
) -> str:
    """Wrap bare metric view measure names in ORDER BY with MEASURE().

    Only applies when the SQL references a metric view in its FROM clause.
    ``metric_view_measures`` maps lowercased short table names to sets of
    lowercased measure column names.
    """
    if not metric_view_measures or not sql:
        return sql

    from_tables: list[str] = []
    for m in re.finditer(r"\bFROM\s+([\w.`]+)", sql, re.IGNORECASE):
        from_tables.append(m.group(1).replace("`", "").split(".")[-1].lower())

    relevant_measures: set[str] = set()
    for tbl in from_tables:
        if tbl in metric_view_measures:
            relevant_measures |= metric_view_measures[tbl]

    if not relevant_measures:
        return sql

    order_match = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    if not order_match:
        return sql

    prefix = sql[: order_match.end()]
    tail = sql[order_match.end() :]

    def _wrap(m: re.Match) -> str:
        col = m.group(1)
        if col.lower() in relevant_measures:
            return f"MEASURE({col})"
        return col

    already_measured = re.compile(r"\bMEASURE\s*\(", re.IGNORECASE)
    rewritten_tail = re.sub(
        r"\b([A-Za-z_]\w*)\b(?!\s*\()",
        lambda m: m.group(0) if already_measured.search(sql[max(0, order_match.end() + m.start() - 10) : order_match.end() + m.start()]) else _wrap(m),
        tail,
    )
    return prefix + rewritten_tail


def build_metric_view_measures(config: dict) -> dict[str, set[str]]:
    """Build {lowered_short_name: {measure_col, ...}} from the parsed Genie config."""
    parsed = config.get("_parsed_space", config)
    ds = parsed.get("data_sources", {})
    if not isinstance(ds, dict):
        return {}
    mvs = ds.get("metric_views", [])
    result: dict[str, set[str]] = {}
    for mv in mvs:
        identifier = mv.get("identifier", "")
        short_name = identifier.split(".")[-1].lower() if identifier else ""
        if not short_name:
            continue
        measures: set[str] = set()
        for cc in mv.get("column_configs", []):
            col_name = cc.get("column_name", "")
            if not col_name:
                continue
            col_type = str(cc.get("column_type", "")).lower()
            if col_type == "measure" or cc.get("is_measure"):
                measures.add(col_name.lower())
        if measures:
            result[short_name] = measures
    return result


@dataclass(frozen=True)
class TemporalIntent:
    """Detected temporal intent from a question's relative time reference."""
    keyword: str
    start_date: date
    end_date: date


_TEMPORAL_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bthis\s+year\b", re.I), "this_year"),
    (re.compile(r"\bytd\b|\byear[\s-]to[\s-]date\b", re.I), "ytd"),
    (re.compile(r"\bthis\s+month\b", re.I), "this_month"),
    (re.compile(r"\bthis\s+quarter\b", re.I), "this_quarter"),
    (re.compile(r"\blast\s+quarter\b", re.I), "last_quarter"),
    (re.compile(r"\blast\s+year\b", re.I), "last_year"),
    (re.compile(r"\blast\s+(\d+)\s+months?\b", re.I), "last_n_months"),
    (re.compile(r"\blast\s+(\d+)\s+days?\b", re.I), "last_n_days"),
]

_DATE_LITERAL_RE = re.compile(r"'(\d{4}-\d{2}-\d{2})'")
_EXPLICIT_YEAR_RE = re.compile(r"\bfor\s+(\d{4})\b|\bin\s+(\d{4})\b|\byear\s+(\d{4})\b", re.I)


def _quarter_start(d: date) -> date:
    """Return the first day of the quarter containing *d*."""
    return date(d.year, ((d.month - 1) // 3) * 3 + 1, 1)


def _month_offset(d: date, months: int) -> date:
    """Shift *d* by *months* (positive or negative), clamping the day."""
    m = d.month + months
    y = d.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    import calendar
    max_day = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, max_day))


def _detect_temporal_intent(
    question: str,
    *,
    today: date | None = None,
) -> TemporalIntent | None:
    """Detect relative temporal references in *question* and compute a date range.

    Returns ``None`` when the question has no relative time phrase or when
    an explicit year is mentioned (e.g. "for 2025").
    """
    if not question:
        return None
    today = today or date.today()

    for pat, keyword in _TEMPORAL_PATTERNS:
        m = pat.search(question)
        if not m:
            continue

        if keyword == "this_year" or keyword == "ytd":
            start = date(today.year, 1, 1)
            end = today
        elif keyword == "this_month":
            start = date(today.year, today.month, 1)
            end = today
        elif keyword == "this_quarter":
            start = _quarter_start(today)
            end = today
        elif keyword == "last_quarter":
            qs = _quarter_start(today)
            end = qs - timedelta(days=1)
            start = _quarter_start(end)
        elif keyword == "last_year":
            start = date(today.year - 1, 1, 1)
            end = date(today.year - 1, 12, 31)
        elif keyword == "last_n_months":
            n = int(m.group(1))
            start = _month_offset(today, -n)
            end = today
        elif keyword == "last_n_days":
            n = int(m.group(1))
            start = today - timedelta(days=n)
            end = today
        else:
            continue

        explicit = _EXPLICIT_YEAR_RE.search(question)
        if explicit:
            explicit_year = int(next(g for g in explicit.groups() if g))
            if start.year <= explicit_year <= end.year:
                return None

        return TemporalIntent(keyword=keyword, start_date=start, end_date=end)

    return None


def _rewrite_temporal_dates(
    gt_sql: str,
    intent: TemporalIntent,
) -> tuple[str, dict | None]:
    """Replace hardcoded date literals in *gt_sql* with *intent* dates.

    Returns ``(rewritten_sql, metadata_dict | None)``.
    ``metadata_dict`` is ``None`` when no rewriting was needed.
    """
    if not gt_sql:
        return gt_sql, None

    literals = _DATE_LITERAL_RE.findall(gt_sql)
    if not literals:
        return gt_sql, None

    sorted_dates = sorted(set(literals))
    gt_start = sorted_dates[0]
    gt_end = sorted_dates[-1]

    gt_start_year = int(gt_start[:4])
    gt_end_year = int(gt_end[:4])
    if intent.start_date.year == gt_start_year and intent.end_date.year == gt_end_year:
        return gt_sql, None

    new_start = intent.start_date.isoformat()
    new_end = intent.end_date.isoformat()

    rewritten = gt_sql
    if len(sorted_dates) >= 2:
        rewritten = rewritten.replace(f"'{gt_start}'", f"'{new_start}'")
        rewritten = rewritten.replace(f"'{gt_end}'", f"'{new_end}'")
    else:
        rewritten = rewritten.replace(f"'{gt_start}'", f"'{new_start}'")

    if rewritten == gt_sql:
        return gt_sql, None

    metadata = {
        "keyword": intent.keyword,
        "original_dates": [gt_start, gt_end] if len(sorted_dates) >= 2 else [gt_start],
        "rewritten_dates": [new_start, new_end] if len(sorted_dates) >= 2 else [new_start],
    }
    return rewritten, metadata


def normalize_result_df(df: pd.DataFrame | None) -> pd.DataFrame:
    """Deterministic normalization of a result DataFrame.

    Sort columns alphabetically, sort rows, round floats to 4 decimals,
    normalize timestamps to UTC, strip whitespace.  We use 4 decimals
    rather than 6 because GT (via Spark toPandas) and Genie (via REST API)
    serialize floats at different precisions.

    The Genie Statement Execution API returns all values as strings
    (including scientific notation like ``1.75E7``), so we attempt
    ``pd.to_numeric`` on object columns before rounding.
    """
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df = df[sorted(df.columns)]
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().any() and converted.notna().sum() >= df[col].notna().sum() * 0.5:
            df[col] = converted
    _BOOL_CANONICAL = {"true": "true", "false": "false"}
    for col in df.select_dtypes(include=["bool"]).columns:
        df[col] = df[col].astype(str).str.lower()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda x: _BOOL_CANONICAL.get(x.lower(), x)
            if isinstance(x, str) and x.lower() in _BOOL_CANONICAL
            else x
        )
    for col in df.select_dtypes(include=["number"]).columns:
        if df[col].dtype.kind == "i":
            df[col] = df[col].astype("float64")
        if df[col].dtype.kind == "f":
            df[col] = df[col].round(4)
    for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        df[col] = pd.to_datetime(df[col], utc=True)
    df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    return df


def result_signature(df: pd.DataFrame | None) -> dict:
    """Schema hash + rowcount + numeric sums for result comparison."""
    if df is None or df.empty:
        return {"schema_hash": "", "row_count": 0, "numeric_sums": {}}
    schema_str = ",".join(f"{c}:{df[c].dtype}" for c in sorted(df.columns))
    schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:8]
    numeric_sums: dict[str, float] = {}
    for col in df.select_dtypes(include=["number"]).columns:
        numeric_sums[col] = round(float(df[col].sum()), 4)
    return {
        "schema_hash": schema_hash,
        "row_count": len(df),
        "numeric_sums": numeric_sums,
    }


def build_asi_metadata(
    failure_type: str = "other",
    severity: str = "minor",
    confidence: float = 0.5,
    wrong_clause: str | None = None,
    blame_set: list[str] | None = None,
    quoted_metadata_text: str | None = None,
    missing_metadata: str | None = None,
    ambiguity_detected: bool = False,
    expected_value: str | None = None,
    actual_value: str | None = None,
    counterfactual_fix: str | None = None,
    affected_question_pattern: str | None = None,
) -> dict:
    """Build an ASI metadata dict conforming to ASI_SCHEMA."""
    return {
        "failure_type": failure_type if failure_type in FAILURE_TAXONOMY else "other",
        "severity": severity,
        "confidence": confidence,
        "wrong_clause": wrong_clause,
        "blame_set": blame_set or [],
        "quoted_metadata_text": quoted_metadata_text,
        "missing_metadata": missing_metadata,
        "ambiguity_detected": ambiguity_detected,
        "expected_value": expected_value,
        "actual_value": actual_value,
        "counterfactual_fix": counterfactual_fix,
        "affected_question_pattern": affected_question_pattern,
    }


def format_asi_markdown(
    *,
    judge_name: str,
    value: str,
    rationale: str,
    metadata: dict | None = None,
    extra: dict | None = None,
    question_id: str | None = None,
) -> str:
    """Render scorer feedback in a structured markdown + JSON ASI format.

    When *question_id* is provided, the payload is also written to
    ``_SCORER_FEEDBACK_CACHE`` so that downstream code (``run_evaluation``)
    can recover rationale / metadata even when MLflow's eval_results table
    only stores the verdict value.
    """
    verdict_map = {
        "yes": "Pass",
        "no": "Fail",
        "unknown": "Unknown",
        "skipped": "Skipped",
        "genie_correct": "Pass",
        "both_correct": "Pass",
        "ground_truth_correct": "Fail",
        "neither_correct": "Fail",
    }
    verdict = verdict_map.get(value, value)
    rationale_text = (rationale or "").strip() or "No rationale provided."

    payload: dict[str, Any] = {
        "judge": judge_name,
        "verdict": verdict,
        "raw_value": value,
        "failure_type": None,
        "severity": None,
        "wrong_clause": None,
        "missing_metadata": None,
        "expected_value": None,
        "actual_value": None,
        "counterfactual_fix": None,
        "blame_set": [],
        "confidence": None,
        "rationale": rationale_text,
    }
    if metadata:
        for key in (
            "failure_type",
            "severity",
            "wrong_clause",
            "missing_metadata",
            "expected_value",
            "actual_value",
            "counterfactual_fix",
            "blame_set",
            "confidence",
            "quoted_metadata_text",
            "ambiguity_detected",
            "affected_question_pattern",
        ):
            if key in metadata:
                payload[key] = metadata[key]
    if extra:
        payload.update(extra)

    if question_id:
        cache_meta = {
            k: payload[k]
            for k in ("failure_type", "severity", "wrong_clause", "blame_set",
                       "confidence", "counterfactual_fix")
            if payload.get(k) is not None
        }
        _cache_scorer_feedback(question_id, judge_name, rationale_text, cache_meta)

    _MLFLOW_ASSESSMENT_LIMIT = 60_000
    raw = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if len(raw) > _MLFLOW_ASSESSMENT_LIMIT:
        for bulky_key in ("comparison", "llm_response", "extra"):
            if bulky_key in payload:
                payload[bulky_key] = "(truncated — exceeds MLflow 64KB limit)"
        raw = json.dumps(payload, indent=2, sort_keys=True, default=str)

    return (
        f"### {judge_name}\n"
        f"**Verdict:** {verdict}\n\n"
        f"{rationale_text}\n\n"
        "```json\n"
        f"{raw}\n"
        "```"
    )


def _parse_asi_from_rationale(rationale: str) -> dict:
    """Extract the ASI JSON payload embedded in a ``format_asi_markdown`` rationale.

    Handles both real newlines and literal ``\\n`` sequences that arise when
    the rationale survives a SQL round-trip through ``_esc`` / ``_opt_json``.
    """
    if not rationale:
        return {}
    _MARKERS = [
        ("```json\n", "\n```"),
        ("```json\\n", "\\n```"),
    ]
    for start_marker, end_marker in _MARKERS:
        try:
            start = rationale.index(start_marker) + len(start_marker)
            end = rationale.index(end_marker, start)
            json_text = rationale[start:end]
            if "\\n" in json_text and "\n" not in json_text:
                json_text = json_text.replace("\\n", "\n").replace("\\t", "\t")
            return json.loads(json_text)
        except (ValueError, json.JSONDecodeError):
            continue
    return {}


def _extract_assessments_from_traces(results_df) -> dict[int, dict[str, dict]]:
    """Pull scorer rationale + metadata from trace or assessments columns.

    Returns ``{row_index: {judge_name: {"rationale": str, "metadata": dict}}}``.

    Checks three sources in order:
    1. ``trace.data.assessments`` / ``trace.info.assessments`` (legacy path)
    2. Top-level ``assessments`` column (MLflow genai >=2.x puts Feedback
       objects here directly)
    3. Falls back gracefully if nothing is available.
    """
    out: dict[int, dict[str, dict]] = {}

    has_trace = "trace" in results_df.columns
    has_assessments = "assessments" in results_df.columns

    if not has_trace and not has_assessments:
        return out

    for row_idx, (_, row) in enumerate(results_df.iterrows()):
        assessments = None

        if has_trace:
            trace = row.get("trace")
            if trace is not None:
                for attr_chain in [("data", "assessments"), ("info", "assessments")]:
                    obj = trace
                    for attr in attr_chain:
                        obj = getattr(obj, attr, None)
                        if obj is None:
                            break
                    if obj is not None:
                        assessments = obj
                        break

        if not assessments and has_assessments:
            raw = row.get("assessments")
            if isinstance(raw, list):
                assessments = raw
            elif raw is not None and hasattr(raw, "__iter__"):
                try:
                    assessments = list(raw)
                except Exception:
                    pass

        if not assessments:
            continue

        row_data: dict[str, dict] = {}
        for a in assessments:
            if isinstance(a, dict):
                name = a.get("name", "") or ""
                rationale_raw = a.get("rationale", "") or ""
                meta = a.get("metadata")
                if not isinstance(meta, dict):
                    meta = {}
            else:
                name = getattr(a, "name", "") or ""
                rationale_raw = getattr(a, "rationale", "") or ""
                meta = getattr(a, "metadata", None)
                if not isinstance(meta, dict):
                    meta = {}
            if not meta:
                meta = _parse_asi_from_rationale(rationale_raw)
            if name:
                row_data[name] = {"rationale": rationale_raw, "metadata": meta}
        out[row_idx] = row_data
    return out


def normalize_scores(scores: dict[str, float]) -> dict[str, float]:
    """Convert 0-1 scale → 0-100 scale; leave 0-100 unchanged."""
    normalized: dict[str, float] = {}
    for key, val in scores.items():
        if 0 <= val <= 1.0:
            normalized[key] = round(val * 100, 2)
        else:
            normalized[key] = round(val, 2)
    return normalized


def all_thresholds_met(
    scores: dict[str, float],
    targets: dict[str, float] | None = None,
) -> bool:
    """Return True only when every judge meets its threshold.

    ``scores`` should be on a 0-100 scale. ``targets`` defaults to
    ``DEFAULT_THRESHOLDS`` from config.
    """
    targets = targets or DEFAULT_THRESHOLDS
    for judge, threshold in targets.items():
        actual = scores.get(judge)
        if actual is None:
            return False
        if actual < threshold:
            return False
    return True


# ── Asset Type Normalization ───────────────────────────────────────────

_VALID_ASSET_TYPES = frozenset({"MV", "TVF", "TABLE"})


def _normalize_expected_asset(raw: str, expected_sql: str) -> str:
    """Normalize ``expected_asset`` to a valid type category.

    Benchmarks may store table *names* (``BOOKING_ANALYTICS_METRICS``) instead
    of type categories (``MV``/``TVF``/``TABLE``).  When the stored value is
    not a recognized type, fall back to ``detect_asset_type(expected_sql)``.
    """
    upper = raw.strip().upper() if raw else ""
    if upper in _VALID_ASSET_TYPES:
        return upper
    return detect_asset_type(expected_sql)


# ── Arbiter-Adjusted Accuracy ──────────────────────────────────────────

_ARBITER_CORRECT_VERDICTS = frozenset({"genie_correct", "both_correct"})


def _compute_arbiter_adjusted_accuracy(
    rows: list[dict],
) -> tuple[float, int, list[str], int]:
    """Compute overall accuracy that accounts for arbiter overrides.

    A row is considered correct if:
      - ``result_correctness`` == "yes" (results matched), OR
      - ``result_correctness`` == "no" AND arbiter verdict is
        ``genie_correct`` or ``both_correct``

    Rows where ``result_correctness`` == "excluded" (GT-side infrastructure
    failures) are removed from the denominator entirely.

    Returns ``(accuracy_pct, correct_count, failure_ids, excluded_count)``.
    """
    if not rows:
        return 0.0, 0, [], 0

    total = 0
    correct = 0
    excluded = 0
    failure_ids: list[str] = []
    for row in rows:
        rc = str(
            row.get("result_correctness/value", row.get("result_correctness", ""))
        ).lower()

        if rc == "excluded":
            excluded += 1
            continue

        total += 1
        av = str(
            row.get("arbiter/value", row.get("arbiter", "skipped"))
        ).lower()

        is_correct = rc in ("yes", "true", "1", "1.0") or (
            rc in ("no", "false", "0", "0.0") and av in _ARBITER_CORRECT_VERDICTS
        )

        if is_correct:
            correct += 1
        else:
            _rq = row.get("request") or {}
            if isinstance(_rq, str):
                try:
                    _rq = json.loads(_rq)
                except (json.JSONDecodeError, TypeError):
                    _rq = {}
            _rqk = _rq.get("kwargs", {}) if isinstance(_rq, dict) else {}
            qid = (
                row.get("inputs/question_id")
                or (row.get("inputs") or {}).get("question_id", "")
                or row.get("question_id")
                or _rqk.get("question_id")
                or (_rq.get("question_id") if isinstance(_rq, dict) else None)
                or ""
            )
            if qid:
                failure_ids.append(str(qid))

    accuracy_pct = round((correct / total) * 100, 2) if total > 0 else 0.0
    return accuracy_pct, correct, failure_ids, excluded


# ── Benchmark Filtering ─────────────────────────────────────────────────


def filter_benchmarks_by_scope(
    benchmarks: list[dict],
    scope: str = "full",
    patched_objects: list[str] | None = None,
    affected_question_ids: set[str] | None = None,
) -> list[dict]:
    """Filter benchmarks based on evaluation scope.

    Scopes: "full" (all), "slice" (affected by patches),
    "p0" (priority P0 only), "held_out" (held-out split).

    For "slice" scope, benchmarks are included if:
    - Their required tables/columns overlap with *patched_objects*, OR
    - Their question id is in *affected_question_ids* (from proposal clusters).
    """
    if scope == "full":
        return benchmarks
    if scope == "slice":
        patched = {o.lower() for o in patched_objects} if patched_objects else set()
        affected_qids = affected_question_ids or set()
        result = []
        for b in benchmarks:
            qid = b.get("id", "")
            if qid and qid in affected_qids:
                result.append(b)
                continue
            if patched and any(
                t.lower() in patched
                for t in b.get("required_tables", []) + b.get("required_columns", [])
            ):
                result.append(b)
        return result
    if scope == "p0":
        return [b for b in benchmarks if b.get("priority", "P1") == "P0"]
    if scope == "held_out":
        return [b for b in benchmarks if b.get("split") == "held_out"]
    return benchmarks


def _load_known_functions(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> set[str]:
    """Load functions available in the target schema for fast pre-checks."""
    if not catalog or not schema:
        return set()
    try:
        _set_sql_context(spark, catalog, schema)
        rows = spark.sql(f"SHOW USER FUNCTIONS IN `{catalog}`.`{schema}`").collect()
    except Exception:
        logger.warning("Could not list functions for %s.%s", catalog, schema)
        return set()

    known: set[str] = set()
    for row in rows:
        row_dict = row.asDict() if hasattr(row, "asDict") else {}
        raw_name = str(row_dict.get("function") or row_dict.get("name") or "").strip()
        if not raw_name:
            continue
        known.add(raw_name.lower())
        known.add(raw_name.split(".")[-1].lower())
    return known


def _extract_sql_function_calls(sql: str, catalog: str, schema: str) -> set[str]:
    """Extract fully-qualified function names called with parentheses."""
    if not sql or not catalog or not schema:
        return set()
    pattern = re.compile(
        rf"(?i)\b{re.escape(catalog)}\s*\.\s*{re.escape(schema)}\s*\.\s*([a-zA-Z_][\w]*)\s*\(",
    )
    return {m.group(1).lower() for m in pattern.finditer(sql)}


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _set_sql_context(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> None:
    """Ensure Spark SQL context is aligned to target catalog/schema."""
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(schema)}")


_SQL_PARAM_RE = re.compile(
    r"(?<![:\w])"     # not preceded by : or word char (avoids ::cast, timestamps)
    r":([a-zA-Z_]\w*)"  # :param_name
    r"(?!\s*:)"        # not followed by : (avoids :: cast operator)
)


def _extract_sql_params(sql: str) -> list[str]:
    """Return SQL named-parameter placeholders (e.g. :min_amount) found in *sql*."""
    if not sql:
        return []
    return _SQL_PARAM_RE.findall(sql)


def _is_infrastructure_sql_error(message: str) -> bool:
    """Detect environment/config errors that should fail evaluation.

    With OBO-first execution the job runs as the triggering user, so
    permission errors (INSUFFICIENT_PERMISSIONS, permission denied) are
    genuine evaluation failures rather than infrastructure mis-config.
    Only SQL context and object-existence errors are treated as infra.
    """
    m = (message or "").lower()
    patterns = (
        "not in the current catalog",
        "please set the current catalog",
        "catalog does not exist",
        "schema does not exist",
        "resource_does_not_exist",
        "table_or_view_not_found",
        "cannot be found. verify the spelling",
        "unresolvable_table_valued_function",
    )
    return any(p in m for p in patterns)


def _extract_sqlstate(message: str) -> str | None:
    match = re.search(r"SQLSTATE:\s*([A-Z0-9]+)", message or "", flags=re.IGNORECASE)
    return match.group(1).upper() if match else None


def _classify_sql_validation_error(message: str) -> str:
    """Classify SQL validation failures into stable reason codes."""
    lowered = (message or "").lower()
    if "metric_view_join_not_supported" in lowered:
        return "metric_view_join"
    if "insufficient_permissions" in lowered or "permission denied" in lowered:
        return "permission_blocked"
    if "does not have execute on routine" in lowered:
        return "permission_blocked"
    if "unresolved_column" in lowered:
        if "join" in lowered:
            return "bad_join_key"
        return "unknown_column"
    if "table_or_view_not_found" in lowered or "cannot be found" in lowered:
        return "missing_object"
    if "parseexception" in lowered or "syntax error" in lowered:
        return "syntax_error"
    return "sql_compile_error"


_MV_JOIN_RE = re.compile(r"\bJOIN\b", re.IGNORECASE)


def _precheck_benchmarks_for_eval(
    *,
    benchmarks: list[dict],
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    known_functions: set[str],
    metric_view_names: set[str] | None = None,
    metric_view_measures: dict[str, set[str]] | None = None,
) -> tuple[list[dict], list[dict[str, Any]], dict[str, int]]:
    """Apply strict SQL + routine checks before entering mlflow.genai.evaluate()."""
    valid: list[dict] = []
    quarantined: list[dict[str, Any]] = []
    reason_counts = {
        "invalid_benchmark_count": 0,
        "permission_blocked_count": 0,
        "unresolved_column_count": 0,
        "bad_join_key_count": 0,
    }
    mv_names_lower = {n.lower().split(".")[-1] for n in (metric_view_names or set())}
    _mv_measures = metric_view_measures or {}

    for idx, benchmark in enumerate(benchmarks):
        question = str(benchmark.get("question") or "").strip()
        qid = str(benchmark.get("id") or benchmark.get("question_id") or f"q-{idx}")
        sql = str(benchmark.get("expected_sql") or "").strip()
        if not sql:
            valid.append(benchmark)
            continue

        resolved_sql = resolve_sql(sql, catalog=catalog, gold_schema=gold_schema)
        if _mv_measures:
            resolved_sql = _rewrite_measure_refs(resolved_sql, _mv_measures)

        _found_params = _extract_sql_params(resolved_sql)
        if _found_params:
            from genie_space_optimizer.optimization.benchmarks import _resolve_params_with_defaults
            _bench_params = benchmark.get("parameters", [])
            _resolved_default, _all_resolved = _resolve_params_with_defaults(
                resolved_sql, _bench_params,
            )
            if _all_resolved:
                logger.info(
                    "Benchmark %s: substituted defaults for %d params — running EXPLAIN",
                    qid, len(_found_params),
                )
                resolved_sql = _resolved_default
            else:
                logger.info(
                    "Benchmark %s has parameterized SQL (some without defaults) — "
                    "skipping EXPLAIN quarantine",
                    qid,
                )
                valid.append(benchmark)
                continue

        try:
            _set_sql_context(spark, catalog, gold_schema)
            spark.sql(f"EXPLAIN {resolved_sql}")
        except Exception as exc:
            msg = str(exc)
            if "UNBOUND_SQL_PARAMETER" in msg:
                logger.info(
                    "Benchmark %s hit UNBOUND_SQL_PARAMETER in EXPLAIN — "
                    "treating as valid (parameterized SQL)",
                    qid,
                )
                valid.append(benchmark)
                continue
            reason = _classify_sql_validation_error(msg)
            quarantined.append(
                {
                    "question_id": qid,
                    "question": question,
                    "reason": reason,
                    "sqlstate": _extract_sqlstate(msg),
                    "error": msg[:500],
                    "expected_sql": resolved_sql[:1500],
                }
            )
            reason_counts["invalid_benchmark_count"] += 1
            if reason == "permission_blocked":
                reason_counts["permission_blocked_count"] += 1
            if reason == "unknown_column":
                reason_counts["unresolved_column_count"] += 1
            if reason == "bad_join_key":
                reason_counts["bad_join_key_count"] += 1
            continue

        expected_asset = _normalize_expected_asset(
            str(benchmark.get("expected_asset", "")), resolved_sql,
        )
        uses_measure = "MEASURE(" in resolved_sql.upper()
        refs_metric_view = any(
            mv in resolved_sql.lower() for mv in mv_names_lower
        ) if mv_names_lower else False
        is_mv_context = expected_asset == "MV" or uses_measure or refs_metric_view
        if is_mv_context and _MV_JOIN_RE.search(resolved_sql):
            quarantined.append(
                {
                    "question_id": qid,
                    "question": question,
                    "reason": "metric_view_join",
                    "sqlstate": None,
                    "error": "Metric view / MEASURE() benchmarks cannot use JOINs (METRIC_VIEW_JOIN_NOT_SUPPORTED)",
                    "expected_sql": resolved_sql[:1500],
                }
            )
            reason_counts["invalid_benchmark_count"] += 1
            continue

        called_functions = _extract_sql_function_calls(resolved_sql, catalog, gold_schema)
        blocked_functions = sorted(fn for fn in called_functions if fn not in known_functions)
        if blocked_functions:
            quarantined.append(
                {
                    "question_id": qid,
                    "question": question,
                    "reason": "permission_blocked",
                    "sqlstate": "42501",
                    "blocked_routines": blocked_functions,
                    "error": (
                        "No EXECUTE privilege or function unavailable for one or more routines: "
                        + ", ".join(blocked_functions)
                    ),
                    "expected_sql": resolved_sql[:1500],
                }
            )
            reason_counts["invalid_benchmark_count"] += 1
            reason_counts["permission_blocked_count"] += 1
            continue

        valid.append(benchmark)

    return valid, quarantined, reason_counts


# ── Predict Function (Factory Closure) ──────────────────────────────────


def make_predict_fn(
    w: WorkspaceClient,
    space_id: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    metric_view_measures: dict[str, set[str]] | None = None,
    *,
    optimization_run_id: str = "",
    iteration: int | None = None,
    lever: int | None = None,
    eval_scope: str = "",
    triggered_by: str = "",
):
    """Return a predict function with bound workspace/spark context.

    The returned closure is suitable for ``mlflow.genai.evaluate(predict_fn=...)``.
    ``metric_view_measures`` maps lowercased metric view short names to sets
    of measure column names — used to auto-rewrite ORDER BY for GT SQL.
    """

    known_functions = _load_known_functions(spark, catalog, schema)
    _mv_measures = metric_view_measures or {}

    @mlflow.trace
    def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
        """Query Genie, fetch its results via Statement API, execute only GT SQL.

        Steps: rate-limit → Genie call → fetch Genie result via statement_id →
               resolve & execute GT SQL → normalize → compare hashes.

        We never re-execute Genie's SQL ourselves.  Genie runs queries on its
        own SQL warehouse; re-executing via Spark Connect can hit different
        limitations (e.g. METRIC_VIEW_JOIN_NOT_SUPPORTED).
        """
        try:
            _trace_tags: dict[str, str] = {
                "question_id": kwargs.get("question_id", ""),
                "space_id": space_id,
            }
            if optimization_run_id:
                _trace_tags["genie.optimization_run_id"] = optimization_run_id
            if iteration is not None:
                _trace_tags["genie.iteration"] = str(iteration)
            if lever is not None:
                _trace_tags["genie.lever"] = str(lever)
            if eval_scope:
                _trace_tags["genie.eval_scope"] = eval_scope
            _trace_metadata: dict[str, str] = {}
            if triggered_by:
                _trace_metadata["mlflow.trace.user"] = triggered_by
            if optimization_run_id:
                _trace_metadata["mlflow.trace.session"] = optimization_run_id
            if _trace_metadata:
                mlflow.update_current_trace(tags=_trace_tags, metadata=_trace_metadata)
            else:
                mlflow.update_current_trace(tags=_trace_tags)
        except Exception:
            pass

        comparison: dict[str, Any] = {
            "match": False,
            "match_type": "mismatch",
            "gt_rows": 0,
            "genie_rows": 0,
            "gt_hash": None,
            "genie_hash": None,
            "gt_signature": None,
            "genie_signature": None,
            "error": None,
        }
        result: dict[str, Any] = {}
        genie_sql = ""
        gt_sql = ""
        temporal_rewrite_meta: dict | None = None
        try:
            time.sleep(RATE_LIMIT_SECONDS)
            result = run_genie_query(w, space_id, question)
            genie_sql = sanitize_sql(result.get("sql") or "")
            gt_sql = resolve_sql(expected_sql, catalog, schema)
            if _mv_measures and gt_sql:
                gt_sql = _rewrite_measure_refs(gt_sql, _mv_measures)
            temporal_intent = _detect_temporal_intent(question)
            if temporal_intent and gt_sql:
                gt_sql, temporal_rewrite_meta = _rewrite_temporal_dates(gt_sql, temporal_intent)
                if temporal_rewrite_meta:
                    logger.info(
                        "Temporal rewrite for '%s': %s → %s",
                        temporal_intent.keyword,
                        temporal_rewrite_meta["original_dates"],
                        temporal_rewrite_meta["rewritten_dates"],
                    )
            statement_id = result.get("statement_id")

            if genie_sql and not comparison.get("error"):
                try:
                    _set_sql_context(spark, catalog, schema)
                    spark.sql(f"EXPLAIN {genie_sql}")
                except Exception as _genie_explain_exc:
                    _genie_msg = str(_genie_explain_exc)
                    if "UNBOUND_SQL_PARAMETER" not in _genie_msg:
                        comparison["error"] = (
                            f"Genie SQL failed EXPLAIN: {_genie_msg[:400]}"
                        )
                        comparison["error_type"] = "genie_sql_invalid"
                        logger.warning(
                            "Genie SQL for '%s' failed EXPLAIN pre-check: %.200s",
                            question[:60], _genie_msg,
                        )

            if genie_sql and gt_sql:
                _genie_sql_norm = genie_sql.strip().lower()
                _gt_sql_norm = gt_sql.strip().lower()

                if _genie_sql_norm and _gt_sql_norm and _genie_sql_norm == _gt_sql_norm:
                    comparison = {
                        "match": True,
                        "match_type": "identical_sql",
                        "gt_rows": None,
                        "genie_rows": None,
                        "gt_hash": None,
                        "genie_hash": None,
                        "gt_signature": None,
                        "genie_signature": None,
                        "error": None,
                        "identical_sql": True,
                    }
                else:
                    _unbound_params = _extract_sql_params(gt_sql)
                    if _unbound_params:
                        from genie_space_optimizer.optimization.benchmarks import _resolve_params_with_defaults
                        _bench_params = kwargs.get("parameters", [])
                        _gt_resolved, _gt_all = _resolve_params_with_defaults(
                            gt_sql, _bench_params,
                        )
                        if _gt_all:
                            logger.info(
                                "Substituted defaults for %d params in GT SQL for '%s'",
                                len(_unbound_params), question[:60],
                            )
                            gt_sql = _gt_resolved
                        else:
                            logger.warning(
                                "GT SQL contains unbound parameters %s — "
                                "skipping result comparison for '%s'",
                                _unbound_params, question[:80],
                            )
                            comparison["error"] = (
                                f"GT SQL contains parameterized placeholders "
                                f"({', '.join(':' + p for p in _unbound_params)}) "
                                f"that cannot be executed directly"
                            )
                            comparison["error_type"] = "parameterized_sql"

                    if not comparison.get("error"):
                        try:
                            _set_sql_context(spark, catalog, schema)

                            called_functions = _extract_sql_function_calls(gt_sql, catalog, schema)
                            missing_gt_functions = sorted(f for f in called_functions if f not in known_functions)
                            if missing_gt_functions:
                                comparison["error"] = (
                                    "Missing function(s) in GT SQL for schema "
                                    f"{catalog}.{schema}: {', '.join(missing_gt_functions)}"
                                )
                                comparison["error_type"] = "permission_blocked"
                            else:
                                try:
                                    spark.sql(f"EXPLAIN {gt_sql}")
                                except Exception as explain_exc:
                                    explain_msg = str(explain_exc)
                                    if "UNBOUND_SQL_PARAMETER" in explain_msg:
                                        comparison["error"] = (
                                            f"GT SQL contains parameterized placeholders "
                                            f"that cannot be executed directly: {explain_msg[:300]}"
                                        )
                                        comparison["error_type"] = "parameterized_sql"
                                    else:
                                        comparison["error"] = f"ground_truth SQL compilation failed: {explain_msg[:400]}"
                                        comparison["error_type"] = "infrastructure"
                                    comparison["sqlstate"] = _extract_sqlstate(explain_msg)

                                if not comparison["error"]:
                                    gt_df = normalize_result_df(spark.sql(gt_sql).toPandas())

                                genie_df = None
                                if statement_id:
                                    raw_genie_df = fetch_genie_result_df(w, statement_id)
                                    genie_df = normalize_result_df(raw_genie_df)

                                if genie_df is None or genie_df.empty:
                                    comparison["error"] = (
                                        "Could not retrieve Genie query results"
                                        + (f" (statement_id={statement_id})" if statement_id else " (no statement_id)")
                                    )
                                    comparison["error_type"] = "genie_result_unavailable"
                                    comparison["gt_rows"] = len(gt_df)
                                    comparison["gt_sample"] = gt_df.head(5).to_csv(index=False, float_format="%.4f")
                                else:
                                    mapped_genie_df = genie_df
                                    _FLOAT_FMT = "%.4f"
                                    gt_hash = hashlib.md5(
                                        gt_df.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                    ).hexdigest()[:8]
                                    genie_hash = hashlib.md5(
                                        genie_df.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                    ).hexdigest()[:8]
                                    exact_match = gt_df.shape == genie_df.shape and gt_df.equals(genie_df)
                                    hash_match = gt_hash == genie_hash

                                    subset_match = False
                                    subset_type = None
                                    if not hash_match:
                                        genie_cols = set(genie_df.columns)
                                        gt_cols = set(gt_df.columns)
                                        shared_cols = sorted(genie_cols & gt_cols)
                                        all_mapped = genie_cols <= gt_cols

                                        if not all_mapped:
                                            unmatched_genie = sorted(genie_cols - gt_cols)
                                            candidate_gt = sorted(gt_cols - genie_cols)
                                            col_map: dict[str, str] = {}
                                            _ALIAS_SAMPLE = min(50, len(genie_df))
                                            for gc in unmatched_genie:
                                                g_vals = genie_df[gc].head(_ALIAS_SAMPLE).tolist()
                                                for gtc in candidate_gt:
                                                    if gtc in col_map.values():
                                                        continue
                                                    gt_vals = gt_df[gtc].head(_ALIAS_SAMPLE).tolist()
                                                    if g_vals == gt_vals:
                                                        col_map[gc] = gtc
                                                        break
                                                    try:
                                                        import numpy as np
                                                        g_arr = np.array(g_vals, dtype=float)
                                                        gt_arr = np.array(gt_vals, dtype=float)
                                                        if np.allclose(g_arr, gt_arr, rtol=1e-4, atol=1e-4, equal_nan=True):
                                                            col_map[gc] = gtc
                                                            break
                                                    except (ValueError, TypeError):
                                                        pass
                                            if len(col_map) == len(unmatched_genie):
                                                mapped_genie_df = genie_df.rename(columns=col_map)
                                                genie_cols = set(mapped_genie_df.columns)
                                                shared_cols = sorted(genie_cols & gt_cols)
                                                all_mapped = genie_cols <= gt_cols

                                        if shared_cols and all_mapped:
                                            _GENIE_ROW_CAP = 5000
                                            gt_sub = gt_df[shared_cols].sort_values(shared_cols).reset_index(drop=True)
                                            genie_sub = mapped_genie_df[shared_cols].sort_values(shared_cols).reset_index(drop=True)
                                            if len(genie_df) == _GENIE_ROW_CAP and len(gt_df) > _GENIE_ROW_CAP:
                                                gt_sub = gt_sub.head(_GENIE_ROW_CAP)
                                            gt_sub_hash = hashlib.md5(
                                                gt_sub.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                            ).hexdigest()[:8]
                                            genie_sub_hash = hashlib.md5(
                                                genie_sub.to_csv(index=False, float_format=_FLOAT_FMT).encode()
                                            ).hexdigest()[:8]
                                            if gt_sub_hash == genie_sub_hash:
                                                subset_match = True
                                                subset_type = "column_subset"
                                                if len(genie_df) == _GENIE_ROW_CAP and len(gt_df) > _GENIE_ROW_CAP:
                                                    subset_type = "column_subset_row_capped"

                                    approx_match = False
                                    _approx_genie = mapped_genie_df if mapped_genie_df is not genie_df else genie_df
                                    if (
                                        not hash_match
                                        and not subset_match
                                        and gt_df.shape == _approx_genie.shape
                                        and list(gt_df.columns) == list(_approx_genie.columns)
                                    ):
                                        try:
                                            import numpy as np

                                            gt_sorted = gt_df.sort_values(list(gt_df.columns)).reset_index(drop=True)
                                            genie_sorted = _approx_genie.sort_values(list(_approx_genie.columns)).reset_index(drop=True)

                                            all_numeric = set(
                                                gt_sorted.select_dtypes(include=["number"]).columns
                                            ) | set(
                                                genie_sorted.select_dtypes(include=["number"]).columns
                                            )
                                            for col in list(all_numeric):
                                                for _df in (gt_sorted, genie_sorted):
                                                    if _df[col].dtype == object:
                                                        _df[col] = pd.to_numeric(_df[col], errors="coerce")

                                            non_numeric = [c for c in gt_sorted.columns if c not in all_numeric]
                                            non_num_match = gt_sorted[non_numeric].equals(genie_sorted[non_numeric]) if non_numeric else True
                                            numeric = sorted(all_numeric)
                                            num_match = (
                                                np.allclose(
                                                    gt_sorted[numeric].values.astype(float),
                                                    genie_sorted[numeric].values.astype(float),
                                                    rtol=1e-4,
                                                    atol=1e-4,
                                                    equal_nan=True,
                                                )
                                                if numeric
                                                else True
                                            )
                                            approx_match = bool(non_num_match and num_match)
                                        except Exception:
                                            approx_match = False

                                    gt_sig = result_signature(gt_df)
                                    genie_sig = result_signature(genie_df)
                                    sig_match = (
                                        gt_sig["schema_hash"] == genie_sig["schema_hash"]
                                        and gt_sig["row_count"] == genie_sig["row_count"]
                                    )

                                    tied_subset = False
                                    if (
                                        not exact_match
                                        and not hash_match
                                        and not subset_match
                                        and not approx_match
                                        and len(gt_df) == len(genie_df)
                                        and len(gt_df) > 0
                                        and bool(re.search(r"\bLIMIT\b", gt_sql, re.I))
                                    ):
                                        try:
                                            import numpy as np

                                            _tg = mapped_genie_df if mapped_genie_df is not genie_df else genie_df
                                            _shared = sorted(set(gt_df.columns) & set(_tg.columns))
                                            if _shared:
                                                _gt_s = gt_df[_shared].sort_values(_shared).reset_index(drop=True)
                                                _ge_s = _tg[_shared].sort_values(_shared).reset_index(drop=True)
                                                _num_cols = sorted(
                                                    set(_gt_s.select_dtypes(include=["number"]).columns)
                                                    | set(_ge_s.select_dtypes(include=["number"]).columns)
                                                )
                                                _non_num = [c for c in _shared if c not in _num_cols]
                                                _nn_ok = _gt_s[_non_num].equals(_ge_s[_non_num]) if _non_num else True
                                                _n_ok = (
                                                    np.allclose(
                                                        _gt_s[_num_cols].values.astype(float),
                                                        _ge_s[_num_cols].values.astype(float),
                                                        rtol=1e-4, atol=1e-4, equal_nan=True,
                                                    )
                                                    if _num_cols
                                                    else True
                                                )
                                                tied_subset = bool(_nn_ok and _n_ok)
                                        except Exception:
                                            tied_subset = False

                                    if exact_match:
                                        match_type = "exact"
                                    elif hash_match:
                                        match_type = "hash"
                                    elif subset_match:
                                        match_type = subset_type
                                    elif approx_match:
                                        match_type = "approx"
                                    elif tied_subset:
                                        match_type = "tied_subset"
                                    elif sig_match:
                                        match_type = "signature"
                                    else:
                                        match_type = "mismatch"

                                    def _truncated_sample(df: pd.DataFrame, max_chars: int = 4000) -> str:
                                        sample = df.head(5).copy()
                                        for col in sample.select_dtypes(include=["object"]).columns:
                                            sample[col] = sample[col].apply(
                                                lambda x: (x[:100] + "...") if isinstance(x, str) and len(x) > 100 else x
                                            )
                                        csv = sample.to_csv(index=False, float_format=_FLOAT_FMT)
                                        return csv[:max_chars] if len(csv) > max_chars else csv

                                    gt_col_list = sorted(gt_df.columns.tolist())
                                    genie_col_list = sorted(genie_df.columns.tolist())
                                    comparison = {
                                        "match": exact_match or hash_match or subset_match or approx_match or tied_subset or sig_match,
                                        "match_type": match_type,
                                        "gt_rows": len(gt_df),
                                        "genie_rows": len(genie_df),
                                        "gt_columns": gt_col_list,
                                        "genie_columns": genie_col_list,
                                        "gt_hash": gt_hash,
                                        "genie_hash": genie_hash,
                                        "gt_signature": gt_sig,
                                        "genie_signature": genie_sig,
                                        "gt_sample": _truncated_sample(gt_df),
                                        "genie_sample": _truncated_sample(genie_df),
                                        "error": None,
                                    }
                        except Exception as exc:
                            err_msg = str(exc)
                            comparison["error"] = err_msg[:500]
                            if "UNBOUND_SQL_PARAMETER" in err_msg:
                                comparison["error_type"] = "parameterized_sql"
                            elif _is_infrastructure_sql_error(err_msg):
                                comparison["error_type"] = "infrastructure"
                            else:
                                comparison["error_type"] = "query_execution"
                            comparison["sqlstate"] = _extract_sqlstate(err_msg)
            else:
                if not genie_sql:
                    comparison["error"] = "Genie did not return SQL"
                    comparison["error_type"] = "no_genie_sql"
                elif not gt_sql:
                    comparison["error"] = "Missing expected SQL for comparison"
                    comparison["error_type"] = "missing_expected_sql"
        except Exception as exc:
            err_msg = str(exc)
            comparison["error"] = err_msg[:500]
            if "UNBOUND_SQL_PARAMETER" in err_msg:
                comparison["error_type"] = "parameterized_sql"
            elif _is_infrastructure_sql_error(err_msg):
                comparison["error_type"] = "infrastructure"
            else:
                comparison["error_type"] = "predict_fn_error"
            comparison["sqlstate"] = _extract_sqlstate(err_msg)

        if temporal_rewrite_meta:
            comparison["temporal_rewrite"] = temporal_rewrite_meta

        output = {
            "response": genie_sql,
            "status": result.get("status", "ERROR"),
            "conversation_id": result.get("conversation_id", ""),
            "comparison": comparison,
            "analysis_text": result.get("analysis_text"),
        }

        if EVAL_DEBUG:
            qid = kwargs.get("question_id", "?")
            cmp = comparison
            logger.info(
                "\n"
                "═══ EVAL [Q:%s] ═══════════════════════════════════════════════\n"
                "  Question: \"%s\"\n"
                "  Status:   %s\n"
                "  Genie SQL:\n"
                "    %s\n"
                "  GT SQL:\n"
                "    %s\n"
                "  Comparison: match=%s | type=%s | gt_rows=%s | genie_rows=%s\n"
                "              gt_hash=%s | genie_hash=%s\n"
                "  Error:      %s\n"
                "  Analysis:   %s\n"
                "═══════════════════════════════════════════════════════════════",
                qid,
                question,
                output["status"],
                genie_sql or "(none)",
                gt_sql or "(none)",
                cmp.get("match"),
                cmp.get("match_type", "n/a"),
                cmp.get("gt_rows", "?"),
                cmp.get("genie_rows", "?"),
                cmp.get("gt_hash", "n/a"),
                cmp.get("genie_hash", "n/a"),
                cmp.get("error") or "(none)",
                str(output.get("analysis_text") or "(none)")[:200],
            )

        return output

    return genie_predict_fn


# ── MLflow Integration ──────────────────────────────────────────────────


PROMPT_REGISTRY_REQUIRED_PRIVILEGES = ("CREATE FUNCTION", "EXECUTE", "MANAGE")


def _classify_prompt_registration_error(message: str, uc_schema: str) -> dict[str, Any]:
    """Classify prompt registration failure into actionable root-cause buckets."""
    lowered = (message or "").lower()
    permission_markers = (
        "permission",
        "privilege",
        "not authorized",
        "forbidden",
        "insufficient",
        "access denied",
        "permission_denied",
    )
    missing_privileges = [
        priv for priv in PROMPT_REGISTRY_REQUIRED_PRIVILEGES if priv.lower() in lowered
    ]

    if any(marker in lowered for marker in permission_markers):
        if not missing_privileges:
            missing_privileges = list(PROMPT_REGISTRY_REQUIRED_PRIVILEGES)
        schema_target = uc_schema or "<catalog>.<schema>"
        return {
            "reason": "missing_uc_permissions",
            "missing_privileges": missing_privileges,
            "remediation": (
                f"Grant {', '.join(missing_privileges)} on schema {schema_target} "
                "to the Databricks App service principal used by job tasks."
            ),
        }

    if "preview" in lowered and ("prompt" in lowered or "genai" in lowered):
        return {
            "reason": "feature_not_enabled",
            "missing_privileges": [],
            "remediation": (
                "Enable MLflow Prompt Registry / GenAI preview in workspace settings."
            ),
        }

    if "does not exist" in lowered or "resource_does_not_exist" in lowered:
        schema_target = uc_schema or "<catalog>.<schema>"
        return {
            "reason": "registry_path_not_found",
            "missing_privileges": [],
            "remediation": (
                f"Verify catalog/schema exists and is accessible: {schema_target}."
            ),
        }

    return {
        "reason": "unknown",
        "missing_privileges": [],
        "remediation": (
            "Inspect full stack trace for prompt registration failure details "
            "and verify Prompt Registry availability."
        ),
    }


def register_instruction_version(
    uc_schema: str,
    space_id: str,
    instruction_text: str,
    *,
    run_id: str = "",
    lever: int = 0,
    iteration: int = 0,
    accuracy: float = 0.0,
    domain: str = "",
) -> dict[str, Any] | None:
    """Register the current Genie Space instruction text as a versioned prompt.

    Best-effort: failures are logged but never raise, so the optimization
    pipeline is never blocked by prompt registration issues.

    Returns ``{"prompt_name": ..., "version": ...}`` on success, ``None`` otherwise.
    """
    if not instruction_text or not instruction_text.strip():
        return None

    safe_space_id = re.sub(r"[^a-zA-Z0-9_]+", "_", space_id or "unknown").strip("_")
    prompt_name = format_mlflow_template(
        INSTRUCTION_PROMPT_NAME_TEMPLATE, uc_schema=uc_schema, space_id=safe_space_id,
    ) if uc_schema else f"genie_instructions_{safe_space_id}"

    commit_msg = (
        f"Genie instructions after lever {lever}, iteration {iteration} "
        f"(accuracy={accuracy:.3f}, run={run_id[:12]})"
    )
    tags = {
        "run_id": run_id,
        "lever": str(lever),
        "iteration": str(iteration),
        "accuracy": f"{accuracy:.4f}",
        "domain": domain,
        "space_id": space_id,
        "type": "genie_instructions",
    }

    try:
        version = mlflow.genai.register_prompt(
            name=prompt_name,
            template=instruction_text,
            commit_message=commit_msg,
            tags=tags,
        )
        mlflow.genai.set_prompt_alias(
            name=prompt_name,
            alias=INSTRUCTION_PROMPT_ALIAS,
            version=version.version,
        )
        logger.info(
            "[Instruction Registry] %s v%s (lever=%d, iter=%d, acc=%.3f)",
            prompt_name, version.version, lever, iteration, accuracy,
        )
        return {"prompt_name": prompt_name, "version": version.version}
    except Exception as exc:
        classification = _classify_prompt_registration_error(
            str(exc), uc_schema=uc_schema,
        )
        logger.warning(
            "Instruction registration failed for space=%s: %s (cause=%s)",
            space_id, str(exc)[:300], classification["reason"],
            exc_info=True,
        )
        return None


def register_judge_prompts(
    uc_schema: str,
    domain: str,
    experiment_name: str,
    *,
    register_registry: bool = True,
) -> dict[str, dict]:
    """Register judge prompts to MLflow Prompt Registry + experiment artifacts.

    Dual storage: Prompt Registry (versioned, aliased) and experiment
    artifacts (UI visibility). Idempotent.
    """
    registered: dict[str, dict] = {}
    failed_judges: list[str] = []
    failed_details: dict[str, dict[str, Any]] = {}

    mlflow.set_experiment(experiment_name)
    if uc_schema:
        try:
            # Align experiment with target prompt registry schema for discoverability.
            mlflow.set_experiment_tags({"mlflow.promptRegistryLocation": uc_schema})
        except Exception:
            logger.warning(
                "Failed to set experiment prompt registry location to %s",
                uc_schema,
                exc_info=True,
            )

    if register_registry:
        for name, template in JUDGE_PROMPTS.items():
            candidates = _prompt_name_candidates(uc_schema=uc_schema, domain=domain, judge_name=name)
            attempt_failures: list[dict[str, Any]] = []
            for prompt_name in candidates:
                try:
                    version = mlflow.genai.register_prompt(
                        name=prompt_name,
                        template=template,
                        commit_message=f"Genie eval judge: {name} (domain: {domain})",
                        tags={"domain": domain, "type": "judge"},
                    )
                    mlflow.genai.set_prompt_alias(
                        name=prompt_name,
                        alias=PROMPT_ALIAS,
                        version=version.version,
                    )
                    registered[name] = {
                        "prompt_name": prompt_name,
                        "version": version.version,
                    }
                    _REGISTERED_PROMPT_NAMES[name] = prompt_name
                    logger.info("[Prompt Registry] %s v%s", prompt_name, version.version)
                    break
                except Exception as exc:
                    err_msg = str(exc).strip()
                    classification = _classify_prompt_registration_error(
                        err_msg,
                        uc_schema=uc_schema,
                    )
                    attempt_failures.append(
                        {
                            "prompt_name": prompt_name,
                            "error": err_msg[:1500],
                            "classification": classification["reason"],
                            "missing_privileges": classification["missing_privileges"],
                            "remediation": classification["remediation"],
                        },
                    )
                    logger.warning(
                        "Prompt registration attempt failed for judge=%s name=%s cause=%s",
                        name,
                        prompt_name,
                        classification["reason"],
                        exc_info=True,
                    )
            if name not in registered:
                logger.error("Prompt registration failed for judge=%s", name)
                failed_judges.append(name)
                last_attempt = attempt_failures[-1] if attempt_failures else {}
                failed_details[name] = {
                    "attempted_names": [attempt.get("prompt_name", "") for attempt in attempt_failures],
                    "classification": last_attempt.get("classification", "unknown"),
                    "missing_privileges": last_attempt.get("missing_privileges", []),
                    "remediation": last_attempt.get("remediation", ""),
                    "last_error": last_attempt.get("error", ""),
                    "attempts": attempt_failures,
                }

    if register_registry:
        _all_extra: dict[str, dict[str, str]] = {}
        for category_label, prompt_dict, tag_type in [
            ("lever", LEVER_PROMPTS, "lever"),
            ("benchmark", BENCHMARK_PROMPTS, "benchmark"),
        ]:
            for name, template in prompt_dict.items():
                candidates = _prompt_name_candidates(uc_schema=uc_schema, domain=domain, judge_name=name)
                for prompt_name in candidates:
                    try:
                        version = mlflow.genai.register_prompt(
                            name=prompt_name,
                            template=template,
                            commit_message=f"Genie {category_label}: {name} (domain: {domain})",
                            tags={"domain": domain, "type": tag_type},
                        )
                        mlflow.genai.set_prompt_alias(
                            name=prompt_name,
                            alias=PROMPT_ALIAS,
                            version=version.version,
                        )
                        _all_extra[name] = {
                            "prompt_name": prompt_name,
                            "version": str(version.version),
                        }
                        _REGISTERED_PROMPT_NAMES[name] = prompt_name
                        logger.info("[Prompt Registry] %s %s v%s", category_label, prompt_name, version.version)
                        break
                    except Exception:
                        logger.debug(
                            "Prompt registration attempt failed for %s=%s name=%s",
                            category_label, name, prompt_name, exc_info=True,
                        )
                if name not in _all_extra:
                    logger.warning("Could not register %s prompt: %s", category_label, name)
        registered.update(_all_extra)

    active = mlflow.active_run()
    if active:
        _log_judge_prompt_artifacts(
            domain=domain,
            uc_schema=uc_schema,
            registered=registered,
            register_registry=register_registry,
            failed_judges=failed_judges,
            failed_details=failed_details,
        )
    else:
        run_name = f"register_prompts_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with mlflow.start_run(run_name=run_name):
            _log_judge_prompt_artifacts(
                domain=domain,
                uc_schema=uc_schema,
                registered=registered,
                register_registry=register_registry,
                failed_judges=failed_judges,
                failed_details=failed_details,
            )

    if register_registry and STRICT_PROMPT_REGISTRATION and failed_judges:
        cause_codes = sorted(
            {
                str(details.get("classification") or "unknown")
                for details in failed_details.values()
            }
        )
        missing_privileges = sorted(
            {
                str(priv)
                for details in failed_details.values()
                for priv in details.get("missing_privileges", [])
            }
        )
        root_cause_hint = ""
        if missing_privileges and uc_schema:
            root_cause_hint = (
                f" Root-cause hint: missing UC schema privileges {missing_privileges} on {uc_schema}."
            )
        if cause_codes:
            root_cause_hint += f" Detected cause classes: {cause_codes}."
        raise RuntimeError(
            "Prompt registration failed for judges: "
            + ", ".join(sorted(failed_judges))
            + "."
            + root_cause_hint,
        )

    total_prompt_count = len(JUDGE_PROMPTS) + len(LEVER_PROMPTS) + len(BENCHMARK_PROMPTS)
    logger.info(
        "Registered %d/%d prompts (judges=%d, levers=%d, benchmarks=%d, registry=%s)",
        len(registered), total_prompt_count,
        len(JUDGE_PROMPTS), len(LEVER_PROMPTS), len(BENCHMARK_PROMPTS),
        bool(register_registry and uc_schema),
    )
    return registered


def register_scorers_with_experiment(
    scorers: list,
    experiment_name: str,
) -> dict[str, Any]:
    """Register scorers with the MLflow experiment so they appear in the Judges tab.

    Iterates over *scorers*, calling ``.register(name=...)`` on each.
    Failures are logged but do **not** halt evaluation.
    """
    mlflow.set_experiment(experiment_name)

    registered: dict[str, Any] = {}
    failures: list[tuple[str, Exception]] = []

    for s in scorers:
        name = getattr(s, "name", getattr(s, "__name__", str(s)))
        try:
            reg = s.register(name=name)
            registered[name] = reg
            logger.info("[Scorer Registration] Registered %s", name)
        except ValueError as ve:
            if "already been registered" in str(ve):
                registered[name] = name
                logger.info("[Scorer Registration] %s already registered — skipping", name)
            else:
                failures.append((name, ve))
                logger.warning(
                    "[Scorer Registration] Failed to register %s: %s: %s",
                    name,
                    type(ve).__name__,
                    str(ve)[:400],
                )
        except Exception as exc:
            failures.append((name, exc))
            logger.warning(
                "[Scorer Registration] Failed to register %s: %s: %s",
                name,
                type(exc).__name__,
                str(exc)[:400],
            )

    logger.info(
        "Scorer registration complete: %d/%d registered",
        len(registered),
        len(scorers),
    )
    if failures:
        logger.warning(
            "Scorer registration failures: %s",
            ", ".join(f"{n}: {e}" for n, e in failures),
        )

    return registered


def _log_judge_prompt_artifacts(
    *,
    domain: str,
    uc_schema: str,
    registered: dict[str, dict],
    register_registry: bool,
    failed_judges: list[str] | None = None,
    failed_details: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Log judge definitions to the current run for run-level traceability."""
    judges_manifest: dict[str, Any] = {
        "domain": domain,
        "uc_schema": uc_schema,
        "register_registry": register_registry,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "failed_judges": failed_judges or [],
        "failed_judge_details": failed_details or {},
        "judges": [],
    }
    for name, template in JUDGE_PROMPTS.items():
        prompt_name = format_mlflow_template(PROMPT_NAME_TEMPLATE, uc_schema=uc_schema, judge_name=name) if uc_schema else name
        template_hash = hashlib.sha256(template.encode("utf-8")).hexdigest()
        prompt_meta = registered.get(name, {})
        judges_manifest["judges"].append(
            {
                "name": name,
                "prompt_name": prompt_meta.get("prompt_name", prompt_name),
                "prompt_version": prompt_meta.get("version"),
                "prompt_alias": PROMPT_ALIAS,
                "template_sha256": template_hash,
            }
        )
        mlflow.log_text(template, f"judge_prompts/{name}/template.txt")

    mlflow.log_dict(judges_manifest, "judge_prompts/manifest.json")
    mlflow.log_params(
        {
            "num_prompts": len(JUDGE_PROMPTS),
            "prompt_keys": ",".join(JUDGE_PROMPTS.keys()),
            "domain": domain,
            "judge_registry_logged_to_run": "true",
        },
    )
    mlflow.set_tags(
        {
            "traceability.judges_logged": "true",
            "traceability.judges_count": str(len(JUDGE_PROMPTS)),
            "traceability.uc_schema": uc_schema or "",
        },
    )


def _prompt_name_candidates(uc_schema: str, domain: str, judge_name: str) -> list[str]:
    """Try UC-qualified name first, then portable fallback names."""
    safe_domain = re.sub(r"[^a-zA-Z0-9_]+", "_", domain or "default").strip("_").lower() or "default"
    candidates: list[str] = []
    if uc_schema:
        candidates.append(format_mlflow_template(PROMPT_NAME_TEMPLATE, uc_schema=uc_schema, judge_name=judge_name))
        candidates.append(f"{uc_schema}.genie_opt_{safe_domain}_{judge_name}")
    candidates.append(f"genie_opt_{safe_domain}_{judge_name}")
    return list(dict.fromkeys(candidates))


def _configure_uc_trace_destination(
    *,
    experiment_id: str,
    uc_schema: str,
    warehouse_id: str,
) -> str:
    """Traces are stored in the MLflow experiment (default storage).

    UC OTEL trace storage is intentionally skipped: calling
    ``set_destination(UC)`` before the UC tables are fully provisioned
    causes all traces to be silently lost, breaking the evaluation UI.

    We also actively clear any stale UC destination that a previous run
    (or old code path) may have set in this process.
    """
    os.environ.pop("MLFLOW_TRACING_DESTINATION", None)
    try:
        mlflow.tracing.reset()
    except Exception:
        pass
    logger.info("Traces will be stored in MLflow experiment (default storage)")
    return ""


def _is_retryable_eval_exception(exc: Exception) -> bool:
    """Return True for known transient mlflow.genai.evaluate() harness failures.

    Known patterns (all originate inside mlflow.genai.evaluation.harness):
      1. ``eval_item.trace`` is None  ->  AttributeError: 'NoneType' ... 'info'
      2. ``eval_item.trace.info`` is None  ->  AttributeError on .assessments
      3. Transient gRPC / Spark Connect timeouts during scorer execution
    """
    message = str(exc).lower()
    full_tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    tb_text = "".join(full_tb).lower()

    if isinstance(exc, AttributeError):
        if "nonetype" in message and ("info" in message or "assessments" in message or "trace" in message):
            return True
        if "harness" in tb_text and "nonetype" in message:
            return True

    if "grpc" in message or "_multithreadedrendezvous" in message:
        return True

    if "harness" in tb_text and ("nonetype" in tb_text or "trace" in tb_text):
        if isinstance(exc, (AttributeError, TypeError)):
            return True

    return False


_HARNESS_PATCHED = False


def _patch_mlflow_harness_none_trace() -> None:
    """Monkey-patch MLflow internals that crash when eval_item.trace is None.

    MLflow >=3.4 has multiple code paths that access ``eval_item.trace.info``
    without guarding against ``trace`` being ``None``:

      1. ``harness._get_new_expectations`` (line ~394) — crashes on
         ``eval_item.trace.info.assessments``
      2. ``trace_utils.batch_link_traces_to_run`` (line ~964) — crashes on
         ``eval_result.eval_item.trace.info.trace_id`` in a list comprehension

    When the predict function involves complex I/O (Genie API + Spark Connect),
    the MLflow trace context can be lost, leaving ``trace = None``.  These patches
    allow evaluation to complete successfully even when traces are missing.
    """
    global _HARNESS_PATCHED
    if _HARNESS_PATCHED:
        return

    patched: list[str] = []

    try:
        import mlflow.genai.evaluation.harness as _harness_mod

        _orig_get_new_expectations = _harness_mod._get_new_expectations

        def _safe_get_new_expectations(eval_item: Any) -> list:
            if eval_item is None:
                return []
            trace = getattr(eval_item, "trace", None)
            if trace is None or getattr(trace, "info", None) is None:
                return []
            try:
                return _orig_get_new_expectations(eval_item)
            except Exception:
                return []

        _harness_mod._get_new_expectations = _safe_get_new_expectations  # type: ignore[assignment]
        patched.append("_get_new_expectations")
    except Exception:
        logger.warning("Could not patch _get_new_expectations", exc_info=True)

    try:
        import mlflow.genai.utils.trace_utils as _trace_utils_mod

        _orig_batch_link = _trace_utils_mod.batch_link_traces_to_run

        def _safe_batch_link_traces_to_run(*args: Any, **kwargs: Any) -> Any:
            eval_results = kwargs.get("eval_results") or (args[1] if len(args) > 1 else [])

            def _has_valid_trace(r: Any) -> bool:
                ei = getattr(r, "eval_item", None)
                if ei is None:
                    return False
                tr = getattr(ei, "trace", None)
                return tr is not None and getattr(tr, "info", None) is not None

            safe_results = [r for r in eval_results if _has_valid_trace(r)]
            if not safe_results:
                logger.info(
                    "batch_link_traces_to_run: %d/%d eval results have None traces, skipping linkage",
                    len(eval_results) - len(safe_results),
                    len(eval_results),
                )
                return None
            kwargs["eval_results"] = safe_results
            if args:
                return _orig_batch_link(args[0], **kwargs)
            return _orig_batch_link(**kwargs)

        _trace_utils_mod.batch_link_traces_to_run = _safe_batch_link_traces_to_run  # type: ignore[assignment]

        # Aggressively patch every module that imported the function directly,
        # scanning sys.modules to catch all references regardless of import style.
        import sys as _sys
        _patched_modules: list[str] = []
        for _mod_name, _mod_obj in list(_sys.modules.items()):
            if _mod_obj is None or _mod_obj is _trace_utils_mod:
                continue
            try:
                if hasattr(_mod_obj, "batch_link_traces_to_run"):
                    _existing = getattr(_mod_obj, "batch_link_traces_to_run")
                    if _existing is not _safe_batch_link_traces_to_run:
                        setattr(_mod_obj, "batch_link_traces_to_run", _safe_batch_link_traces_to_run)
                        _patched_modules.append(_mod_name)
            except Exception:
                pass
        if _patched_modules:
            logger.info(
                "Patched batch_link_traces_to_run in %d modules: %s",
                len(_patched_modules),
                ", ".join(_patched_modules),
            )

        patched.append("batch_link_traces_to_run")
    except Exception:
        logger.warning("Could not patch batch_link_traces_to_run", exc_info=True)

    _HARNESS_PATCHED = True
    if patched:
        logger.info("Patched MLflow None-trace safety: %s", ", ".join(patched))


def _run_evaluate_with_retries(
    *,
    evaluate_kwargs: dict[str, Any],
) -> tuple[Any, list[dict[str, Any]]]:
    """Run mlflow.genai.evaluate() with targeted retry for transient harness errors."""
    _patch_mlflow_harness_none_trace()

    attempts: list[dict[str, Any]] = []
    initial_workers = os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS")
    initial_skip_validation = os.getenv("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION")
    os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = "True"

    try:
        for attempt in range(1, max(1, EVAL_MAX_ATTEMPTS) + 1):
            workers = initial_workers if attempt == 1 else EVAL_SINGLE_WORKER_FALLBACK
            if workers is None:
                os.environ.pop("MLFLOW_GENAI_EVAL_MAX_WORKERS", None)
            else:
                os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = workers

            try:
                result = mlflow.genai.evaluate(**evaluate_kwargs)
                attempts.append(
                    {
                        "attempt": attempt,
                        "workers": os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS"),
                        "status": "success",
                    }
                )
                return result, attempts
            except Exception as exc:
                err_type = type(exc).__name__
                err_message = str(exc)
                attempts.append(
                    {
                        "attempt": attempt,
                        "workers": os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS"),
                        "status": "failed",
                        "error_type": err_type,
                        "error_message": err_message[:1000],
                        "traceback": traceback.format_exc(limit=30),
                    }
                )
                retryable = _is_retryable_eval_exception(exc)
                logger.exception(
                    "mlflow.genai.evaluate failed (attempt %d/%d, retryable=%s)",
                    attempt,
                    EVAL_MAX_ATTEMPTS,
                    retryable,
                )
                if attempt >= EVAL_MAX_ATTEMPTS or not retryable:
                    setattr(exc, "_eval_attempts", attempts)
                    raise
                time.sleep(EVAL_RETRY_SLEEP_SECONDS * attempt)
    finally:
        if initial_workers is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_MAX_WORKERS", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = initial_workers
        if initial_skip_validation is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = initial_skip_validation

    raise RuntimeError("Evaluation retry loop exhausted unexpectedly")


def _run_evaluate_sequential_fallback(
    *,
    evaluate_kwargs: dict[str, Any],
) -> Any:
    """Deterministic fallback path: evaluate one benchmark row at a time.

    Each row is wrapped in try/except so a single harness failure (e.g. a
    None-trace bug in mlflow) does not crash the entire evaluation.
    """
    _patch_mlflow_harness_none_trace()

    data = evaluate_kwargs.get("data")
    if not isinstance(data, pd.DataFrame):
        logger.info("Sequential fallback: converting non-DataFrame data to DataFrame")
        if hasattr(data, "to_dataframe"):
            data = data.to_dataframe()
        elif hasattr(data, "to_df"):
            data = data.to_df()
        else:
            raise RuntimeError("Sequential fallback requires DataFrame-convertible input")
        evaluate_kwargs = dict(evaluate_kwargs)
        evaluate_kwargs["data"] = data
    if data.empty:
        raise RuntimeError("Sequential fallback requires non-empty DataFrame input")

    metrics_accumulator: dict[str, list[float]] = {}
    row_tables: list[pd.DataFrame] = []
    skipped_count = 0
    total_rows = len(data)

    previous_workers = os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS")
    previous_skip = os.getenv("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION")
    os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "1"
    os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = "True"
    try:
        for row_idx in range(total_rows):
            row_df = data.iloc[[row_idx]].reset_index(drop=True)
            row_kwargs = dict(evaluate_kwargs)
            row_kwargs["data"] = row_df
            try:
                row_result = mlflow.genai.evaluate(**row_kwargs)
            except Exception as row_exc:
                logger.warning(
                    "Sequential fallback: row %d/%d failed, skipping: %s",
                    row_idx + 1,
                    total_rows,
                    str(row_exc)[:300],
                )
                skipped_count += 1
                continue

            if hasattr(row_result, "metrics"):
                for metric_name, value in row_result.metrics.items():
                    if isinstance(value, (int, float)):
                        metrics_accumulator.setdefault(metric_name, []).append(float(value))

            if hasattr(row_result, "tables") and isinstance(row_result.tables, dict):
                eval_table = row_result.tables.get("eval_results")
                if isinstance(eval_table, pd.DataFrame):
                    row_tables.append(eval_table)
    finally:
        if previous_workers is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_MAX_WORKERS", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = previous_workers
        if previous_skip is None:
            os.environ.pop("MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION", None)
        else:
            os.environ["MLFLOW_GENAI_EVAL_SKIP_TRACE_VALIDATION"] = previous_skip

    if skipped_count:
        logger.warning(
            "Sequential fallback completed with %d/%d rows skipped due to harness errors",
            skipped_count,
            total_rows,
        )

    metrics = {
        metric_name: (sum(values) / len(values))
        for metric_name, values in metrics_accumulator.items()
        if values
    }
    merged_eval_results = (
        pd.concat(row_tables, ignore_index=True) if row_tables else pd.DataFrame()
    )
    return SimpleNamespace(
        metrics=metrics,
        tables={"eval_results": merged_eval_results},
        skipped_count=skipped_count,
    )


def _collect_infra_eval_errors(rows: list[dict[str, Any]]) -> list[str]:
    """Extract infrastructure-like SQL errors from eval result rows.

    Only checks specific error/comparison columns — NOT scorer rationales or
    arbitrary string values, which frequently contain error keywords as part of
    legitimate judge explanations (e.g. "TABLE_OR_VIEW_NOT_FOUND" in a
    rationale describing why the Genie response was wrong).
    """
    infra_errors: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        candidates: list[str] = []
        outputs = row.get("outputs")
        if isinstance(outputs, dict):
            comparison = outputs.get("comparison")
            if isinstance(comparison, dict):
                err = comparison.get("error")
                err_type = comparison.get("error_type", "")
                if err and str(err_type) == "infrastructure":
                    candidates.append(str(err))
        for key in (
            "outputs/comparison/error",
            "comparison/error",
            "comparison.error",
        ):
            err = row.get(key)
            if not err:
                continue
            err_type_key = key.replace("/error", "/error_type").replace(".error", ".error_type")
            err_type = row.get(err_type_key, "")
            if str(err_type) == "infrastructure":
                candidates.append(str(err))

        for msg in candidates:
            if _is_infrastructure_sql_error(msg):
                infra_errors.append(msg[:500])

    seen: set[str] = set()
    deduped: list[str] = []
    for msg in infra_errors:
        if msg in seen:
            continue
        seen.add(msg)
        deduped.append(msg)
    return deduped


def create_evaluation_dataset(
    spark: SparkSession,
    benchmarks: list[dict],
    uc_schema: str,
    domain: str,
    space_id: str = "",
    catalog: str = "",
    gold_schema: str = "",
    experiment_id: str = "",
) -> Any | None:
    """Create or update the MLflow UC evaluation dataset from benchmarks.

    Uses ``merge_records`` (upsert by question_id) to preserve version history
    rather than dropping and recreating each run.

    Pass *experiment_id* to link the dataset to the experiment so it appears
    in the experiment's Datasets tab in the UI.
    """
    uc_table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    exp_ids = [experiment_id] if experiment_id else None
    try:
        try:
            eval_dataset = mlflow.genai.datasets.get_dataset(name=uc_table_name)
            logger.info("Reusing existing evaluation dataset: %s", uc_table_name)
        except Exception:
            create_kwargs: dict[str, Any] = {"name": uc_table_name}
            if exp_ids:
                create_kwargs["experiment_id"] = exp_ids
            eval_dataset = mlflow.genai.datasets.create_dataset(**create_kwargs)
            logger.info(
                "Created new evaluation dataset: %s (experiment_id=%s)",
                uc_table_name, exp_ids,
            )
        records = []
        for b in benchmarks:
            _expected_sql = b.get("expected_sql", "")
            expectations = {
                "expected_response": _expected_sql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"), _expected_sql,
                ),
                "category": b.get("category", ""),
                "source": b.get("source", ""),
                "provenance": b.get("provenance", ""),
                "validation_status": b.get("validation_status", ""),
                "validation_reason_code": b.get("validation_reason_code", ""),
                "validation_error": b.get("validation_error", ""),
                "correction_source": b.get("correction_source", ""),
            }
            expectations = {k: v for k, v in expectations.items() if v is not None}
            records.append(
                {
                    "inputs": {
                        "question_id": b.get("id", ""),
                        "question": b["question"],
                        "space_id": space_id,
                        "expected_sql": b.get("expected_sql", ""),
                        "catalog": catalog,
                        "gold_schema": gold_schema,
                    },
                    "expectations": expectations,
                }
            )
        eval_dataset.merge_records(records)
        logger.info("UC Evaluation Dataset: %s (%d records merged)", uc_table_name, len(records))
        return eval_dataset
    except Exception:
        logger.exception("UC dataset creation failed for %s", uc_table_name)
        return None


def _drop_benchmark_table(spark: SparkSession, uc_table_name: str) -> None:
    """Best-effort DROP of the benchmark table to clear stale rows."""
    try:
        parts = uc_table_name.split(".")
        quoted = ".".join(f"`{p.strip('`')}`" for p in parts)
        spark.sql(f"DROP TABLE IF EXISTS {quoted}")
        logger.info("Dropped stale benchmark table %s", uc_table_name)
    except Exception:
        logger.warning("Could not drop benchmark table %s (may not exist)", uc_table_name, exc_info=True)


_JUDGE_ORDER = [
    "syntax_validity", "schema_accuracy", "logical_accuracy",
    "semantic_equivalence", "completeness", "response_quality",
    "asset_routing", "result_correctness", "arbiter",
]


def _get_nested(row: dict, *paths: str, default: Any = "") -> Any:
    """Try multiple key paths (both flattened and nested dict forms)."""
    for path in paths:
        if "/" in path:
            val = row.get(path)
            if val not in (None, "", {}, []):
                return val
            parts = path.split("/", 1)
            parent = row.get(parts[0])
            if isinstance(parent, dict) and len(parts) == 2:
                val = parent.get(parts[1])
                if val not in (None, "", {}, []):
                    return val
        else:
            val = row.get(path)
            if val not in (None, "", {}, []):
                return val
    return default


def _print_eval_summary(
    rows: list[dict],
    scores_100: dict[str, float],
    thresholds_passed: bool,
    iteration: int,
    eval_scope: str,
    total_questions: int,
) -> None:
    """Print a nicely formatted per-question evaluation summary to stdout."""
    lines: list[str] = []
    lines.append("")
    header = (
        f"  EVALUATION SUMMARY — Iteration {iteration} | "
        f"Scope: {eval_scope} | Questions: {total_questions}"
    )
    width = max(len(header) + 4, 78)
    lines.append("=" * width)
    lines.append(header)
    lines.append("=" * width)

    if rows:
        first_keys = sorted(rows[0].keys())
        lines.append(f"[DEBUG] First row keys: {first_keys[:30]}")

    for qi, row in enumerate(rows, 1):
        _request = row.get("request", {})
        if isinstance(_request, str):
            try:
                _request = json.loads(_request)
            except (json.JSONDecodeError, ValueError):
                _request = {}
        if not isinstance(_request, dict):
            _request = {}

        _response = row.get("response", {})
        if isinstance(_response, str):
            try:
                _response = json.loads(_response)
            except (json.JSONDecodeError, ValueError):
                _response = {}
        if not isinstance(_response, dict):
            _response = {}

        qid = (
            _request.get("question_id")
            or _get_nested(row, "inputs/question_id", "question_id")
            or f"q{qi}"
        )
        question = (
            _request.get("question")
            or _get_nested(row, "inputs/question", "question")
            or ""
        )
        genie_sql = (
            _response.get("response")
            or _get_nested(row, "outputs/response")
            or "(none)"
        )
        status = (
            _response.get("status")
            or _get_nested(row, "outputs/status", "status")
            or row.get("state", "?")
        )
        gt_sql = (
            _request.get("expected_sql")
            or _get_nested(
                row, "expectations/expected_response", "expected_response",
                "inputs/expected_sql", "expected_sql",
            )
            or "(none)"
        )

        cmp = _response.get("comparison", {})
        if not isinstance(cmp, dict):
            cmp = {}
        if not cmp:
            outputs_val = row.get("outputs")
            if isinstance(outputs_val, dict):
                cmp = outputs_val.get("comparison", {})
            if not cmp:
                cmp_raw = row.get("outputs/comparison", {})
                cmp = cmp_raw if isinstance(cmp_raw, dict) else {}

        match_str = "YES" if cmp.get("match") else "NO"
        match_type = cmp.get("match_type", "n/a")

        lines.append("")
        lines.append(f"--- Q{qi}: {qid} " + "-" * max(0, width - len(f"--- Q{qi}: {qid} ") - 1))
        lines.append(f"| Question:  \"{question}\"")
        lines.append(f"|")
        lines.append(f"| Genie SQL:")
        lines.append(f"|   {genie_sql}")
        lines.append(f"| Genie Status: {status}")

        analysis = (
            _response.get("analysis_text")
            or _get_nested(row, "outputs/analysis_text")
            or None
        )
        if analysis:
            lines.append(f"| Genie Analysis: {str(analysis)[:200]}")

        lines.append(f"|")
        lines.append(f"| Ground Truth SQL:")
        lines.append(f"|   {gt_sql}")
        lines.append(f"|")
        lines.append(
            f"| Result Comparison: Match: {match_str} ({match_type}) | "
            f"GT rows: {cmp.get('gt_rows', '?')} | Genie rows: {cmp.get('genie_rows', '?')}"
        )
        if cmp.get("gt_hash") or cmp.get("genie_hash"):
            lines.append(
                f"|   GT hash: {cmp.get('gt_hash', 'n/a')} | "
                f"Genie hash: {cmp.get('genie_hash', 'n/a')}"
            )
        if cmp.get("error"):
            lines.append(f"|   Error: {cmp['error']}")
        if cmp.get("gt_sample"):
            lines.append("|   GT Result Sample (first 5 rows):")
            for sample_line in str(cmp["gt_sample"]).strip().split("\n")[:6]:
                lines.append(f"|     {sample_line}")
        if cmp.get("genie_sample"):
            lines.append("|   Genie Result Sample (first 5 rows):")
            for sample_line in str(cmp["genie_sample"]).strip().split("\n")[:6]:
                lines.append(f"|     {sample_line}")
        lines.append(f"|")
        lines.append(f"| Judge Verdicts:")

        for judge in _JUDGE_ORDER:
            val = row.get(f"{judge}/value", row.get(judge, ""))
            val_str = str(val) if val else "n/a"
            rationale = row.get(f"{judge}/rationale", "")
            if isinstance(rationale, str) and rationale:
                short_rat = rationale.split("\n")[0][:120]
            else:
                short_rat = ""

            if val_str.lower() in ("yes", "true", "1", "1.0", "skipped"):
                verdict_label = "PASS" if val_str.lower() != "skipped" else val_str
            elif val_str.lower() in ("no", "false", "0", "0.0"):
                verdict_label = "FAIL"
            elif val_str in ("genie_correct", "both_correct"):
                verdict_label = val_str
            elif val_str in ("ground_truth_correct", "neither_correct"):
                verdict_label = val_str
            else:
                verdict_label = val_str or "n/a"

            rat_suffix = f"  -- {short_rat}" if short_rat and verdict_label not in ("PASS", "n/a") else ""
            lines.append(f"|   {judge:<24s} {verdict_label}{rat_suffix}")

        lines.append("-" * width)

    lines.append("")
    lines.append("--- SCORE SUMMARY " + "-" * max(0, width - 19))
    for judge in _JUDGE_ORDER:
        score = scores_100.get(judge)
        if score is None:
            continue
        threshold = DEFAULT_THRESHOLDS.get(judge, 0.0)
        passed = score >= threshold
        marker = "" if passed else "  <<<"
        lines.append(
            f"|   {judge:<24s} {score:6.1f}  (threshold: {threshold:.1f})  "
            f"{'PASS' if passed else 'FAIL'}{marker}"
        )
    arbiter_counts: dict[str, int] = {
        "both_correct": 0, "genie_correct": 0,
        "ground_truth_correct": 0, "neither_correct": 0, "skipped": 0,
    }
    for row in rows:
        av = str(row.get("arbiter/value", row.get("arbiter", "skipped"))).lower()
        if av in arbiter_counts:
            arbiter_counts[av] += 1
        else:
            arbiter_counts["skipped"] += 1
    arbiter_total = sum(arbiter_counts.values())
    lines.append(f"|")
    lines.append(f"|   Arbiter verdicts ({arbiter_total} questions):")
    for verdict in ("both_correct", "genie_correct", "ground_truth_correct", "neither_correct", "skipped"):
        cnt = arbiter_counts[verdict]
        pct = (cnt / arbiter_total * 100) if arbiter_total else 0
        lines.append(f"|     {verdict:<24s} {cnt:3d}  ({pct:5.1f}%)")

    adj_accuracy, _adj_correct, adj_failures, adj_excluded = (
        _compute_arbiter_adjusted_accuracy(rows)
    )
    rc_raw = scores_100.get("result_correctness", 0.0)
    lines.append(f"|")
    lines.append(f"|   Overall accuracy: {adj_accuracy:.1f}%  (result_correctness raw: {rc_raw:.1f}%)")
    if adj_excluded:
        lines.append(f"|   Excluded (GT infra): {adj_excluded}")
    lines.append(f"|   Thresholds met: {'YES' if thresholds_passed else 'NO'}")
    if adj_failures:
        lines.append(f"|   Failed questions: {adj_failures}")
    lines.append("-" * width)

    print("\n".join(lines))


def run_evaluation(
    space_id: str,
    experiment_name: str,
    iteration: int,
    benchmarks: list[dict],
    domain: str,
    model_id: str | None,
    eval_scope: str,
    predict_fn: Any,
    scorers: list[Any],
    *,
    spark: SparkSession | None = None,
    catalog: str = "",
    gold_schema: str = "",
    uc_schema: str = "",
    warehouse_id: str = "",
    patched_objects: list[str] | None = None,
    reference_sqls: dict[str, str] | None = None,
    metric_view_names: set[str] | None = None,
    metric_view_measures: dict[str, set[str]] | None = None,
    optimization_run_id: str = "",
    lever: int | None = None,
) -> dict:
    """Run ``mlflow.genai.evaluate()`` and return structured results.

    Args:
        reference_sqls: Optional ``{question_id: sql}`` from a prior iteration.
            When provided the ``repeatability_scorer`` is automatically added
            and ``previous_sql`` is injected into each row's expectations.

    Returns dict with: run_id, run_name, experiment_id, iteration,
    overall_accuracy, per_judge, thresholds_passed, failure_question_ids,
    arbiter_verdicts, etc.
    """
    import re as _re
    domain = _re.sub(r"[^a-z0-9_]+", "_", domain.lower()).strip("_") or "default"

    mlflow.set_experiment(experiment_name)
    exp = mlflow.get_experiment_by_name(experiment_name)
    mlflow_model_id = (
        model_id
        if isinstance(model_id, str) and model_id.startswith("m-")
        else None
    )

    trace_destination = _configure_uc_trace_destination(
        experiment_id=exp.experiment_id if exp else "",
        uc_schema=uc_schema,
        warehouse_id=warehouse_id or os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
    )

    scope_filtered = filter_benchmarks_by_scope(benchmarks, eval_scope, patched_objects)
    if not scope_filtered and benchmarks:
        scope_filtered = benchmarks

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = format_mlflow_template(RUN_NAME_TEMPLATE, iteration=iteration, timestamp=ts)

    with mlflow.start_run(run_name=run_name) as run:
        _version_tags: dict[str, str] = {
            "genie.space_id": space_id,
            "genie.domain": domain,
            "genie.iteration": str(iteration),
            "genie.eval_scope": eval_scope,
        }
        if optimization_run_id:
            _version_tags["genie.optimization_run_id"] = optimization_run_id
        if lever is not None:
            _version_tags["genie.lever"] = str(lever)
        else:
            _version_tags["genie.lever"] = "baseline"
        mlflow.set_tags(_version_tags)

        _eval_dataset_obj = None
        try:
            _ds_table = f"{uc_schema}.genie_benchmarks_{domain}" if uc_schema and domain else ""
            if _ds_table:
                _eval_dataset_obj = mlflow.genai.datasets.get_dataset(name=_ds_table)
                logger.info("Loaded evaluation dataset '%s' for linking", _ds_table)
        except Exception as _ds_err:
            logger.warning(
                "Failed to load evaluation dataset '%s': %s — "
                "will use plain DataFrame (dataset won't appear in experiment Datasets tab)",
                _ds_table, _ds_err,
            )

        if spark is not None:
            known_functions = _load_known_functions(spark, catalog, gold_schema)
            filtered, quarantined_benchmarks, precheck_counts = _precheck_benchmarks_for_eval(
                benchmarks=scope_filtered,
                spark=spark,
                catalog=catalog,
                gold_schema=gold_schema,
                known_functions=known_functions,
                metric_view_names=metric_view_names,
                metric_view_measures=metric_view_measures,
            )
        else:
            filtered = list(scope_filtered)
            quarantined_benchmarks = []
            precheck_counts = {
                "invalid_benchmark_count": 0,
                "permission_blocked_count": 0,
                "unresolved_column_count": 0,
                "bad_join_key_count": 0,
            }

        has_reference_sqls = bool(reference_sqls)
        if has_reference_sqls:
            from genie_space_optimizer.optimization.scorers import repeatability_scorer as _rep_scorer
            if _rep_scorer not in scorers:
                scorers = list(scorers) + [_rep_scorer]

        eval_records = []
        for b in filtered:
            qid = b.get("id", "")
            _esql = b.get("expected_sql", "")
            expectations = {
                "expected_response": _esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"), _esql,
                ),
            }
            if has_reference_sqls:
                expectations["previous_sql"] = (reference_sqls or {}).get(qid, "")
            eval_records.append(
                {
                    "inputs": {
                        "question_id": qid,
                        "question": b["question"],
                        "space_id": space_id,
                        "expected_sql": b.get("expected_sql", ""),
                        "catalog": catalog,
                        "gold_schema": gold_schema,
                    },
                    "expectations": expectations,
                }
            )
        eval_data = pd.DataFrame(eval_records)

        run_params = {
            "space_id": space_id,
            "iteration": iteration,
            "dataset": f"{domain}_benchmarks",
            "eval_scope": eval_scope,
            "num_scorers": len(scorers),
            "domain": domain,
            "benchmark_count": len(filtered),
            "scope_benchmark_count": len(scope_filtered),
            "invalid_benchmark_count": precheck_counts["invalid_benchmark_count"],
            "permission_blocked_count": precheck_counts["permission_blocked_count"],
            "unresolved_column_count": precheck_counts["unresolved_column_count"],
            "bad_join_key_count": precheck_counts["bad_join_key_count"],
        }
        if model_id:
            run_params["model_id"] = model_id
        if catalog:
            run_params["catalog"] = catalog
        if gold_schema:
            run_params["gold_schema"] = gold_schema
        if uc_schema:
            run_params["uc_schema"] = uc_schema
        if trace_destination:
            run_params["trace_destination"] = trace_destination
        mlflow.log_params(run_params)
        if quarantined_benchmarks:
            mlflow.log_dict(
                {
                    "total_scoped_benchmarks": len(scope_filtered),
                    "evaluable_benchmark_count": len(filtered),
                    "counts": precheck_counts,
                    "quarantined": quarantined_benchmarks,
                },
                "evaluation_runtime/benchmark_precheck.json",
            )
        if not filtered:
            msg = (
                "No evaluable benchmarks remain after strict pre-eval SQL + routine checks. "
                f"Counts: {precheck_counts}"
            )
            mlflow.log_dict(
                {
                    "status": "failed",
                    "error_type": "NoEvaluableBenchmarks",
                    "error_message": msg,
                    "quarantined": quarantined_benchmarks[:50],
                    "counts": precheck_counts,
                },
                "evaluation_failure/no_evaluable_benchmarks.json",
            )
            raise RuntimeError(msg)
        # Ensure every evaluation run carries a full, queryable judge manifest.
        register_judge_prompts(
            uc_schema=uc_schema,
            domain=domain,
            experiment_name=experiment_name,
            register_registry=(iteration == 0 and eval_scope == "full"),
        )

        if iteration == 0 and eval_scope == "full":
            register_scorers_with_experiment(scorers, experiment_name)

        if mlflow_model_id:
            mlflow.set_active_model(model_id=mlflow_model_id)

        evaluate_kwargs: dict[str, Any] = {
            "predict_fn": predict_fn,
            "data": _eval_dataset_obj if _eval_dataset_obj is not None else eval_data,
            "scorers": scorers,
        }
        if mlflow_model_id:
            evaluate_kwargs["model_id"] = mlflow_model_id

        eval_attempts: list[dict[str, Any]] = []
        try:
            eval_result, eval_attempts = _run_evaluate_with_retries(
                evaluate_kwargs=evaluate_kwargs,
            )
        except Exception as exc:
            attempts_from_exc = getattr(exc, "_eval_attempts", None)
            if isinstance(attempts_from_exc, list):
                eval_attempts = attempts_from_exc
            is_retryable = _is_retryable_eval_exception(exc)
            if is_retryable:
                logger.warning(
                    "Falling back to sequential evaluation after retryable harness failure: %s",
                    str(exc)[:400],
                )
                eval_result = _run_evaluate_sequential_fallback(
                    evaluate_kwargs=evaluate_kwargs,
                )
                eval_attempts.append(
                    {
                        "attempt": len(eval_attempts) + 1,
                        "workers": "1",
                        "status": "success",
                        "mode": "sequential_fallback",
                    }
                )
                mlflow.set_tag("evaluation_mode", "sequential_fallback")
            else:
                failure_payload = {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:2000],
                    "attempts": eval_attempts,
                }
                try:
                    mlflow.log_dict(
                        failure_payload,
                        "evaluation_failure/evaluate_failure.json",
                    )
                    mlflow.set_tags(
                        {
                            "evaluation_status": "failed",
                            "evaluation_error_type": type(exc).__name__,
                        },
                    )
                except Exception:
                    logger.warning("Could not log evaluation failure artifact", exc_info=True)
                raise

        if eval_attempts:
            mlflow.log_dict(
                {"attempts": eval_attempts},
                "evaluation_runtime/evaluate_attempts.json",
            )
            mlflow.log_param(
                "evaluate_attempt_count",
                str(len(eval_attempts)),
            )
        harness_retry_count = max(0, len(eval_attempts) - 1)
        mlflow.log_metric("harness_retry_count", float(harness_retry_count))

        per_judge: dict[str, float] = {}
        for metric_name in eval_result.metrics:
            if "/mean" in metric_name:
                judge_name = metric_name.replace("/mean", "")
                per_judge[judge_name] = eval_result.metrics[metric_name]

        scores_100 = normalize_scores(per_judge)
        thresholds_passed = all_thresholds_met(scores_100)
        mlflow.log_metric("thresholds_passed", 1.0 if thresholds_passed else 0.0)

        arbiter_verdicts: dict[str, int] = {
            "genie_correct": 0,
            "ground_truth_correct": 0,
            "both_correct": 0,
            "neither_correct": 0,
            "skipped": 0,
        }
        arbiter_actions: list[dict[str, str]] = []
        rows_for_output: list[dict] = []

        _STRIP_COLS = {"trace", "assessments", "spans", "trace_metadata"}
        cached_feedback = _drain_scorer_feedback_cache()

        if hasattr(eval_result, "tables") and "eval_results" in eval_result.tables:
            results_df = eval_result.tables["eval_results"]

            assessment_map = _extract_assessments_from_traces(results_df)

            for row_idx, (_, row) in enumerate(results_df.iterrows()):
                row_dict = {}
                for col in results_df.columns:
                    if col in _STRIP_COLS:
                        continue
                    val = row[col]
                    if hasattr(val, "item"):
                        val = val.item()
                    if not isinstance(val, (str, int, float, bool, type(None), list, dict)):
                        val = str(val)
                    row_dict[col] = val

                for judge_name, adata in assessment_map.get(row_idx, {}).items():
                    rat_key = f"{judge_name}/rationale"
                    meta_key = f"{judge_name}/metadata"
                    if rat_key not in row_dict and adata.get("rationale"):
                        row_dict[rat_key] = adata["rationale"]
                    if meta_key not in row_dict and adata.get("metadata"):
                        row_dict[meta_key] = adata["metadata"]

                _req_raw = row_dict.get("request") or {}
                if isinstance(_req_raw, str):
                    try:
                        _req_raw = json.loads(_req_raw)
                    except (json.JSONDecodeError, TypeError):
                        _req_raw = {}
                _req_kw = _req_raw.get("kwargs", {}) if isinstance(_req_raw, dict) else {}
                qid = (
                    row_dict.get("inputs/question_id")
                    or (row_dict.get("inputs") or {}).get("question_id", "")
                    or row_dict.get("question_id")
                    or _req_kw.get("question_id")
                    or (_req_raw.get("question_id") if isinstance(_req_raw, dict) else None)
                    or ""
                )
                if qid and qid in cached_feedback:
                    for judge_name, fb_data in cached_feedback[qid].items():
                        rat_key = f"{judge_name}/rationale"
                        meta_key = f"{judge_name}/metadata"
                        if rat_key not in row_dict and fb_data.get("rationale"):
                            row_dict[rat_key] = fb_data["rationale"]
                        if meta_key not in row_dict and fb_data.get("metadata"):
                            row_dict[meta_key] = fb_data["metadata"]

                for col_name in list(row_dict.keys()):
                    if col_name.endswith("/rationale"):
                        jname = col_name.rsplit("/rationale", 1)[0]
                        if jname.startswith("feedback/"):
                            jname = jname[len("feedback/"):]
                        mkey = f"{jname}/metadata"
                        if mkey not in row_dict:
                            parsed = _parse_asi_from_rationale(str(row_dict.get(col_name, "")))
                            if parsed:
                                row_dict[mkey] = parsed

                _ASI_FLAT_FIELDS = ("failure_type", "blame_set", "wrong_clause", "counterfactual_fix", "severity", "confidence")
                for col_name in list(row_dict.keys()):
                    if not col_name.endswith("/metadata"):
                        continue
                    jname = col_name.removesuffix("/metadata")
                    meta = row_dict[col_name]
                    if not isinstance(meta, dict):
                        continue
                    for fld in _ASI_FLAT_FIELDS:
                        flat_key = f"metadata/{jname}/{fld}"
                        if flat_key not in row_dict and meta.get(fld) is not None:
                            row_dict[flat_key] = meta[fld]

                rows_for_output.append(row_dict)

                av = str(row.get("arbiter/value", row.get("arbiter", "skipped")))
                if av in arbiter_verdicts:
                    arbiter_verdicts[av] += 1
                else:
                    arbiter_verdicts["skipped"] += 1

                if av == "genie_correct":
                    _gc_sql = (
                        row.get("outputs/response")
                        or (row.get("outputs") or {}).get("response", "")
                    )
                    _gc_question = (
                        row.get("inputs/question")
                        or (row.get("inputs") or {}).get("question", "")
                    )
                    if _gc_sql and _gc_question:
                        arbiter_actions.append({
                            "question": str(_gc_question),
                            "new_expected_sql": str(_gc_sql),
                            "verdict": "genie_correct",
                        })

        question_failure_artifacts: list[dict[str, Any]] = []
        for row in rows_for_output:
            error_val = (
                row.get("outputs/comparison/error")
                or row.get("comparison/error")
                or row.get("comparison.error")
            )
            if not error_val:
                continue
            _fa_req = row.get("request") or {}
            if isinstance(_fa_req, str):
                try:
                    _fa_req = json.loads(_fa_req)
                except (json.JSONDecodeError, TypeError):
                    _fa_req = {}
            _fa_kw = _fa_req.get("kwargs", {}) if isinstance(_fa_req, dict) else {}
            question_failure_artifacts.append(
                {
                    "question_id": str(
                        row.get("inputs/question_id")
                        or row.get("question_id")
                        or _fa_kw.get("question_id")
                        or (_fa_req.get("question_id") if isinstance(_fa_req, dict) else None)
                        or ""
                    ),
                    "expected_sql": str(
                        row.get("inputs/expected_sql")
                        or _fa_kw.get("expected_sql")
                        or (_fa_req.get("expected_sql") if isinstance(_fa_req, dict) else None)
                        or ""
                    ),
                    "generated_sql": str(row.get("outputs/response") or row.get("response") or ""),
                    "error_type": str(
                        row.get("outputs/comparison/error_type")
                        or row.get("comparison/error_type")
                        or row.get("comparison.error_type")
                        or ""
                    ),
                    "sqlstate": str(
                        row.get("outputs/comparison/sqlstate")
                        or row.get("comparison/sqlstate")
                        or row.get("comparison.sqlstate")
                        or ""
                    ),
                    "error": str(error_val)[:1000],
                }
            )
        if question_failure_artifacts:
            mlflow.log_dict(
                {
                    "count": len(question_failure_artifacts),
                    "items": question_failure_artifacts,
                },
                "evaluation_runtime/question_failure_artifacts.json",
            )

        infra_errors = _collect_infra_eval_errors(rows_for_output)
        if FAIL_ON_INFRA_EVAL_ERRORS and infra_errors:
            mlflow.log_dict(
                {
                    "status": "failed",
                    "reason": "infrastructure_sql_error",
                    "errors": infra_errors,
                },
                "evaluation_failure/infrastructure_sql_errors.json",
            )
            mlflow.set_tags(
                {
                    "evaluation_status": "failed",
                    "evaluation_error_type": "infrastructure_sql_error",
                },
            )
            raise RuntimeError(
                "Infrastructure SQL errors detected during evaluation: "
                + " | ".join(infra_errors[:3]),
            )

        arbiter_adjusted_accuracy, arbiter_adjusted_correct, failure_ids, excluded_count = (
            _compute_arbiter_adjusted_accuracy(rows_for_output)
        )

        # Arbiter-adjust result_correctness so detect_regressions sees true
        # signal instead of raw hash-mismatch noise.
        if rows_for_output:
            _rc_total = _rc_correct = 0
            for _rc_row in rows_for_output:
                _rc_val = str(_rc_row.get("result_correctness/value", "")).lower()
                if _rc_val == "excluded":
                    continue
                _rc_total += 1
                if _rc_val in ("yes", "true", "1", "1.0"):
                    _rc_correct += 1
                elif str(_rc_row.get("arbiter/value", "")).lower() in _ARBITER_CORRECT_VERDICTS:
                    _rc_correct += 1
            if _rc_total > 0:
                per_judge["result_correctness"] = _rc_correct / _rc_total
                scores_100 = normalize_scores(per_judge)
                thresholds_passed = all_thresholds_met(scores_100)

        row_unresolved_column_count = sum(
            1
            for artifact in question_failure_artifacts
            if _classify_sql_validation_error(artifact.get("error", "")) == "unknown_column"
        )
        row_permission_blocked_count = sum(
            1
            for artifact in question_failure_artifacts
            if (
                artifact.get("error_type") == "permission_blocked"
                or _classify_sql_validation_error(artifact.get("error", "")) == "permission_blocked"
            )
        )
        unresolved_column_count = (
            precheck_counts["unresolved_column_count"] + row_unresolved_column_count
        )
        permission_blocked_count = (
            precheck_counts["permission_blocked_count"] + row_permission_blocked_count
        )
        mlflow.log_metrics({
            "overall_accuracy": arbiter_adjusted_accuracy,
            "correct_count": float(arbiter_adjusted_correct),
            "total_questions": float(len(filtered)),
            "failure_count": float(len(failure_ids)),
            "excluded_count": float(excluded_count),
        })
        mlflow.set_tags(
            {
                "evaluation_status": "success",
                "invalid_benchmark_count": str(precheck_counts["invalid_benchmark_count"]),
                "permission_blocked_count": str(permission_blocked_count),
                "unresolved_column_count": str(unresolved_column_count),
                "harness_retry_count": str(harness_retry_count),
            }
        )

        trace_map: dict[str, str] = {}
        for _row in rows_for_output:
            _qid = (
                _row.get("question_id")
                or _row.get("inputs/question_id")
                or (_row.get("inputs") or {}).get("question_id", "")
            )
            _tid = _row.get("trace_id")
            if _qid and _tid:
                trace_map[_qid] = str(_tid)

        output: dict[str, Any] = {
            "run_id": run.info.run_id,
            "mlflow_run_id": run.info.run_id,
            "run_name": run_name,
            "experiment_id": exp.experiment_id if exp else "",
            "iteration": iteration,
            "overall_accuracy": arbiter_adjusted_accuracy,
            "total_questions": len(filtered),
            "correct_count": arbiter_adjusted_correct,
            "scores": scores_100,
            "thresholds_met": thresholds_passed,
            "thresholds_passed": thresholds_passed,
            "per_judge": per_judge,
            "failures": failure_ids,
            "failure_question_ids": failure_ids,
            "remaining_failures": failure_ids,
            "arbiter_verdicts": arbiter_verdicts,
            "arbiter_actions": arbiter_actions,
            "model_id": model_id,
            "rows": rows_for_output,
            "trace_map": trace_map,
            "invalid_benchmark_count": precheck_counts["invalid_benchmark_count"],
            "permission_blocked_count": permission_blocked_count,
            "unresolved_column_count": unresolved_column_count,
            "harness_retry_count": harness_retry_count,
            "excluded_count": excluded_count,
            "quarantined_benchmarks": quarantined_benchmarks,
        }

    logger.info(
        "Evaluation complete: %s — accuracy=%.1f%%, thresholds=%s",
        run_name,
        output["overall_accuracy"],
        "PASS" if thresholds_passed else "FAIL",
    )

    if EVAL_DEBUG:
        _print_eval_summary(
            rows_for_output, scores_100, thresholds_passed,
            iteration, eval_scope, len(filtered),
        )

    return output


# ── Repeatability Evaluation ──────────────────────────────────────────


REPEATABILITY_RUN_NAME_TEMPLATE = "genie_repeatability_iter{iteration}_{timestamp}"


def run_repeatability_evaluation(
    space_id: str,
    experiment_name: str,
    iteration: int,
    benchmarks: list[dict],
    domain: str,
    reference_sqls: dict[str, str],
    predict_fn: Any,
    *,
    spark: SparkSession | None = None,
    catalog: str = "",
    gold_schema: str = "",
    uc_schema: str = "",
    model_id: str | None = None,
    run_label: str = "",
) -> dict:
    """Run a repeatability evaluation through ``mlflow.genai.evaluate()``.

    Re-queries Genie via *predict_fn* and uses a repeatability scorer to
    compare the new SQL against *reference_sqls* (``{question_id: sql}``
    from a prior iteration).  Produces full MLflow traces and judge verdicts.

    Args:
        reference_sqls: Mapping of question_id → SQL from a previous run.
        run_label: Optional suffix for the run name (e.g. "final_1").
    """
    from genie_space_optimizer.optimization.scorers import make_repeatability_scorers

    mlflow.set_experiment(experiment_name)
    exp = mlflow.get_experiment_by_name(experiment_name)

    trace_destination = _configure_uc_trace_destination(
        experiment_id=exp.experiment_id if exp else "",
        uc_schema=uc_schema,
        warehouse_id=os.getenv("GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID", ""),
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{run_label}" if run_label else ""
    run_name = f"genie_repeatability_iter{iteration}_{ts}{suffix}"

    scorers = make_repeatability_scorers()

    mlflow_model_id = (
        model_id
        if isinstance(model_id, str) and model_id.startswith("m-")
        else None
    )

    eval_records = []
    for b in benchmarks:
        qid = b.get("id", "")
        prev_sql = reference_sqls.get(qid, "")
        eval_records.append(
            {
                "inputs": {
                    "question_id": qid,
                    "question": b["question"],
                    "space_id": space_id,
                    "expected_sql": b.get("expected_sql", ""),
                    "catalog": catalog,
                    "gold_schema": gold_schema,
                },
                "expectations": {
                    "expected_response": b.get("expected_sql", ""),
                    "expected_asset": _normalize_expected_asset(
                        b.get("expected_asset", "TABLE"),
                        b.get("expected_sql", ""),
                    ),
                    "previous_sql": prev_sql,
                },
            }
        )
    eval_data = pd.DataFrame(eval_records)

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params(
            {
                "space_id": space_id,
                "iteration": iteration,
                "eval_type": "repeatability",
                "domain": domain,
                "benchmark_count": len(benchmarks),
                "reference_sql_count": sum(1 for v in reference_sqls.values() if v),
                "run_label": run_label or "standard",
            }
        )

        if mlflow_model_id:
            mlflow.set_active_model(model_id=mlflow_model_id)

        evaluate_kwargs: dict[str, Any] = {
            "predict_fn": predict_fn,
            "data": eval_data,
            "scorers": scorers,
        }
        if mlflow_model_id:
            evaluate_kwargs["model_id"] = mlflow_model_id

        try:
            eval_result, eval_attempts = _run_evaluate_with_retries(
                evaluate_kwargs=evaluate_kwargs,
            )
        except Exception as exc:
            if _is_retryable_eval_exception(exc):
                logger.warning(
                    "Repeatability eval falling back to sequential: %s",
                    str(exc)[:300],
                )
                eval_result = _run_evaluate_sequential_fallback(
                    evaluate_kwargs=evaluate_kwargs,
                )
            else:
                logger.error("Repeatability evaluation failed: %s", str(exc)[:500])
                raise

        per_judge: dict[str, float] = {}
        for metric_name in eval_result.metrics:
            if "/mean" in metric_name:
                judge_name = metric_name.replace("/mean", "")
                per_judge[judge_name] = eval_result.metrics[metric_name]

        repeatability_raw = per_judge.get("repeatability", 0.0)
        repeatability_pct = repeatability_raw * 100 if repeatability_raw <= 1.0 else repeatability_raw

        rows_for_output: list[dict] = []
        _STRIP_COLS_REP = {"trace", "assessments", "spans", "trace_metadata"}
        if hasattr(eval_result, "tables") and "eval_results" in eval_result.tables:
            rep_df = eval_result.tables["eval_results"]
            rep_assessment_map = _extract_assessments_from_traces(rep_df)
            for row_idx, (_, row) in enumerate(rep_df.iterrows()):
                row_dict = {}
                for col in rep_df.columns:
                    if col in _STRIP_COLS_REP:
                        continue
                    val = row[col]
                    if hasattr(val, "item"):
                        val = val.item()
                    if not isinstance(val, (str, int, float, bool, type(None), list, dict)):
                        val = str(val)
                    row_dict[col] = val
                for judge_name, adata in rep_assessment_map.get(row_idx, {}).items():
                    rat_key = f"{judge_name}/rationale"
                    meta_key = f"{judge_name}/metadata"
                    if rat_key not in row_dict and adata.get("rationale"):
                        row_dict[rat_key] = adata["rationale"]
                    if meta_key not in row_dict and adata.get("metadata"):
                        row_dict[meta_key] = adata["metadata"]

                for col_name in list(row_dict.keys()):
                    if col_name.endswith("/rationale"):
                        jname = col_name.rsplit("/rationale", 1)[0]
                        if jname.startswith("feedback/"):
                            jname = jname[len("feedback/"):]
                        mkey = f"{jname}/metadata"
                        if mkey not in row_dict:
                            parsed = _parse_asi_from_rationale(str(row_dict.get(col_name, "")))
                            if parsed:
                                row_dict[mkey] = parsed

                rows_for_output.append(row_dict)

        mlflow.log_metric("repeatability_pct", repeatability_pct)
        mlflow.set_tags(
            {
                "evaluation_type": "repeatability",
                "repeatability_pct": f"{repeatability_pct:.1f}",
                "iteration": str(iteration),
            }
        )

    logger.info(
        "Repeatability evaluation complete: %s — repeatability=%.1f%%",
        run_name,
        repeatability_pct,
    )

    _rep_lines = [
        f"\n-- REPEATABILITY EVALUATION: {run_name} " + "-" * 30,
        f"  |  Repeatability:  {repeatability_pct:.1f}%",
        f"  |  Questions:      {len(benchmarks)}",
        f"  |  Reference SQLs: {sum(1 for v in reference_sqls.values() if v)}",
    ]
    for _judge, _score in per_judge.items():
        _disp = _score * 100 if _score <= 1.0 else _score
        _rep_lines.append(f"  |  {_judge}: {_disp:.1f}")
    _rep_lines.append("-" * 60)
    print("\n".join(_rep_lines))

    rep_trace_map: dict[str, str] = {}
    for _row in rows_for_output:
        _qid = (
            _row.get("question_id")
            or _row.get("inputs/question_id")
            or (_row.get("inputs") or {}).get("question_id", "")
        )
        _tid = _row.get("trace_id")
        if _qid and _tid:
            rep_trace_map[_qid] = str(_tid)

    return {
        "run_id": run.info.run_id,
        "mlflow_run_id": run.info.run_id,
        "run_name": run_name,
        "repeatability_pct": repeatability_pct,
        "per_judge": per_judge,
        "rows": rows_for_output,
        "scores": normalize_scores(per_judge),
        "trace_map": rep_trace_map,
    }


def extract_reference_sqls(eval_result: dict) -> dict[str, str]:
    """Extract ``{question_id: generated_sql}`` from an evaluation output.

    Used to build *reference_sqls* for subsequent repeatability evaluations.
    Handles both flat column names (``inputs/question_id``) and nested
    dicts (``request.kwargs.question_id``, ``response.response``).
    """
    ref: dict[str, str] = {}
    rows = eval_result.get("rows", [])
    for row in rows:
        _req = row.get("request") or {}
        _req_kwargs = _req.get("kwargs", {}) if isinstance(_req, dict) else {}
        _resp = row.get("response") or {}
        qid = (
            row.get("inputs/question_id")
            or (row.get("inputs", {}) or {}).get("question_id", "")
            or _req_kwargs.get("question_id", "")
            or row.get("question_id", "")
        )
        sql = (
            row.get("outputs/response")
            or (row.get("outputs", {}) or {}).get("response", "")
            or (_resp.get("response", "") if isinstance(_resp, dict) else "")
        )
        if qid:
            ref[str(qid)] = str(sql or "")
    return ref


# ── Benchmark Extraction from Genie Space ──────────────────────────────


def extract_genie_space_benchmarks(
    config: dict,
    spark: SparkSession,
    catalog: str = "",
    schema: str = "",
) -> list[dict]:
    """Extract curated benchmark questions from a Genie Space config.

    Sources (in priority order):
      1. ``instructions.example_question_sqls`` — curated Q+SQL pairs the space
         owner has defined. These have the highest fidelity.
      2. ``sample_questions`` — natural-language-only questions from the space
         config (no expected SQL).

    Each returned dict has ``question``, ``expected_sql`` (may be empty for
    sample-only questions), ``source`` = ``"genie_space"``, and
    ``expected_asset``.
    """
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    parsed_space = config.get("_parsed_space", {})
    if not isinstance(parsed_space, dict):
        parsed_space = {}
    instr = parsed_space.get("instructions", {})
    if not isinstance(instr, dict):
        instr = {}

    benchmarks: list[dict] = []
    seen_questions: set[str] = set()

    example_qs = instr.get("example_question_sqls", [])
    for ex in (example_qs if isinstance(example_qs, list) else []):
        if not isinstance(ex, dict):
            continue
        q_raw = ex.get("question", "")
        if isinstance(q_raw, list):
            q_raw = q_raw[0] if q_raw else ""
        question = str(q_raw).strip()
        if not question:
            continue
        q_lower = question.lower()
        if q_lower in seen_questions:
            continue
        seen_questions.add(q_lower)

        sql_raw = ex.get("sql", "")
        if isinstance(sql_raw, list):
            sql_raw = sql_raw[0] if sql_raw else ""
        expected_sql = str(sql_raw).strip()

        if expected_sql:
            is_valid, err = validate_ground_truth_sql(
                expected_sql, spark, catalog=catalog, gold_schema=schema,
            )
            if not is_valid:
                logger.warning(
                    "Genie space example_question_sql failed validation: %s — %s",
                    question[:60], err,
                )
                expected_sql = ""

        benchmarks.append({
            "question": question,
            "expected_sql": expected_sql,
            "expected_asset": detect_asset_type(expected_sql) if expected_sql else "TABLE",
            "category": "curated",
            "required_tables": [],
            "required_columns": [],
            "expected_facts": [],
            "source": "genie_space",
        })

    bench_section = parsed_space.get("benchmarks", {})
    if not isinstance(bench_section, dict):
        bench_section = {}
    bench_questions = bench_section.get("questions", [])
    for bq in (bench_questions if isinstance(bench_questions, list) else []):
        if not isinstance(bq, dict):
            continue
        q_raw = bq.get("question", [])
        if isinstance(q_raw, list):
            q_raw = q_raw[0] if q_raw else ""
        question = str(q_raw).strip()
        if not question:
            continue
        q_lower = question.lower()
        if q_lower in seen_questions:
            continue
        seen_questions.add(q_lower)

        expected_sql = ""
        answers = bq.get("answer", [])
        if isinstance(answers, list):
            for ans in answers:
                if isinstance(ans, dict) and ans.get("format") == "SQL":
                    content = ans.get("content", [])
                    if isinstance(content, list):
                        expected_sql = "".join(str(c) for c in content).strip()
                    elif isinstance(content, str):
                        expected_sql = content.strip()
                    break

        if expected_sql:
            is_valid, err = validate_ground_truth_sql(
                expected_sql, spark, catalog=catalog, gold_schema=schema,
            )
            if not is_valid:
                logger.warning(
                    "Genie space benchmark question failed SQL validation: %s — %s",
                    question[:60], err,
                )
                expected_sql = ""

        benchmarks.append({
            "question": question,
            "expected_sql": expected_sql,
            "expected_asset": detect_asset_type(expected_sql) if expected_sql else "TABLE",
            "category": "curated",
            "required_tables": [],
            "required_columns": [],
            "expected_facts": [],
            "source": "genie_space",
        })

    logger.info(
        "Extracted %d curated benchmarks from Genie space config "
        "(%d with SQL, %d without SQL)",
        len(benchmarks),
        sum(1 for b in benchmarks if b["expected_sql"]),
        sum(1 for b in benchmarks if not b["expected_sql"]),
    )
    return benchmarks


# ── Benchmark Generation ────────────────────────────────────────────────


def _build_valid_assets_context(config: dict) -> str:
    """Build an explicit allowlist of Genie space data assets for the LLM prompt."""
    lines: list[str] = []
    for tbl in config.get("_tables", []):
        lines.append(f"- TABLE: {tbl}")
    for mv in config.get("_metric_views", []):
        lines.append(f"- METRIC VIEW: {mv}")
    for fn in config.get("_functions", []):
        lines.append(f"- FUNCTION: {fn}")
    return "\n".join(lines) if lines else "(no assets configured)"


def _build_schema_contexts(
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
) -> dict[str, str]:
    """Build the schema context strings for benchmark prompts."""
    tables_context = "\n".join(
        f"- {c.get('table_name', '')}.{c.get('column_name', '')} ({c.get('data_type', '')}): {c.get('comment', '')}"
        for c in uc_columns
    )
    mvs = config.get("_metric_views", [])
    metric_views_context = "\n".join(f"- {mv}" for mv in mvs) if mvs else "(none)"

    tvfs = config.get("_functions", [])
    tvfs_context = "\n".join(
        f"- {r.get('routine_name', '')}: {r.get('routine_definition', '')[:200]}"
        for r in uc_routines
    ) if uc_routines else (
        "\n".join(f"- {t}" for t in tvfs) if tvfs else "(none)"
    )

    instructions = config.get("_instructions", [])
    instructions_context = "\n".join(
        f"- {i.get('text', i) if isinstance(i, dict) else i}" for i in instructions
    ) if instructions else "(none)"

    sample_questions = config.get("_parsed_space", {}).get("sample_questions", [])
    sample_questions_context = "\n".join(
        f"- {q.get('question', q) if isinstance(q, dict) else q}"
        for q in sample_questions
    ) if sample_questions else "(none)"

    columns_by_table: dict[str, list[str]] = {}
    for c in uc_columns:
        if not isinstance(c, dict):
            continue
        tbl = str(c.get("table_name") or "").strip()
        col = str(c.get("column_name") or "").strip()
        dtype = str(c.get("data_type") or "").strip().upper()
        if tbl and col:
            entry = f"{col} ({dtype})" if dtype else col
            columns_by_table.setdefault(tbl, []).append(entry)
    column_allowlist_lines: list[str] = []
    for tbl_name in sorted(columns_by_table):
        column_allowlist_lines.append(f"{tbl_name}: {', '.join(columns_by_table[tbl_name])}")
    column_allowlist = "\n".join(column_allowlist_lines) if column_allowlist_lines else "(no columns)"

    return {
        "tables_context": tables_context,
        "metric_views_context": metric_views_context,
        "tvfs_context": tvfs_context,
        "instructions_context": instructions_context,
        "sample_questions_context": sample_questions_context,
        "valid_assets_context": _build_valid_assets_context(config),
        "column_allowlist": column_allowlist,
    }


def _validate_benchmark_sql(
    sql: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    execute: bool = False,
) -> tuple[bool, str]:
    """Validate a benchmark's expected_sql. Returns (is_valid, error)."""
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    resolved = resolve_sql(sql, catalog, schema)
    sanitized = sanitize_sql(resolved)
    if not sanitized.strip():
        return False, "Empty SQL"
    return validate_ground_truth_sql(
        sanitized, spark, catalog=catalog, gold_schema=schema, execute=execute,
    )


def _attempt_benchmark_correction(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    invalid_benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: SparkSession,
    allowlist: dict[str, Any],
) -> list[dict]:
    """Send invalid benchmarks back to the LLM for correction.

    Returns corrected benchmarks that pass validation.
    """
    if not invalid_benchmarks:
        return []

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)
    benchmarks_to_fix = json.dumps(
        [
            {
                "question": b["question"],
                "original_expected_sql": b["expected_sql"],
                "error": b.get("validation_error", "unknown"),
            }
            for b in invalid_benchmarks
        ],
        indent=2,
    )

    prompt = format_mlflow_template(
        BENCHMARK_CORRECTION_PROMPT,
        valid_assets_context=ctx["valid_assets_context"],
        tables_context=ctx["tables_context"],
        column_allowlist=ctx.get("column_allowlist", "(no columns)"),
        metric_views_context=ctx.get("metric_views_context", "None"),
        tvfs_context=ctx.get("tvfs_context", "None"),
        benchmarks_to_fix=benchmarks_to_fix,
    )

    try:
        response = _call_llm_for_scoring(w, prompt)
        corrections: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    except Exception:
        logger.warning("Benchmark correction LLM call failed", exc_info=True)
        return []

    corrected: list[dict] = []
    for c in corrections:
        sql = c.get("expected_sql")
        if not sql or c.get("unfixable_reason"):
            logger.info("Benchmark unfixable: %s — %s", c.get("question", "")[:60], c.get("unfixable_reason", ""))
            continue
        metadata_ok, _reason_code, reason_message = _enforce_metadata_constraints(
            benchmark=c,
            sql=str(sql),
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            logger.warning(
                "Corrected benchmark violates metadata constraints: %s — %s",
                c.get("question", "")[:60],
                reason_message,
            )
            continue
        is_valid, err = _validate_benchmark_sql(sql, spark, catalog, schema)
        if is_valid:
            c["provenance"] = "auto_corrected"
            c["validation_status"] = "valid"
            c["validation_reason_code"] = "ok"
            c["validation_error"] = None
            c["correction_source"] = "llm_correction"
            corrected.append(c)
        else:
            logger.warning(
                "Corrected benchmark still invalid: %s — %s", c.get("question", "")[:60], err,
            )
    return corrected


MAX_CORRECTION_ROUNDS = 2

_SQL_REFERENCE_PATTERN = re.compile(
    r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
    r"(`[^`]+`\.`[^`]+`\.`[^`]+`"
    r"|[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*)",
    re.IGNORECASE,
)


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9_]", "", (value or "").lower())


def _identifier_candidates(value: str) -> set[str]:
    cleaned = (value or "").replace("`", "").strip().lower()
    if not cleaned:
        return set()
    parts = [p for p in cleaned.split(".") if p]
    candidates = {cleaned}
    if parts:
        candidates.add(parts[-1])
    if len(parts) >= 2:
        candidates.add(".".join(parts[-2:]))
    return candidates


def _build_metadata_allowlist(
    *,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
) -> dict[str, Any]:
    allowed_assets: set[str] = set()
    allowed_columns: set[str] = set()
    normalized_to_column: dict[str, str] = {}
    allowed_routines: set[str] = set()

    for key in ("_tables", "_metric_views", "_functions"):
        for raw in config.get(key, []) if isinstance(config.get(key), list) else []:
            if not raw:
                continue
            allowed_assets.update(_identifier_candidates(str(raw)))

    for col in uc_columns:
        if not isinstance(col, dict):
            continue
        col_name = str(col.get("column_name") or "").strip()
        table_name = str(col.get("table_name") or "").strip()
        if col_name:
            allowed_columns.add(col_name.lower())
            normalized_to_column.setdefault(_normalize_name(col_name), col_name)
        if table_name and col_name:
            fq_col = f"{table_name}.{col_name}".lower()
            allowed_columns.add(fq_col)
            normalized_to_column.setdefault(_normalize_name(fq_col), f"{table_name}.{col_name}")

    for routine in uc_routines:
        if not isinstance(routine, dict):
            continue
        raw_name = str(
            routine.get("routine_name")
            or routine.get("specific_name")
            or ""
        ).strip()
        if not raw_name:
            continue
        allowed_routines.update(_identifier_candidates(raw_name))

    for fn in config.get("_functions", []) if isinstance(config.get("_functions"), list) else []:
        allowed_routines.update(_identifier_candidates(str(fn)))

    return {
        "assets": allowed_assets,
        "columns": allowed_columns,
        "column_index": normalized_to_column,
        "routines": allowed_routines,
    }


def _extract_sql_asset_references(sql: str) -> set[str]:
    refs: set[str] = set()
    for match in _SQL_REFERENCE_PATTERN.finditer(sql or ""):
        refs.update(_identifier_candidates(match.group(1)))
    return refs


def _compute_asset_coverage(
    benchmarks: list[dict],
    config: dict,
) -> dict[str, Any]:
    """Identify which Genie Space assets have/lack benchmark coverage.

    Collects covered assets from ``required_tables`` and ``expected_sql``
    SQL references across all benchmarks, then diffs against the full asset
    list from the Genie Space config.

    Returns a dict with ``covered``, ``uncovered_tables``,
    ``uncovered_mvs``, and ``uncovered_functions`` sets (leaf-name normalised).
    """
    covered: set[str] = set()
    for b in benchmarks:
        for tbl in b.get("required_tables", []):
            covered.update(_identifier_candidates(str(tbl)))
        sql = str(b.get("expected_sql") or "")
        if sql:
            covered.update(_extract_sql_asset_references(sql))

    def _leaf(name: str) -> str:
        parts = name.replace("`", "").strip().split(".")
        return parts[-1].lower() if parts else ""

    all_tables = {_leaf(t) for t in config.get("_tables", []) if t}
    all_mvs = {_leaf(m) for m in config.get("_metric_views", []) if m}
    all_functions = {_leaf(f) for f in config.get("_functions", []) if f}

    covered_leaves = {_leaf(c) for c in covered if c}

    return {
        "covered": covered_leaves,
        "uncovered_tables": all_tables - covered_leaves,
        "uncovered_mvs": all_mvs - covered_leaves,
        "uncovered_functions": all_functions - covered_leaves,
    }


def _fill_coverage_gaps(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_routines: list[dict],
    benchmarks: list[dict],
    catalog: str,
    schema: str,
    spark: "SparkSession",
    allowlist: dict[str, Any],
    domain: str,
    existing_questions: set[str],
) -> list[dict]:
    """Generate targeted benchmarks for Genie Space assets with zero coverage.

    Runs after the main generation pipeline. Identifies uncovered assets via
    ``_compute_asset_coverage``, then makes a single LLM call asking for 1-2
    questions per uncovered asset.  Results go through the same metadata
    constraint and SQL validation pipeline as normal benchmarks.

    Returns only validated gap-fill benchmarks (may be empty).
    """
    soft_cap = int(TARGET_BENCHMARK_COUNT * COVERAGE_GAP_SOFT_CAP_FACTOR)
    if len(benchmarks) >= soft_cap:
        logger.info(
            "Skipping coverage gap-fill: benchmark count %d already at soft cap %d",
            len(benchmarks), soft_cap,
        )
        return []

    coverage = _compute_asset_coverage(benchmarks, config)
    uncovered_tables = coverage["uncovered_tables"]
    uncovered_mvs = coverage["uncovered_mvs"]
    uncovered_functions = coverage["uncovered_functions"]

    if not uncovered_tables and not uncovered_mvs and not uncovered_functions:
        logger.info("All Genie Space assets already covered by benchmarks")
        return []

    # Prioritise MVs and TVFs (higher routing-issue risk), then tables.
    budget = soft_cap - len(benchmarks)
    ordered_uncovered: list[str] = []
    for mv in sorted(uncovered_mvs):
        ordered_uncovered.append(f"METRIC VIEW: {mv}")
    for fn in sorted(uncovered_functions):
        ordered_uncovered.append(f"FUNCTION: {fn}")
    for tbl in sorted(uncovered_tables):
        ordered_uncovered.append(f"TABLE: {tbl}")

    # Each uncovered asset targets ~2 questions; trim to budget.
    max_assets = max(budget // 2, 1)
    targeted = ordered_uncovered[:max_assets]

    logger.info(
        "Coverage gap-fill: %d uncovered assets (%d tables, %d MVs, %d functions). "
        "Targeting %d within budget of %d.",
        len(ordered_uncovered), len(uncovered_tables),
        len(uncovered_mvs), len(uncovered_functions),
        len(targeted), budget,
    )

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)
    existing_q_lines = "\n".join(f"- {q}" for q in sorted(existing_questions)) or "(none)"
    uncovered_lines = "\n".join(f"- {a}" for a in targeted)

    prompt = format_mlflow_template(
        BENCHMARK_COVERAGE_GAP_PROMPT,
        domain=domain,
        categories=json.dumps(BENCHMARK_CATEGORIES),
        uncovered_assets=uncovered_lines,
        existing_questions=existing_q_lines,
        **ctx,
    )

    try:
        response = _call_llm_for_scoring(w, prompt)
        raw: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])
    except Exception:
        logger.warning("Coverage gap-fill LLM call failed", exc_info=True)
        return []

    valid: list[dict] = []
    for b in raw:
        if not isinstance(b, dict):
            continue
        expected_sql = str(b.get("expected_sql", "") or "")
        if not expected_sql:
            continue
        q_lower = str(b.get("question", "") or "").lower().strip()
        if q_lower in existing_questions:
            continue

        required_tables = b.get("required_tables", [])
        if not isinstance(required_tables, list):
            required_tables = []
        required_columns = b.get("required_columns", [])
        if not isinstance(required_columns, list):
            required_columns = []
        expected_facts = b.get("expected_facts", [])
        if not isinstance(expected_facts, list):
            expected_facts = []

        benchmark: dict[str, Any] = {
            "question": b.get("question", ""),
            "expected_sql": expected_sql,
            "expected_asset": _normalize_expected_asset(
                b.get("expected_asset", "TABLE"), expected_sql,
            ),
            "category": b.get("category", ""),
            "required_tables": [str(t) for t in required_tables],
            "required_columns": [str(c) for c in required_columns],
            "expected_facts": [str(f) for f in expected_facts],
            "source": "llm_generated",
            "provenance": "coverage_gap_fill",
            "validation_status": "valid",
            "validation_reason_code": "ok",
            "validation_error": None,
            "correction_source": "",
        }

        metadata_ok, _reason_code, _reason_msg = _enforce_metadata_constraints(
            benchmark=benchmark,
            sql=expected_sql,
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            logger.debug(
                "Gap-fill benchmark failed metadata constraints: %s",
                str(benchmark.get("question", ""))[:60],
            )
            continue

        is_valid, err = _validate_benchmark_sql(expected_sql, spark, catalog, schema)
        if is_valid:
            valid.append(benchmark)
        else:
            logger.debug(
                "Gap-fill benchmark failed SQL validation: %s — %s",
                str(benchmark.get("question", ""))[:60], err,
            )

    logger.info(
        "Coverage gap-fill complete: %d valid out of %d generated for %d uncovered assets",
        len(valid), len(raw), len(targeted),
    )
    return valid


def _suggest_column_name(column: str, allowed_index: dict[str, str]) -> str | None:
    if not column:
        return None
    normalized = _normalize_name(column)
    if not normalized:
        return None
    exact = allowed_index.get(normalized)
    if exact:
        return exact
    candidates = list(allowed_index.keys())
    if not candidates:
        return None
    closest = get_close_matches(normalized, candidates, n=1, cutoff=0.72)
    if not closest:
        return None
    return allowed_index.get(closest[0])


def _apply_metadata_field_drift_corrections(
    *,
    sql: str,
    required_columns: list[str],
    allowed_index: dict[str, str],
) -> tuple[str, list[dict[str, str]]]:
    corrected_sql = sql
    applied: list[dict[str, str]] = []
    seen: set[str] = set()

    for col in required_columns:
        token = str(col or "").strip()
        if not token:
            continue
        col_leaf = token.split(".")[-1]
        if not col_leaf:
            continue
        key = col_leaf.lower()
        if key in seen:
            continue
        seen.add(key)

        suggestion = _suggest_column_name(col_leaf, allowed_index)
        if not suggestion:
            continue
        suggestion_leaf = suggestion.split(".")[-1]
        if suggestion_leaf.lower() == col_leaf.lower():
            continue

        pattern = re.compile(rf"(?i)\b{re.escape(col_leaf)}\b")
        updated_sql, count = pattern.subn(suggestion_leaf, corrected_sql)
        if count > 0:
            corrected_sql = updated_sql
            applied.append(
                {
                    "from": col_leaf,
                    "to": suggestion_leaf,
                    "reason": "metadata_field_drift",
                }
            )

    return corrected_sql, applied


def _enforce_metadata_constraints(
    *,
    benchmark: dict,
    sql: str,
    allowlist: dict[str, Any],
    catalog: str,
    schema: str,
) -> tuple[bool, str, str]:
    refs = _extract_sql_asset_references(sql)
    unknown_refs = sorted(ref for ref in refs if ref not in allowlist["assets"])
    if unknown_refs:
        return (
            False,
            "unknown_asset",
            f"SQL references assets not found in metadata: {unknown_refs[:5]}",
        )

    required_tables = benchmark.get("required_tables", [])
    if isinstance(required_tables, list):
        bad_required_tables: list[str] = []
        for item in required_tables:
            candidates = _identifier_candidates(str(item))
            if candidates and not any(c in allowlist["assets"] for c in candidates):
                bad_required_tables.append(str(item))
        if bad_required_tables:
            return (
                False,
                "unknown_asset",
                f"required_tables contains unknown assets: {bad_required_tables[:5]}",
            )

    required_columns = benchmark.get("required_columns", [])
    if isinstance(required_columns, list):
        bad_columns: list[str] = []
        for col in required_columns:
            raw = str(col or "").strip()
            if not raw:
                continue
            col_candidates = _identifier_candidates(raw)
            if any(c in allowlist["columns"] for c in col_candidates):
                continue
            leaf = raw.split(".")[-1].lower()
            if leaf in allowlist["columns"]:
                continue
            bad_columns.append(raw)
        if bad_columns:
            return (
                False,
                "unknown_column",
                f"required_columns contains unknown metadata fields: {bad_columns[:8]}",
            )

    called_functions = _extract_sql_function_calls(sql, catalog, schema)
    unknown_functions = sorted(fn for fn in called_functions if fn not in allowlist["routines"])
    if unknown_functions:
        return (
            False,
            "unknown_routine",
            f"SQL references routines not found in metadata: {unknown_functions[:5]}",
        )

    return True, "ok", ""


def generate_benchmarks(
    w: WorkspaceClient,
    config: dict,
    uc_columns: list[dict],
    uc_tags: list[dict],
    uc_routines: list[dict],
    domain: str,
    catalog: str,
    schema: str,
    spark: SparkSession,
    target_count: int = TARGET_BENCHMARK_COUNT,
    genie_space_benchmarks: list[dict] | None = None,
    existing_benchmarks: list[dict] | None = None,
) -> list[dict]:
    """Generate benchmark questions via LLM from Genie Space context.

    Pipeline:
      1. Start with curated Genie space benchmarks (if provided)
      2. Calculate how many synthetic benchmarks to generate to reach target
      3. Build schema context from actual Genie Space assets + UC metadata
      4. Call LLM with BENCHMARK_GENERATION_PROMPT (includes valid asset allowlist)
      5. Enforce strict metadata constraints (assets/routines/required fields)
      6. Run deterministic metadata drift auto-correction (field suggestions)
      7. Validate each expected_sql via EXPLAIN + table existence check
      8. Send remaining invalid benchmarks to correction LLM (bounded retries)
      9. Persist provenance + validation metadata per benchmark record

    Args:
        existing_benchmarks: Previously validated benchmarks to keep. When
            provided, these are carried forward and the generation targets
            only the gap (``target_count - len(existing_benchmarks)``).
    """
    curated = genie_space_benchmarks or []
    _existing = existing_benchmarks or []
    curated_questions = {b.get("question", "").lower().strip() for b in curated}
    existing_questions = {b.get("question", "").lower().strip() for b in _existing}
    curated_questions |= existing_questions
    synthetic_target = max(target_count - len(curated) - len(_existing), 5)
    allowlist = _build_metadata_allowlist(
        config=config,
        uc_columns=uc_columns,
        uc_routines=uc_routines,
    )

    if curated:
        logger.info(
            "Starting with %d curated Genie space benchmarks (%d with SQL). "
            "Generating %d synthetic to reach target of %d.",
            len(curated),
            sum(1 for b in curated if b.get("expected_sql")),
            synthetic_target,
            target_count,
        )

    ctx = _build_schema_contexts(config, uc_columns, uc_routines)

    all_existing = list(curated) + list(_existing)
    existing_questions_context = ""
    if all_existing:
        existing_questions_context = (
            "\n\n## Already Covered Questions (do NOT duplicate these)\n"
            + "\n".join(f"- {b.get('question', '')}" for b in all_existing)
        )

    prompt = format_mlflow_template(
        BENCHMARK_GENERATION_PROMPT,
        domain=domain,
        target_count=synthetic_target,
        categories=json.dumps(BENCHMARK_CATEGORIES),
        **ctx,
    )
    if existing_questions_context:
        prompt += existing_questions_context

    response = _call_llm_for_scoring(w, prompt)
    raw_benchmarks: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])

    valid_benchmarks: list[dict] = []
    invalid_benchmarks: list[dict] = []
    accepted_questions: set[str] = set()

    def _register_valid(candidate: dict) -> None:
        question = str(candidate.get("question") or "").strip().lower()
        if not question or question in accepted_questions or question in curated_questions:
            return
        accepted_questions.add(question)
        valid_benchmarks.append(candidate)

    for b in raw_benchmarks:
        if not isinstance(b, dict):
            continue
        expected_sql = str(b.get("expected_sql", "") or "")
        if not expected_sql:
            continue
        q_lower = str(b.get("question", "") or "").lower().strip()
        if q_lower in curated_questions:
            logger.debug("Skipping synthetic duplicate of curated question: %s", q_lower[:50])
            continue

        required_tables = b.get("required_tables", [])
        if not isinstance(required_tables, list):
            required_tables = []
        required_columns = b.get("required_columns", [])
        if not isinstance(required_columns, list):
            required_columns = []
        expected_facts = b.get("expected_facts", [])
        if not isinstance(expected_facts, list):
            expected_facts = []

        benchmark: dict[str, Any] = {
            "question": b.get("question", ""),
            "expected_sql": expected_sql,
            "expected_asset": _normalize_expected_asset(
                b.get("expected_asset", "TABLE"), expected_sql,
            ),
            "category": b.get("category", ""),
            "required_tables": [str(t) for t in required_tables],
            "required_columns": [str(c) for c in required_columns],
            "expected_facts": [str(f) for f in expected_facts],
            "source": "llm_generated",
            "provenance": "synthetic",
            "validation_status": "valid",
            "validation_reason_code": "ok",
            "validation_error": None,
            "correction_source": "",
        }

        metadata_ok, reason_code, reason_message = _enforce_metadata_constraints(
            benchmark=benchmark,
            sql=expected_sql,
            allowlist=allowlist,
            catalog=catalog,
            schema=schema,
        )
        if not metadata_ok:
            # Deterministic correction for common field drift before LLM-based correction.
            if reason_code == "unknown_column":
                corrected_sql, replacements = _apply_metadata_field_drift_corrections(
                    sql=expected_sql,
                    required_columns=[str(c) for c in benchmark.get("required_columns", [])],
                    allowed_index=allowlist["column_index"],
                )
                if replacements and corrected_sql != expected_sql:
                    candidate = dict(benchmark)
                    candidate["expected_sql"] = corrected_sql
                    candidate["provenance"] = "auto_corrected"
                    candidate["correction_source"] = "metadata_suggestion"
                    candidate["field_drift_fixes"] = replacements
                    candidate_ok, _, candidate_msg = _enforce_metadata_constraints(
                        benchmark=candidate,
                        sql=corrected_sql,
                        allowlist=allowlist,
                        catalog=catalog,
                        schema=schema,
                    )
                    if candidate_ok:
                        is_candidate_valid, candidate_err = _validate_benchmark_sql(
                            corrected_sql, spark, catalog, schema,
                        )
                        if is_candidate_valid:
                            candidate["validation_status"] = "valid"
                            candidate["validation_reason_code"] = "ok"
                            candidate["validation_error"] = None
                            _register_valid(candidate)
                            continue
                        reason_message = candidate_err
                    else:
                        reason_message = candidate_msg

            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = reason_code
            benchmark["validation_error"] = reason_message
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark failed metadata constraints: %s — %s",
                str(benchmark.get("question", ""))[:60],
                reason_message,
            )
            continue

        is_valid, err = _validate_benchmark_sql(
            expected_sql, spark, catalog, schema, execute=True,
        )
        if is_valid:
            benchmark["validation_status"] = "valid"
            benchmark["validation_reason_code"] = "ok"
            benchmark["validation_error"] = None
            _register_valid(benchmark)
        else:
            benchmark["validation_status"] = "invalid"
            benchmark["validation_reason_code"] = _classify_sql_validation_error(err)
            benchmark["validation_error"] = err
            invalid_benchmarks.append(benchmark)
            logger.warning(
                "Benchmark failed validation: %s — %s",
                str(benchmark.get("question", ""))[:60], err,
            )

    for correction_round in range(MAX_CORRECTION_ROUNDS):
        if not invalid_benchmarks:
            break
        logger.info(
            "Correction round %d: attempting to fix %d invalid benchmarks",
            correction_round + 1, len(invalid_benchmarks),
        )
        metadata_corrected: list[dict] = []
        still_invalid: list[dict] = []
        for invalid in invalid_benchmarks:
            expected_sql = str(invalid.get("expected_sql") or "")
            if not expected_sql:
                still_invalid.append(invalid)
                continue
            corrected_sql, replacements = _apply_metadata_field_drift_corrections(
                sql=expected_sql,
                required_columns=[str(c) for c in invalid.get("required_columns", [])],
                allowed_index=allowlist["column_index"],
            )
            if not replacements or corrected_sql == expected_sql:
                still_invalid.append(invalid)
                continue
            candidate = dict(invalid)
            candidate["expected_sql"] = corrected_sql
            candidate["field_drift_fixes"] = replacements
            candidate["provenance"] = "auto_corrected"
            candidate["correction_source"] = "metadata_suggestion_loop"
            candidate_ok, candidate_reason, candidate_message = _enforce_metadata_constraints(
                benchmark=candidate,
                sql=corrected_sql,
                allowlist=allowlist,
                catalog=catalog,
                schema=schema,
            )
            if not candidate_ok:
                candidate["validation_status"] = "invalid"
                candidate["validation_reason_code"] = candidate_reason
                candidate["validation_error"] = candidate_message
                still_invalid.append(candidate)
                continue
            candidate_valid, candidate_err = _validate_benchmark_sql(
                corrected_sql, spark, catalog, schema,
            )
            if candidate_valid:
                candidate["validation_status"] = "valid"
                candidate["validation_reason_code"] = "ok"
                candidate["validation_error"] = None
                metadata_corrected.append(candidate)
                continue
            candidate["validation_status"] = "invalid"
            candidate["validation_reason_code"] = _classify_sql_validation_error(candidate_err)
            candidate["validation_error"] = candidate_err
            still_invalid.append(candidate)

        for corrected in metadata_corrected:
            _register_valid(corrected)
        invalid_benchmarks = still_invalid
        if not invalid_benchmarks:
            break

        corrected = _attempt_benchmark_correction(
            w, config, uc_columns, uc_routines,
            invalid_benchmarks, catalog, schema, spark, allowlist,
        )
        for corrected_item in corrected:
            _register_valid(corrected_item)
        corrected_questions = {
            str(c.get("question") or "").strip().lower()
            for c in corrected
            if str(c.get("question") or "").strip()
        }
        invalid_benchmarks = [
            b for b in invalid_benchmarks
            if str(b.get("question") or "").strip().lower() not in corrected_questions
        ]

    if invalid_benchmarks:
        logger.warning(
            "Discarded %d benchmarks after %d correction rounds (unfixable): %s",
            len(invalid_benchmarks),
            MAX_CORRECTION_ROUNDS,
            [b.get("question", "")[:50] for b in invalid_benchmarks[:3]],
        )

    # ── Post-validation: check question-SQL alignment via LLM ──────────
    try:
        from genie_space_optimizer.optimization.benchmarks import (
            validate_question_sql_alignment,
        )
        alignment_targets = [b for b in valid_benchmarks if b.get("expected_sql")]
        if alignment_targets:
            alignment_results = validate_question_sql_alignment(alignment_targets)
            for b, ar in zip(alignment_targets, alignment_results):
                if not ar.get("aligned", True):
                    b["alignment_issues"] = ar.get("issues", [])
                    logger.info(
                        "Alignment issue in benchmark: %s — %s",
                        b.get("question", "")[:80],
                        "; ".join(ar.get("issues", [])),
                    )
    except Exception as _align_err:
        logger.warning("Alignment validation skipped: %s", _align_err)

    all_benchmarks: list[dict] = list(_existing)

    for idx, b in enumerate(curated):
        question_id = f"{domain}_gs_{idx + 1:03d}"
        priority = "P0"
        split = "train"
        expected_sql = str(b.get("expected_sql", "") or "")
        curated_status = "question_only" if not expected_sql else str(
            b.get("validation_status", "valid"),
        )
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": expected_sql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"), expected_sql,
                ),
                "category": b.get("category", "curated"),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": priority,
                "split": split,
                "source": b.get("source", "genie_space"),
                "provenance": "curated",
                "validation_status": curated_status,
                "validation_reason_code": "ok" if expected_sql else "missing_expected_sql",
                "validation_error": None if expected_sql else "No expected SQL in curated sample question",
                "correction_source": "",
            }
        )

    offset = len(curated)
    for idx, b in enumerate(valid_benchmarks):
        question_id = f"{domain}_{offset + idx + 1:03d}"
        priority = "P0" if idx < 3 else "P1"
        split = "held_out" if (idx + 1) % 5 == 0 else "train"
        _b_esql = b.get("expected_sql", "")
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": _b_esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"), _b_esql,
                ),
                "category": b.get("category", ""),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": priority,
                "split": split,
                "source": b.get("source", "llm_generated"),
                "provenance": b.get("provenance", "synthetic"),
                "validation_status": b.get("validation_status", "valid"),
                "validation_reason_code": b.get("validation_reason_code", "ok"),
                "validation_error": b.get("validation_error"),
                "correction_source": b.get("correction_source", ""),
            }
        )

    # ── Coverage gap-fill: ensure every asset has at least one benchmark ──
    all_accepted_questions = (
        curated_questions
        | accepted_questions
        | {str(b.get("question", "")).lower().strip() for b in _existing}
    )
    gap_fill_benchmarks = _fill_coverage_gaps(
        w=w,
        config=config,
        uc_columns=uc_columns,
        uc_routines=uc_routines,
        benchmarks=all_benchmarks,
        catalog=catalog,
        schema=schema,
        spark=spark,
        allowlist=allowlist,
        domain=domain,
        existing_questions=all_accepted_questions,
    )
    gap_fill_offset = len(curated) + len(valid_benchmarks)
    for idx, b in enumerate(gap_fill_benchmarks):
        question_id = f"{domain}_gf_{gap_fill_offset + idx + 1:03d}"
        split = "held_out" if (idx + 1) % 5 == 0 else "train"
        _gf_esql = b.get("expected_sql", "")
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": _gf_esql,
                "expected_asset": _normalize_expected_asset(
                    b.get("expected_asset", "TABLE"), _gf_esql,
                ),
                "category": b.get("category", ""),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": "P1",
                "split": split,
                "source": "llm_generated",
                "provenance": "coverage_gap_fill",
                "validation_status": b.get("validation_status", "valid"),
                "validation_reason_code": b.get("validation_reason_code", "ok"),
                "validation_error": b.get("validation_error"),
                "correction_source": "",
            }
        )

    logger.info(
        "Final benchmark set: %d total (%d curated from Genie space, "
        "%d synthetic, %d gap-fill, %d discarded out of %d raw generated)",
        len(all_benchmarks),
        len(curated),
        len(valid_benchmarks),
        len(gap_fill_benchmarks),
        len(invalid_benchmarks),
        len(raw_benchmarks),
    )
    return all_benchmarks


def load_benchmarks_from_dataset(
    spark: SparkSession,
    uc_schema: str,
    domain: str,
    _max_retries: int = 3,
) -> list[dict]:
    """Load benchmarks from an existing MLflow UC evaluation dataset table.

    Issues ``REFRESH TABLE`` before reading to avoid
    ``DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`` when the upstream preflight task
    drops and recreates the table in the same job run.
    """
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    try:
        parts = uc_schema.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid uc_schema: {uc_schema}")
        catalog, schema = parts
        table = f"genie_benchmarks_{domain}"

        def _q(identifier: str) -> str:
            return f"`{identifier.replace('`', '``')}`"

        quoted_table_name = f"{_q(catalog)}.{_q(schema)}.{_q(table)}"

        df = None
        last_err: Exception | None = None
        for attempt in range(_max_retries):
            try:
                from genie_space_optimizer.common.delta_helpers import _safe_refresh
                _safe_refresh(spark, quoted_table_name)
                df = spark.sql(f"SELECT * FROM {quoted_table_name}").toPandas()
                break
            except Exception as read_err:
                last_err = read_err
                err_msg = str(read_err)
                if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" in err_msg and attempt < _max_retries - 1:
                    import time as _time
                    wait = 5 * (attempt + 1)
                    logger.warning(
                        "Delta schema change on attempt %d/%d for %s — retrying in %ds",
                        attempt + 1, _max_retries, table_name, wait,
                    )
                    _time.sleep(wait)
                    continue
                raise

        if df is None:
            raise last_err or RuntimeError(f"Failed to read {table_name} after {_max_retries} attempts")

        benchmarks: list[dict] = []
        for _, row in df.iterrows():
            inputs = row.get("inputs", {})
            expectations = row.get("expectations", {})
            if isinstance(inputs, str):
                inputs = json.loads(inputs)
            if isinstance(expectations, str):
                expectations = json.loads(expectations)
            if not isinstance(inputs, dict):
                inputs = {}
            if not isinstance(expectations, dict):
                expectations = {}

            _cb_esql = inputs.get("expected_sql", expectations.get("expected_response", ""))
            benchmarks.append(
                {
                    "id": inputs.get("question_id", ""),
                    "question": inputs.get("question", ""),
                    "expected_sql": _cb_esql,
                    "expected_asset": _normalize_expected_asset(
                        expectations.get("expected_asset", "TABLE"), _cb_esql,
                    ),
                    "category": expectations.get("category", ""),
                    "required_tables": expectations.get("required_tables", []),
                    "required_columns": expectations.get("required_columns", []),
                    "expected_facts": expectations.get("expected_facts", []),
                    "source": expectations.get("source", ""),
                    "provenance": expectations.get("provenance", ""),
                    "validation_status": expectations.get("validation_status", ""),
                    "validation_reason_code": expectations.get("validation_reason_code", ""),
                    "validation_error": expectations.get("validation_error"),
                    "correction_source": expectations.get("correction_source", ""),
                }
            )
        logger.info("Loaded %d benchmarks from %s", len(benchmarks), table_name)
        return benchmarks
    except Exception:
        logger.exception("Failed to load benchmarks from %s", table_name)
        return []


# ── MLflow Feedback Helpers (gate outcomes & ASI on traces) ──────────


def log_gate_feedback_on_traces(
    eval_result: dict,
    gate_type: str,
    gate_result: str,
    regressions: list[dict] | None = None,
    lever: int | None = None,
    iteration: int | None = None,
) -> int:
    """Attach gate outcome as Feedback assessment on each evaluation trace.

    Returns the number of feedback entries successfully logged.
    """
    trace_map = eval_result.get("trace_map", {})
    if not trace_map:
        return 0

    logged = 0
    for qid, trace_id in trace_map.items():
        reg_summary = ""
        if regressions:
            reg_summary = "; regressions: " + ", ".join(
                f"{r.get('judge', '?')} -{r.get('drop', 0):.1f}"
                for r in regressions[:3]
            )
        try:
            mlflow.log_feedback(
                trace_id=trace_id,
                name=f"gate_{gate_type}",
                value=gate_result == "pass",
                rationale=f"Lever {lever} gate {gate_type}: {gate_result}{reg_summary}",
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/gate",
                ),
                metadata={
                    "gate_type": gate_type,
                    "gate_result": gate_result,
                    "lever": lever,
                    "iteration": iteration,
                    "question_id": qid,
                    "regressions": (regressions or [])[:3],
                },
            )
            logged += 1
        except Exception:
            logger.debug("Failed to log gate feedback for trace %s", trace_id, exc_info=True)
    if logged:
        logger.info("Logged gate_%s feedback on %d/%d traces", gate_type, logged, len(trace_map))
    return logged


def log_asi_feedback_on_traces(
    eval_result: dict,
    asi_rows: list[dict],
) -> int:
    """Attach ASI root-cause analysis as Feedback on evaluation traces.

    Returns the number of feedback entries successfully logged.
    """
    trace_map = eval_result.get("trace_map", {})
    if not trace_map or not asi_rows:
        return 0

    logged = 0
    for asi in asi_rows:
        qid = asi.get("question_id", "")
        tid = trace_map.get(qid)
        if not tid:
            continue
        judge = asi.get("judge", "unknown")
        try:
            mlflow.log_feedback(
                trace_id=tid,
                name=f"asi_{judge}",
                value=asi.get("value", "no") == "yes",
                rationale=asi.get("counterfactual_fix") or asi.get("rationale_snippet") or "",
                source=AssessmentSource(
                    source_type="CODE",
                    source_id="genie_space_optimizer/asi",
                ),
                metadata={
                    "failure_type": asi.get("failure_type"),
                    "severity": asi.get("severity"),
                    "blame_set": asi.get("blame_set"),
                    "wrong_clause": asi.get("wrong_clause"),
                    "expected_value": asi.get("expected_value"),
                    "actual_value": asi.get("actual_value"),
                    "question_id": qid,
                    "judge": judge,
                },
            )
            logged += 1
        except Exception:
            logger.debug("Failed to log ASI feedback for trace %s judge %s", tid, judge, exc_info=True)
    if logged:
        logger.info("Logged ASI feedback on %d traces", logged)
    return logged
