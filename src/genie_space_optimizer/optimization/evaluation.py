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
from datetime import datetime, timezone
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
    BENCHMARK_GENERATION_PROMPT,
    CODE_SOURCE_ID,
    DEFAULT_THRESHOLDS,
    FAILURE_TAXONOMY,
    INSTRUCTION_PROMPT_ALIAS,
    INSTRUCTION_PROMPT_NAME_TEMPLATE,
    JUDGE_PROMPTS,
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
)
from genie_space_optimizer.common.genie_client import (
    detect_asset_type,
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
    source_id=LLM_SOURCE_ID_TEMPLATE.format(endpoint=LLM_ENDPOINT),
)

EVAL_SCOPES = {"full", "slice", "p0", "held_out"}
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


def _call_llm_for_scoring(
    w: WorkspaceClient,
    prompt: str,
    max_retries: int = LLM_MAX_RETRIES,
) -> dict:
    """Call LLM using Databricks SDK with retry + exponential backoff."""
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
            content = content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1] if "\n" in content else content[3:]
                content = content.rsplit("```", 1)[0]
            return json.loads(content)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
    raise last_err  # type: ignore[misc]


def normalize_result_df(df: pd.DataFrame | None) -> pd.DataFrame:
    """Deterministic normalization of a result DataFrame.

    Sort columns alphabetically, sort rows, round floats to 6 decimals,
    normalize timestamps to UTC, strip whitespace.
    """
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df = df[sorted(df.columns)]
    for col in df.select_dtypes(include=["float64", "float32"]).columns:
        df[col] = df[col].round(6)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
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
        numeric_sums[col] = round(float(df[col].sum()), 6)
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
) -> str:
    """Render scorer feedback in a structured markdown + JSON ASI format."""
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

    return (
        f"### {judge_name}\n"
        f"**Verdict:** {verdict}\n\n"
        f"{rationale_text}\n\n"
        "```json\n"
        f"{json.dumps(payload, indent=2, sort_keys=True)}\n"
        "```"
    )


def _parse_asi_from_rationale(rationale: str) -> dict:
    """Extract the ASI JSON payload embedded in a ``format_asi_markdown`` rationale."""
    if not rationale:
        return {}
    try:
        start = rationale.index("```json\n") + len("```json\n")
        end = rationale.index("\n```", start)
        return json.loads(rationale[start:end])
    except (ValueError, json.JSONDecodeError):
        return {}


def _extract_assessments_from_traces(results_df) -> dict[int, dict[str, dict]]:
    """Pull scorer rationale + metadata from the trace column before it is stripped.

    Returns ``{row_index: {judge_name: {"rationale": str, "metadata": dict}}}``.
    Falls back gracefully if traces/assessments are unavailable.
    """
    out: dict[int, dict[str, dict]] = {}
    if "trace" not in results_df.columns:
        return out
    for row_idx, (_, row) in enumerate(results_df.iterrows()):
        trace = row.get("trace")
        if trace is None:
            continue
        assessments = None
        for attr_chain in [("data", "assessments"), ("info", "assessments")]:
            obj = trace
            for attr in attr_chain:
                obj = getattr(obj, attr, None)
                if obj is None:
                    break
            if obj is not None:
                assessments = obj
                break
        if not assessments:
            continue
        row_data: dict[str, dict] = {}
        for a in assessments:
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


# ── Benchmark Filtering ─────────────────────────────────────────────────


def filter_benchmarks_by_scope(
    benchmarks: list[dict],
    scope: str = "full",
    patched_objects: list[str] | None = None,
) -> list[dict]:
    """Filter benchmarks based on evaluation scope.

    Scopes: "full" (all), "slice" (affected by patches),
    "p0" (priority P0 only), "held_out" (held-out split).
    """
    if scope == "full":
        return benchmarks
    if scope == "slice" and patched_objects:
        patched = {o.lower() for o in patched_objects}
        return [
            b
            for b in benchmarks
            if any(
                t.lower() in patched
                for t in b.get("required_tables", []) + b.get("required_columns", [])
            )
        ]
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


def _precheck_benchmarks_for_eval(
    *,
    benchmarks: list[dict],
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    known_functions: set[str],
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

    for idx, benchmark in enumerate(benchmarks):
        question = str(benchmark.get("question") or "").strip()
        qid = str(benchmark.get("id") or benchmark.get("question_id") or f"q-{idx}")
        sql = str(benchmark.get("expected_sql") or "").strip()
        if not sql:
            valid.append(benchmark)
            continue

        resolved_sql = resolve_sql(sql, catalog=catalog, gold_schema=gold_schema)
        try:
            _set_sql_context(spark, catalog, gold_schema)
            spark.sql(f"EXPLAIN {resolved_sql}")
        except Exception as exc:
            msg = str(exc)
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
):
    """Return a predict function with bound workspace/spark context.

    The returned closure is suitable for ``mlflow.genai.evaluate(predict_fn=...)``.
    """

    known_functions = _load_known_functions(spark, catalog, schema)

    @mlflow.trace
    def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
        """Query Genie, execute both SQLs, return response + comparison.

        Steps: rate-limit → Genie call → sanitize → resolve GT SQL →
               dual-execute → normalize → compare hashes.
        """
        try:
            mlflow.update_current_trace(
                tags={
                    "question_id": kwargs.get("question_id", ""),
                    "space_id": space_id,
                },
            )
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
        try:
            time.sleep(RATE_LIMIT_SECONDS)
            result = run_genie_query(w, space_id, question)
            genie_sql = sanitize_sql(result.get("sql") or "")
            gt_sql = resolve_sql(expected_sql, catalog, schema)

            if genie_sql and gt_sql:
                try:
                    _set_sql_context(spark, catalog, schema)
                    called_functions = _extract_sql_function_calls(gt_sql, catalog, schema)
                    called_functions.update(_extract_sql_function_calls(genie_sql, catalog, schema))
                    missing_functions = sorted(f for f in called_functions if f not in known_functions)
                    if missing_functions:
                        comparison["error"] = (
                            "Missing function(s) in schema "
                            f"{catalog}.{schema}: {', '.join(missing_functions)}"
                        )
                        comparison["error_type"] = "permission_blocked"
                    else:
                        compile_failed = False
                        for _label, _sql in [("ground_truth", gt_sql), ("genie", genie_sql)]:
                            try:
                                spark.sql(f"EXPLAIN {_sql}")
                            except Exception as explain_exc:
                                explain_msg = str(explain_exc)
                                comparison["error"] = (
                                    f"{_label} SQL compilation failed: {explain_msg[:400]}"
                                )
                                comparison["error_type"] = (
                                    "infrastructure"
                                    if _label == "ground_truth"
                                    else "query_execution"
                                )
                                comparison["sqlstate"] = _extract_sqlstate(explain_msg)
                                compile_failed = True
                                break

                        if not compile_failed:
                            gt_df = normalize_result_df(spark.sql(gt_sql).toPandas())
                            genie_df = normalize_result_df(spark.sql(genie_sql).toPandas())
                            gt_hash = hashlib.md5(
                                gt_df.to_csv(index=False).encode()
                            ).hexdigest()[:8]
                            genie_hash = hashlib.md5(
                                genie_df.to_csv(index=False).encode()
                            ).hexdigest()[:8]
                            exact_match = gt_df.shape == genie_df.shape and gt_df.equals(genie_df)
                            hash_match = gt_hash == genie_hash
                            gt_sig = result_signature(gt_df)
                            genie_sig = result_signature(genie_df)
                            sig_match = (
                                gt_sig["schema_hash"] == genie_sig["schema_hash"]
                                and gt_sig["row_count"] == genie_sig["row_count"]
                            )

                            if exact_match:
                                match_type = "exact"
                            elif hash_match:
                                match_type = "hash"
                            elif sig_match:
                                match_type = "signature"
                            else:
                                match_type = "mismatch"

                            comparison = {
                                "match": exact_match or hash_match,
                                "match_type": match_type,
                                "gt_rows": len(gt_df),
                                "genie_rows": len(genie_df),
                                "gt_hash": gt_hash,
                                "genie_hash": genie_hash,
                                "gt_signature": gt_sig,
                                "genie_signature": genie_sig,
                                "error": None,
                            }
                except Exception as exc:
                    err_msg = str(exc)
                    comparison["error"] = err_msg[:500]
                    comparison["error_type"] = (
                        "infrastructure"
                        if _is_infrastructure_sql_error(err_msg)
                        else "query_execution"
                    )
                    comparison["sqlstate"] = _extract_sqlstate(err_msg)
            else:
                comparison["error"] = "Missing SQL for comparison"
                comparison["error_type"] = "missing_expected_sql"
        except Exception as exc:
            # Never raise from predict_fn: MLflow harness should always receive a row.
            err_msg = str(exc)
            comparison["error"] = err_msg[:500]
            comparison["error_type"] = (
                "infrastructure" if _is_infrastructure_sql_error(err_msg) else "predict_fn_error"
            )
            comparison["sqlstate"] = _extract_sqlstate(err_msg)

        return {
            "response": genie_sql,
            "status": result.get("status", "ERROR"),
            "conversation_id": result.get("conversation_id", ""),
            "comparison": comparison,
        }

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
    prompt_name = INSTRUCTION_PROMPT_NAME_TEMPLATE.format(
        uc_schema=uc_schema, space_id=safe_space_id,
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

    logger.info(
        "Registered %d prompts (registry=%s, artifacts=True)",
        len(JUDGE_PROMPTS),
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
        prompt_name = PROMPT_NAME_TEMPLATE.format(uc_schema=uc_schema, judge_name=name) if uc_schema else name
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
        candidates.append(PROMPT_NAME_TEMPLATE.format(uc_schema=uc_schema, judge_name=judge_name))
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
            return _orig_get_new_expectations(eval_item)

        _harness_mod._get_new_expectations = _safe_get_new_expectations
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

        _trace_utils_mod.batch_link_traces_to_run = _safe_batch_link_traces_to_run

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
    if not isinstance(data, pd.DataFrame) or data.empty:
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
) -> Any | None:
    """Create or replace the MLflow UC evaluation dataset from benchmarks.

    Drops the existing table first to prevent stale/invalid benchmarks from
    persisting across runs (``merge_records`` appends and never removes old rows).
    """
    uc_table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    try:
        _drop_benchmark_table(spark, uc_table_name)

        eval_dataset = mlflow.genai.datasets.create_dataset(
            uc_table_name=uc_table_name,
        )
        records = []
        for b in benchmarks:
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
                    "expectations": {
                        "expected_response": b.get("expected_sql", ""),
                        "expected_asset": b.get("expected_asset", "TABLE"),
                        "category": b.get("category", ""),
                        "required_tables": b.get("required_tables", []),
                        "required_columns": b.get("required_columns", []),
                        "expected_facts": b.get("expected_facts", []),
                        "source": b.get("source", ""),
                        "provenance": b.get("provenance", ""),
                        "validation_status": b.get("validation_status", ""),
                        "validation_reason_code": b.get("validation_reason_code", ""),
                        "validation_error": b.get("validation_error"),
                        "correction_source": b.get("correction_source", ""),
                    },
                }
            )
        eval_dataset.merge_records(records)
        logger.info("UC Evaluation Dataset: %s (%d records)", uc_table_name, len(records))
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

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = RUN_NAME_TEMPLATE.format(iteration=iteration, timestamp=ts)

    with mlflow.start_run(run_name=run_name) as run:
        if spark is not None:
            known_functions = _load_known_functions(spark, catalog, gold_schema)
            filtered, quarantined_benchmarks, precheck_counts = _precheck_benchmarks_for_eval(
                benchmarks=scope_filtered,
                spark=spark,
                catalog=catalog,
                gold_schema=gold_schema,
                known_functions=known_functions,
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
            expectations = {
                "expected_response": b.get("expected_sql", ""),
                "expected_asset": b.get("expected_asset", "TABLE"),
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
            "data": eval_data,
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

        failure_ids: list[str] = []
        arbiter_verdicts: dict[str, int] = {
            "genie_correct": 0,
            "ground_truth_correct": 0,
            "both_correct": 0,
            "neither_correct": 0,
            "skipped": 0,
        }
        rows_for_output: list[dict] = []

        _STRIP_COLS = {"trace", "trace_id"}
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

                rows_for_output.append(row_dict)

                rc = row.get(
                    "result_correctness/value", row.get("result_correctness", "")
                )
                if str(rc).lower() in ("no", "false", "0", "0.0"):
                    qid = row.get(
                        "inputs/question_id",
                        row.get("inputs", {}).get("question_id", ""),
                    )
                    if qid:
                        failure_ids.append(str(qid))

                av = str(row.get("arbiter/value", row.get("arbiter", "skipped")))
                if av in arbiter_verdicts:
                    arbiter_verdicts[av] += 1
                else:
                    arbiter_verdicts["skipped"] += 1

        question_failure_artifacts: list[dict[str, Any]] = []
        for row in rows_for_output:
            error_val = (
                row.get("outputs/comparison/error")
                or row.get("comparison/error")
                or row.get("comparison.error")
            )
            if not error_val:
                continue
            question_failure_artifacts.append(
                {
                    "question_id": str(
                        row.get("inputs/question_id")
                        or row.get("question_id")
                        or ""
                    ),
                    "expected_sql": str(row.get("inputs/expected_sql") or ""),
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

        overall_accuracy = per_judge.get("result_correctness", 0.0)
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
        mlflow.set_tags(
            {
                "evaluation_status": "success",
                "invalid_benchmark_count": str(precheck_counts["invalid_benchmark_count"]),
                "permission_blocked_count": str(permission_blocked_count),
                "unresolved_column_count": str(unresolved_column_count),
                "harness_retry_count": str(harness_retry_count),
            }
        )

        output: dict[str, Any] = {
            "run_id": run.info.run_id,
            "mlflow_run_id": run.info.run_id,
            "run_name": run_name,
            "experiment_id": exp.experiment_id if exp else "",
            "iteration": iteration,
            "overall_accuracy": scores_100.get("result_correctness", overall_accuracy * 100),
            "total_questions": len(filtered),
            "correct_count": len(filtered) - len(failure_ids),
            "scores": scores_100,
            "thresholds_met": thresholds_passed,
            "thresholds_passed": thresholds_passed,
            "per_judge": per_judge,
            "failures": failure_ids,
            "failure_question_ids": failure_ids,
            "remaining_failures": failure_ids,
            "arbiter_verdicts": arbiter_verdicts,
            "arbiter_actions": [],
            "model_id": model_id,
            "rows": rows_for_output,
            "invalid_benchmark_count": precheck_counts["invalid_benchmark_count"],
            "permission_blocked_count": permission_blocked_count,
            "unresolved_column_count": unresolved_column_count,
            "harness_retry_count": harness_retry_count,
            "quarantined_benchmarks": quarantined_benchmarks,
        }

    logger.info(
        "Evaluation complete: %s — accuracy=%.1f%%, thresholds=%s",
        run_name,
        output["overall_accuracy"],
        "PASS" if thresholds_passed else "FAIL",
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
                    "expected_asset": b.get("expected_asset", "TABLE"),
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
        _STRIP_COLS_REP = {"trace", "trace_id"}
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

    return {
        "run_id": run.info.run_id,
        "mlflow_run_id": run.info.run_id,
        "run_name": run_name,
        "repeatability_pct": repeatability_pct,
        "per_judge": per_judge,
        "rows": rows_for_output,
        "scores": normalize_scores(per_judge),
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

    return {
        "tables_context": tables_context,
        "metric_views_context": metric_views_context,
        "tvfs_context": tvfs_context,
        "instructions_context": instructions_context,
        "sample_questions_context": sample_questions_context,
        "valid_assets_context": _build_valid_assets_context(config),
    }


def _validate_benchmark_sql(
    sql: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> tuple[bool, str]:
    """Validate a benchmark's expected_sql. Returns (is_valid, error)."""
    from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

    resolved = resolve_sql(sql, catalog, schema)
    sanitized = sanitize_sql(resolved)
    if not sanitized.strip():
        return False, "Empty SQL"
    return validate_ground_truth_sql(sanitized, spark, catalog=catalog, gold_schema=schema)


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

    prompt = BENCHMARK_CORRECTION_PROMPT.format(
        valid_assets_context=ctx["valid_assets_context"],
        tables_context=ctx["tables_context"],
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
    """
    curated = genie_space_benchmarks or []
    curated_questions = {b.get("question", "").lower().strip() for b in curated}
    synthetic_target = max(target_count - len(curated), 5)
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

    existing_questions_context = ""
    if curated:
        existing_questions_context = (
            "\n\n## Already Covered Questions (do NOT duplicate these)\n"
            + "\n".join(f"- {b.get('question', '')}" for b in curated)
        )

    prompt = BENCHMARK_GENERATION_PROMPT.format(
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
            "expected_asset": b.get("expected_asset", "TABLE"),
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

        is_valid, err = _validate_benchmark_sql(expected_sql, spark, catalog, schema)
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

    all_benchmarks: list[dict] = []

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
                "expected_asset": b.get("expected_asset", "TABLE"),
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
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": b.get("expected_sql", ""),
                "expected_asset": b.get("expected_asset", "TABLE"),
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

    logger.info(
        "Final benchmark set: %d total (%d curated from Genie space, "
        "%d synthetic, %d discarded out of %d raw generated)",
        len(all_benchmarks),
        len(curated),
        len(valid_benchmarks),
        len(invalid_benchmarks),
        len(raw_benchmarks),
    )
    return all_benchmarks


def load_benchmarks_from_dataset(
    spark: SparkSession,
    uc_schema: str,
    domain: str,
) -> list[dict]:
    """Load benchmarks from an existing MLflow UC evaluation dataset table."""
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
        df = spark.sql(f"SELECT * FROM {quoted_table_name}").toPandas()
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

            benchmarks.append(
                {
                    "id": inputs.get("question_id", ""),
                    "question": inputs.get("question", ""),
                    "expected_sql": inputs.get("expected_sql", expectations.get("expected_response", "")),
                    "expected_asset": expectations.get("expected_asset", "TABLE"),
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
