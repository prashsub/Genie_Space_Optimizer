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
from datetime import datetime, timezone
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
            content = response.choices[0].message.content
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


def normalize_result_df(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Deterministic normalization of a result DataFrame.

    Sort columns alphabetically, sort rows, round floats to 6 decimals,
    normalize timestamps to UTC, strip whitespace.
    """
    if df is None or df.empty:
        return df
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
        time.sleep(RATE_LIMIT_SECONDS)
        result = run_genie_query(w, space_id, question)
        genie_sql = sanitize_sql(result.get("sql") or "")
        gt_sql = resolve_sql(expected_sql, catalog, schema)

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
                    return {
                        "response": genie_sql,
                        "status": result.get("status", "UNKNOWN"),
                        "conversation_id": result.get("conversation_id", ""),
                        "comparison": comparison,
                    }

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
            except Exception as e:
                err_msg = str(e)
                comparison["error"] = err_msg[:500]
                comparison["error_type"] = (
                    "infrastructure"
                    if _is_infrastructure_sql_error(err_msg)
                    else "query_execution"
                )
        else:
            comparison["error"] = "Missing SQL for comparison"

        return {
            "response": genie_sql,
            "status": result.get("status", "UNKNOWN"),
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
    """Best-effort sync of MLflow traces into UC Delta (OTEL tables)."""
    destination = (
        os.getenv("MLFLOW_TRACING_DESTINATION")
        or os.getenv("GENIE_SPACE_OPTIMIZER_TRACE_UC_SCHEMA")
        or uc_schema
    )
    destination = destination.strip("` ").strip()
    if destination.count(".") != 1:
        return ""

    catalog_name, schema_name = destination.split(".", 1)
    try:
        from mlflow.entities import UCSchemaLocation

        location = UCSchemaLocation(catalog_name=catalog_name, schema_name=schema_name)
        mlflow.tracing.set_destination(destination=location)
        mlflow.tracing.set_experiment_trace_location(
            location=location,
            experiment_id=experiment_id or None,
            sql_warehouse_id=warehouse_id or None,
        )
        if warehouse_id:
            mlflow.tracing.set_databricks_monitoring_sql_warehouse_id(
                sql_warehouse_id=warehouse_id,
                experiment_id=experiment_id or None,
            )
        return destination
    except Exception:
        logger.warning(
            "UC trace sync not configured for destination=%s; continuing without blocking eval",
            destination,
        )
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


def _run_evaluate_with_retries(
    *,
    evaluate_kwargs: dict[str, Any],
) -> tuple[Any, list[dict[str, Any]]]:
    """Run mlflow.genai.evaluate() with targeted retry for transient harness errors."""
    attempts: list[dict[str, Any]] = []
    initial_workers = os.getenv("MLFLOW_GENAI_EVAL_MAX_WORKERS")

    for attempt in range(1, max(1, EVAL_MAX_ATTEMPTS) + 1):
        # First attempt keeps current worker setting; fallback retries force single worker.
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

    raise RuntimeError("Evaluation retry loop exhausted unexpectedly")


def _collect_infra_eval_errors(rows: list[dict[str, Any]]) -> list[str]:
    """Extract infrastructure-like SQL errors from eval result rows."""
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
                if err:
                    candidates.append(str(err))
        for key in (
            "outputs/comparison/error",
            "comparison/error",
            "error",
        ):
            err = row.get(key)
            if err:
                candidates.append(str(err))

        # Some scorer failures are serialized into rationale/metadata columns.
        # Scan all string-like values to catch infra errors there too.
        for value in row.values():
            if isinstance(value, str) and _is_infrastructure_sql_error(value):
                candidates.append(value)

        for msg in candidates:
            if _is_infrastructure_sql_error(msg):
                infra_errors.append(msg[:500])

    # de-duplicate while preserving order
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
    catalog: str = "",
    gold_schema: str = "",
    uc_schema: str = "",
    warehouse_id: str = "",
    patched_objects: list[str] | None = None,
) -> dict:
    """Run ``mlflow.genai.evaluate()`` and return structured results.

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

    filtered = filter_benchmarks_by_scope(benchmarks, eval_scope, patched_objects)

    eval_records = []
    for b in filtered:
        eval_records.append(
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
                },
            }
        )
    eval_data = pd.DataFrame(eval_records)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = RUN_NAME_TEMPLATE.format(iteration=iteration, timestamp=ts)

    with mlflow.start_run(run_name=run_name) as run:
        run_params = {
            "space_id": space_id,
            "iteration": iteration,
            "dataset": f"{domain}_benchmarks",
            "eval_scope": eval_scope,
            "num_scorers": len(scorers),
            "domain": domain,
            "benchmark_count": len(filtered),
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
        # Ensure every evaluation run carries a full, queryable judge manifest.
        register_judge_prompts(
            uc_schema=uc_schema,
            domain=domain,
            experiment_name=experiment_name,
            register_registry=(iteration == 0 and eval_scope == "full"),
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

        eval_attempts: list[dict[str, Any]] = []
        try:
            eval_result, eval_attempts = _run_evaluate_with_retries(
                evaluate_kwargs=evaluate_kwargs,
            )
        except Exception as exc:
            attempts_from_exc = getattr(exc, "_eval_attempts", None)
            if isinstance(attempts_from_exc, list):
                eval_attempts = attempts_from_exc
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

        if hasattr(eval_result, "tables") and "eval_results" in eval_result.tables:
            results_df = eval_result.tables["eval_results"]
            for _, row in results_df.iterrows():
                row_dict = {}
                for col in results_df.columns:
                    val = row[col]
                    if hasattr(val, "item"):
                        val = val.item()
                    row_dict[col] = val
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
        }

    logger.info(
        "Evaluation complete: %s — accuracy=%.1f%%, thresholds=%s",
        run_name,
        output["overall_accuracy"],
        "PASS" if thresholds_passed else "FAIL",
    )
    return output


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

    sq = parsed_space.get("sample_questions", [])
    for q_item in (sq if isinstance(sq, list) else []):
        if isinstance(q_item, dict):
            question = str(q_item.get("question", q_item.get("text", ""))).strip()
        else:
            question = str(q_item).strip()
        if not question:
            continue
        q_lower = question.lower()
        if q_lower in seen_questions:
            continue
        seen_questions.add(q_lower)

        benchmarks.append({
            "question": question,
            "expected_sql": "",
            "expected_asset": "TABLE",
            "category": "sample_question",
            "required_tables": [],
            "required_columns": [],
            "expected_facts": [],
            "source": "genie_space",
        })

    logger.info(
        "Extracted %d benchmarks from Genie space config "
        "(%d with SQL, %d question-only)",
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
        is_valid, err = _validate_benchmark_sql(sql, spark, catalog, schema)
        if is_valid:
            corrected.append(c)
        else:
            logger.warning(
                "Corrected benchmark still invalid: %s — %s", c.get("question", "")[:60], err,
            )
    return corrected


MAX_CORRECTION_ROUNDS = 2


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
      5. Validate each expected_sql via EXPLAIN + table existence check
      6. Send invalid benchmarks to correction LLM (up to MAX_CORRECTION_ROUNDS)
      7. Merge curated + synthetic, assign IDs
    """
    curated = genie_space_benchmarks or []
    curated_questions = {b.get("question", "").lower().strip() for b in curated}
    synthetic_target = max(target_count - len(curated), 5)

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

    for b in raw_benchmarks:
        expected_sql = b.get("expected_sql", "")
        if not expected_sql:
            continue
        q_lower = b.get("question", "").lower().strip()
        if q_lower in curated_questions:
            logger.debug("Skipping synthetic duplicate of curated question: %s", q_lower[:50])
            continue
        is_valid, err = _validate_benchmark_sql(expected_sql, spark, catalog, schema)
        if is_valid:
            valid_benchmarks.append(b)
        else:
            b["validation_error"] = err
            invalid_benchmarks.append(b)
            logger.warning(
                "Benchmark failed validation: %s — %s",
                b.get("question", "")[:60], err,
            )

    for correction_round in range(MAX_CORRECTION_ROUNDS):
        if not invalid_benchmarks:
            break
        logger.info(
            "Correction round %d: attempting to fix %d invalid benchmarks",
            correction_round + 1, len(invalid_benchmarks),
        )
        corrected = _attempt_benchmark_correction(
            w, config, uc_columns, uc_routines,
            invalid_benchmarks, catalog, schema, spark,
        )
        valid_benchmarks.extend(corrected)
        corrected_questions = {c.get("question") for c in corrected}
        invalid_benchmarks = [
            b for b in invalid_benchmarks
            if b.get("question") not in corrected_questions
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
        all_benchmarks.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": b.get("expected_sql", ""),
                "expected_asset": b.get("expected_asset", "TABLE"),
                "category": b.get("category", "curated"),
                "required_tables": b.get("required_tables", []),
                "required_columns": b.get("required_columns", []),
                "expected_facts": b.get("expected_facts", []),
                "priority": priority,
                "split": split,
                "source": b.get("source", "genie_space"),
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
                "source": "llm_generated",
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

            benchmarks.append(
                {
                    "id": inputs.get("question_id", ""),
                    "question": inputs.get("question", ""),
                    "expected_sql": inputs.get("expected_sql", expectations.get("expected_response", "")),
                    "expected_asset": expectations.get("expected_asset", "TABLE"),
                }
            )
        logger.info("Loaded %d benchmarks from %s", len(benchmarks), table_name)
        return benchmarks
    except Exception:
        logger.exception("Failed to load benchmarks from %s", table_name)
        return []
