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
import tempfile
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union

import mlflow
import pandas as pd
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from mlflow.entities import AssessmentSource, Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.config import (
    ASI_SCHEMA,
    BENCHMARK_CATEGORIES,
    BENCHMARK_GENERATION_PROMPT,
    CODE_SOURCE_ID,
    DEFAULT_THRESHOLDS,
    FAILURE_TAXONOMY,
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
                comparison["error"] = str(e)[:200]
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


def register_judge_prompts(
    uc_schema: str,
    domain: str,
    experiment_name: str,
) -> dict[str, dict]:
    """Register judge prompts to MLflow Prompt Registry + experiment artifacts.

    Dual storage: Prompt Registry (versioned, aliased) and experiment
    artifacts (UI visibility). Idempotent.
    """
    registered: dict[str, dict] = {}

    if uc_schema:
        for name, template in JUDGE_PROMPTS.items():
            prompt_name = PROMPT_NAME_TEMPLATE.format(
                uc_schema=uc_schema, judge_name=name
            )
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
            except Exception:
                logger.exception("Prompt registration failed for %s", prompt_name)

    mlflow.set_experiment(experiment_name)
    run_name = f"register_prompts_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    with mlflow.start_run(run_name=run_name):
        for name, template in JUDGE_PROMPTS.items():
            tmp = os.path.join(tempfile.gettempdir(), f"genie_opt_{name}.txt")
            with open(tmp, "w") as f:
                f.write(template)
            mlflow.log_artifact(tmp, artifact_path=f"judge_prompts/{name}")
            os.unlink(tmp)
        mlflow.log_params(
            {
                "num_prompts": len(JUDGE_PROMPTS),
                "prompt_keys": ",".join(JUDGE_PROMPTS.keys()),
                "domain": domain,
            }
        )

    logger.info(
        "Registered %d prompts (registry=%s, artifacts=True)",
        len(JUDGE_PROMPTS),
        bool(uc_schema),
    )
    return registered


def create_evaluation_dataset(
    spark: SparkSession,
    benchmarks: list[dict],
    uc_schema: str,
    domain: str,
    space_id: str = "",
    catalog: str = "",
    gold_schema: str = "",
) -> Any | None:
    """Create or update an MLflow UC evaluation dataset from benchmarks."""
    uc_table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    try:
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
        mlflow.log_params(run_params)

        if model_id:
            mlflow.set_active_model(model_id=model_id)

        evaluate_kwargs: dict[str, Any] = {
            "predict_fn": predict_fn,
            "data": eval_data,
            "scorers": scorers,
        }
        if model_id:
            evaluate_kwargs["model_id"] = model_id

        eval_result = mlflow.genai.evaluate(**evaluate_kwargs)

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


# ── Benchmark Generation ────────────────────────────────────────────────


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
) -> list[dict]:
    """Generate benchmark questions via LLM from schema context.

    1. Build schema context string from Genie Space config + UC metadata
    2. Call LLM with BENCHMARK_GENERATION_PROMPT
    3. Parse JSON response into benchmark dicts
    4. Validate each expected_sql via EXPLAIN — discard failures
    5. Assign question_id, priority, split
    """
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

    prompt = BENCHMARK_GENERATION_PROMPT.format(
        domain=domain,
        tables_context=tables_context,
        metric_views_context=metric_views_context,
        tvfs_context=tvfs_context,
        instructions_context=instructions_context,
        sample_questions_context=sample_questions_context,
        target_count=target_count,
        categories=json.dumps(BENCHMARK_CATEGORIES),
    )

    response = _call_llm_for_scoring(w, prompt)
    raw_benchmarks: list[dict] = response if isinstance(response, list) else response.get("benchmarks", [])

    validated: list[dict] = []
    for idx, b in enumerate(raw_benchmarks):
        expected_sql = b.get("expected_sql", "")
        if not expected_sql:
            continue

        resolved = resolve_sql(expected_sql, catalog, schema)
        sanitized = sanitize_sql(resolved)
        try:
            spark.sql(f"EXPLAIN {sanitized}")
        except Exception:
            logger.warning("Benchmark %d failed EXPLAIN validation, discarding", idx)
            continue

        question_id = f"{domain}_{idx + 1:03d}"
        priority = "P0" if idx < 3 else "P1"
        split = "held_out" if (idx + 1) % 5 == 0 else "train"

        validated.append(
            {
                "id": question_id,
                "question": b.get("question", ""),
                "expected_sql": expected_sql,
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
        "Generated %d benchmarks (%d validated out of %d raw)",
        len(validated),
        len(validated),
        len(raw_benchmarks),
    )
    return validated


def load_benchmarks_from_dataset(
    spark: SparkSession,
    uc_schema: str,
    domain: str,
) -> list[dict]:
    """Load benchmarks from an existing MLflow UC evaluation dataset table."""
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    try:
        df = spark.sql(f"SELECT * FROM {table_name}").toPandas()
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
