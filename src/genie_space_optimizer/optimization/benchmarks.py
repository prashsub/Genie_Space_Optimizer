"""
Benchmark management — loading, validation, splitting, and corrections.

Benchmarks are stored as MLflow evaluation datasets in UC (no YAML files).
"""

from __future__ import annotations

import hashlib
import logging
import random
from typing import Any

from genie_space_optimizer.common.config import TEMPLATE_VARIABLES

logger = logging.getLogger(__name__)


def resolve_sql(sql: str, **kwargs: str) -> str:
    """Substitute ``${catalog}`` / ``${gold_schema}`` template variables."""
    if not sql:
        return sql
    for tmpl_var, param_name in TEMPLATE_VARIABLES.items():
        if param_name in kwargs:
            sql = sql.replace(tmpl_var, kwargs[param_name])
    return sql


# ═══════════════════════════════════════════════════════════════════════
# 1. Loading
# ═══════════════════════════════════════════════════════════════════════


def load_benchmarks_from_dataset(
    spark_or_dataset: Any,
    uc_schema: str,
    domain: str,
) -> list[dict]:
    """Load benchmarks from an MLflow evaluation dataset in UC.

    Table name convention: ``{uc_schema}.genie_benchmarks_{domain}``.

    Args:
        spark_or_dataset: A Spark session or a pre-loaded DataFrame/list.
        uc_schema: Fully-qualified UC schema (``catalog.schema``).
        domain: Domain identifier (e.g. ``cost``, ``booking``).

    Returns:
        List of benchmark question dicts with ``question``, ``expected_sql``,
        ``expected_asset``, ``category``, etc.
    """
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"

    if isinstance(spark_or_dataset, list):
        return spark_or_dataset

    try:
        if hasattr(spark_or_dataset, "read"):
            df = spark_or_dataset.table(table_name)
        else:
            df = spark_or_dataset
        rows = df.collect()
        return [r.asDict() for r in rows]
    except Exception:
        logger.exception("Failed to load benchmarks from %s", table_name)
        return []


# ═══════════════════════════════════════════════════════════════════════
# 2. Validation
# ═══════════════════════════════════════════════════════════════════════


def validate_ground_truth_sql(
    sql: str,
    spark: Any,
    catalog: str = "",
    gold_schema: str = "",
) -> tuple[bool, str]:
    """Validate a single expected SQL via ``spark.sql(f"EXPLAIN ...")``.

    Returns ``(is_valid, error_message)``.
    """
    resolved = resolve_sql(sql, catalog=catalog, gold_schema=gold_schema)
    if not resolved or not resolved.strip():
        return False, "Empty SQL"

    try:
        spark.sql(f"EXPLAIN {resolved}")
        return True, ""
    except Exception as e:
        return False, str(e)


def validate_benchmarks(
    benchmarks: list[dict],
    spark: Any,
    catalog: str = "",
    gold_schema: str = "",
) -> list[dict]:
    """Validate each benchmark's ``expected_sql`` via EXPLAIN.

    Returns a list of validation result dicts:
    ``{question, expected_sql, valid, error}``.
    """
    results: list[dict] = []
    for b in benchmarks:
        sql = b.get("expected_sql", "")
        question = b.get("question", "")
        is_valid, error = validate_ground_truth_sql(
            sql, spark, catalog=catalog, gold_schema=gold_schema
        )
        results.append(
            {
                "question": question,
                "expected_sql": sql,
                "valid": is_valid,
                "error": error,
            }
        )
    return results


# ═══════════════════════════════════════════════════════════════════════
# 3. Train/Held-Out Split
# ═══════════════════════════════════════════════════════════════════════


def assign_splits(
    benchmarks: list[dict],
    train_ratio: float = 0.8,
    seed: int = 42,
) -> list[dict]:
    """Assign ``split`` field (``train`` or ``held_out``) to each benchmark.

    Uses deterministic shuffle based on seed for reproducibility.
    """
    rng = random.Random(seed)
    indices = list(range(len(benchmarks)))
    rng.shuffle(indices)
    cutoff = int(len(benchmarks) * train_ratio)

    for rank, idx in enumerate(indices):
        benchmarks[idx]["split"] = "train" if rank < cutoff else "held_out"

    return benchmarks


# ═══════════════════════════════════════════════════════════════════════
# 4. MLflow Record Building
# ═══════════════════════════════════════════════════════════════════════


def build_eval_records(benchmarks: list[dict]) -> list[dict]:
    """Convert benchmarks to MLflow evaluation record format.

    Each record has ``inputs`` (question, expected_sql, expected_asset)
    and ``expectations`` (expected_sql, expected_facts, required_tables).
    """
    records: list[dict] = []
    for b in benchmarks:
        question = b.get("question", "")
        qid = b.get("question_id") or hashlib.md5(
            question.encode()
        ).hexdigest()[:8]

        records.append(
            {
                "inputs": {
                    "question": question,
                    "question_id": qid,
                    "expected_asset": b.get("expected_asset", "TABLE"),
                },
                "expectations": {
                    "expected_sql": b.get("expected_sql", ""),
                    "expected_facts": b.get("expected_facts", []),
                    "required_tables": b.get("required_tables", []),
                    "required_columns": b.get("required_columns", []),
                    "category": b.get("category", ""),
                },
            }
        )
    return records


# ═══════════════════════════════════════════════════════════════════════
# 5. Corrections
# ═══════════════════════════════════════════════════════════════════════


def apply_benchmark_corrections(
    corrections: list[dict],
    spark: Any,
    uc_schema: str,
    domain: str,
) -> dict:
    """Apply arbiter corrections to the MLflow evaluation dataset.

    Each correction dict should have:
    - ``question``: the benchmark question to correct
    - ``new_expected_sql``: the corrected SQL
    - ``verdict``: arbiter verdict (``genie_correct``, etc.)

    Returns ``{applied: int, skipped: int, errors: list[str]}``.
    """
    table_name = f"{uc_schema}.genie_benchmarks_{domain}"
    applied = 0
    skipped = 0
    errors: list[str] = []

    for c in corrections:
        question = c.get("question", "")
        new_sql = c.get("new_expected_sql", "")
        verdict = c.get("verdict", "")

        if verdict != "genie_correct":
            skipped += 1
            continue

        if not new_sql:
            errors.append(f"Empty new_expected_sql for question: {question[:50]}")
            skipped += 1
            continue

        try:
            escaped_sql = new_sql.replace("'", "\\'")
            escaped_q = question.replace("'", "\\'")
            spark.sql(
                f"""
                UPDATE {table_name}
                SET expected_sql = '{escaped_sql}',
                    corrected_by = 'arbiter',
                    correction_verdict = '{verdict}'
                WHERE question = '{escaped_q}'
                """
            )
            applied += 1
        except Exception as e:
            errors.append(f"Failed to update '{question[:50]}': {e}")
            skipped += 1

    return {"applied": applied, "skipped": skipped, "errors": errors}
