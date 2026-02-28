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


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _set_sql_context(spark: Any, catalog: str, gold_schema: str) -> None:
    if catalog:
        spark.sql(f"USE CATALOG {_quote_identifier(catalog)}")
    if gold_schema:
        spark.sql(f"USE SCHEMA {_quote_identifier(gold_schema)}")


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


def _extract_table_references(sql: str) -> list[tuple[str, bool]]:
    """Extract fully-qualified table references (catalog.schema.table) from SQL.

    Returns a list of ``(fqn, is_tvf)`` tuples.  *is_tvf* is ``True`` when
    the reference is immediately followed by ``(`` in the SQL, indicating
    a table-valued function call.
    """
    import re
    pattern = re.compile(
        r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+"
        r"(`[^`]+`\.`[^`]+`\.`[^`]+`"
        r"|[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*)"
        r"(\s*\()?",
        re.IGNORECASE,
    )
    seen: dict[str, bool] = {}
    for match in pattern.finditer(sql):
        ref = match.group(1).replace("`", "")
        has_paren = bool(match.group(2) and match.group(2).strip())
        if ref and ref not in seen:
            seen[ref] = has_paren
    return [(ref, is_tvf) for ref, is_tvf in seen.items()]


def _verify_table_exists(spark: Any, fqn: str, is_tvf: bool = False) -> tuple[bool, str]:
    """Check whether a table/view/metric-view exists via SELECT ... LIMIT 0.

    TVF references (``is_tvf=True``) are assumed valid since they cannot be
    verified with table-style SELECT syntax.
    """
    if is_tvf:
        return True, ""
    try:
        spark.sql(f"SELECT * FROM {_quote_identifier_fqn(fqn)} LIMIT 0")
        return True, ""
    except Exception as e:
        msg = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in msg or "cannot be found" in msg.lower():
            return False, f"Table/view does not exist: {fqn}"
        if "UNRESOLVABLE_TABLE_VALUED_FUNCTION" in msg:
            return True, ""
        return True, ""


def _quote_identifier_fqn(fqn: str) -> str:
    """Quote a fully-qualified name like catalog.schema.table."""
    parts = fqn.split(".")
    return ".".join(_quote_identifier(p) for p in parts)


def validate_ground_truth_sql(
    sql: str,
    spark: Any,
    catalog: str = "",
    gold_schema: str = "",
) -> tuple[bool, str]:
    """Validate a single expected SQL via EXPLAIN + table existence checks.

    Two-phase validation:
      1. EXPLAIN: catches syntax errors and unresolvable column references.
      2. Table existence: catches hallucinated table/view names that EXPLAIN
         sometimes doesn't catch (e.g. metric views with MEASURE() syntax).

    Returns ``(is_valid, error_message)``.
    """
    resolved = resolve_sql(sql, catalog=catalog, gold_schema=gold_schema)
    if not resolved or not resolved.strip():
        return False, "Empty SQL"

    try:
        _set_sql_context(spark, catalog, gold_schema)
        spark.sql(f"EXPLAIN {resolved}")
    except Exception as e:
        err_msg = str(e)
        if "UNRESOLVED_COLUMN" in err_msg:
            import re as _re
            col_match = _re.search(r"name `([^`]+)`", err_msg)
            suggest_match = _re.search(r"Did you mean one of the following\? \[([^\]]+)\]", err_msg)
            col_name = col_match.group(1) if col_match else "?"
            suggestion = suggest_match.group(1) if suggest_match else "?"
            return False, (
                f"UNRESOLVED_COLUMN: `{col_name}` — "
                f"suggestion: {suggestion} "
                f"(hint: use MEASURE({col_name}) for metric view measures in ORDER BY)"
            )
        return False, err_msg

    table_refs = _extract_table_references(resolved)
    for ref, is_tvf in table_refs:
        exists, err = _verify_table_exists(spark, ref, is_tvf=is_tvf)
        if not exists:
            return False, err

    return True, ""


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
