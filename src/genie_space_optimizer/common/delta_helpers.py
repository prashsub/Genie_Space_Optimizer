"""
Generic Delta table read/write utilities.

Used by ``optimization/state.py`` and the backend routes. Every function
takes a ``spark`` session as its first argument.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _fqn(catalog: str, schema: str, table: str) -> str:
    """Build a fully-qualified Delta table name."""
    return f"{catalog}.{schema}.{table}"


def read_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    filters: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Read a Delta table into a Pandas DataFrame, optionally filtered.

    ``filters`` is a dict of ``{column: value}`` equality predicates
    combined with ``AND``.
    """
    fqn = _fqn(catalog, schema, table)
    where_clauses: list[str] = []
    if filters:
        for col, val in filters.items():
            if isinstance(val, str):
                where_clauses.append(f"{col} = '{val}'")
            else:
                where_clauses.append(f"{col} = {val}")

    query = f"SELECT * FROM {fqn}"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    logger.debug("read_table: %s", query)
    return spark.sql(query).toPandas()


def insert_row(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    row_dict: dict[str, Any],
) -> None:
    """Insert a single row into a Delta table via SQL ``INSERT INTO``.

    Values are auto-quoted based on Python type (str → quoted, else raw).
    """
    fqn = _fqn(catalog, schema, table)
    columns = ", ".join(row_dict.keys())
    values = ", ".join(
        f"'{v}'" if isinstance(v, str) else str(v) for v in row_dict.values()
    )
    stmt = f"INSERT INTO {fqn} ({columns}) VALUES ({values})"
    logger.debug("insert_row: %s", stmt)
    spark.sql(stmt)


def update_row(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    key_cols: dict[str, Any],
    update_cols: dict[str, Any],
) -> None:
    """Update a row in a Delta table matching ``key_cols`` with ``update_cols``.

    Both arguments are ``{column: value}`` dicts. Key columns form the
    ``WHERE`` clause; update columns form the ``SET`` clause.
    """
    fqn = _fqn(catalog, schema, table)

    def _fmt(val: Any) -> str:
        return f"'{val}'" if isinstance(val, str) else str(val)

    set_clause = ", ".join(f"{col} = {_fmt(val)}" for col, val in update_cols.items())
    where_clause = " AND ".join(f"{col} = {_fmt(val)}" for col, val in key_cols.items())

    stmt = f"UPDATE {fqn} SET {set_clause} WHERE {where_clause}"
    logger.debug("update_row: %s", stmt)
    spark.sql(stmt)


def run_query(spark: SparkSession, sql: str) -> pd.DataFrame:
    """Execute arbitrary SQL and return results as a Pandas DataFrame."""
    logger.debug("run_query: %s", sql)
    return spark.sql(sql).toPandas()
