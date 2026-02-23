"""
Unity Catalog introspection queries.

All ``information_schema`` queries used during preflight and model
versioning. Every function takes a ``spark`` session as its first argument.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def get_columns(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch column metadata from ``information_schema.columns``.

    Returns columns: ``table_name``, ``column_name``, ``data_type``, ``comment``.
    """
    return spark.sql(
        f"SELECT table_name, column_name, data_type, comment "
        f"FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = '{schema}'"
    )


def get_tags(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch table tags from ``information_schema.table_tags``.

    Returns all columns from the ``table_tags`` view filtered to the given schema.
    """
    return spark.sql(
        f"SELECT * FROM {catalog}.information_schema.table_tags "
        f"WHERE schema_name = '{schema}'"
    )


def get_routines(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch routine (UDF / TVF) metadata from ``information_schema.routines``.

    Returns columns: ``routine_name``, ``routine_type``,
    ``routine_definition``, ``return_type``, ``routine_schema``.
    """
    return spark.sql(
        f"SELECT routine_name, routine_type, routine_definition, "
        f"data_type AS return_type, routine_schema "
        f"FROM {catalog}.INFORMATION_SCHEMA.ROUTINES "
        f"WHERE routine_schema = '{schema}'"
    )


def get_table_descriptions(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Fetch table-level comments/descriptions.

    Returns columns: ``table_name``, ``comment``.
    """
    return spark.sql(
        f"SELECT table_name, comment "
        f"FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}'"
    )


def get_metric_views(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Discover metric views in the schema.

    Metric views are VIEWs whose names conventionally start with ``mv_``.
    Returns columns: ``table_name``, ``view_definition``.
    """
    return spark.sql(
        f"SELECT table_name, view_definition "
        f"FROM {catalog}.information_schema.views "
        f"WHERE table_schema = '{schema}' AND table_name LIKE 'mv\\_%'"
    )
