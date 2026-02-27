"""
Unity Catalog introspection helpers.

Provides two ways to fetch metadata:

1. **REST API** (preferred) – uses ``WorkspaceClient`` directly, bypasses
   Spark Connect, and avoids ``system.information_schema`` permission issues.
2. **Spark SQL** (legacy fallback) – queries ``{catalog}.information_schema``
   via a Spark session.

Preflight should prefer the REST variants; the Spark functions are kept for
backwards-compatibility and edge cases where the SDK is unavailable.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def parse_table_identifier(identifier: str) -> tuple[str, str, str]:
    """Parse a Genie space table identifier into (catalog, schema, table).

    Identifiers come from the Genie Space API ``data_sources.tables[].identifier``
    and are typically fully-qualified: ``catalog.schema.table``.
    """
    parts = identifier.replace("`", "").split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return "", parts[0], parts[1]
    return "", "", parts[0] if parts else ""


def extract_genie_space_table_refs(
    config: dict[str, Any],
) -> list[tuple[str, str, str]]:
    """Extract (catalog, schema, table) from a Genie Space config.

    Reads ``_tables``, ``_metric_views``, and ``_functions`` keys
    produced by ``fetch_space_config()``.
    """
    refs: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for key in ("_tables", "_metric_views", "_functions"):
        for identifier in config.get(key, []):
            if not identifier or identifier in seen:
                continue
            seen.add(identifier)
            refs.append(parse_table_identifier(identifier))
    return refs


def get_unique_schemas(
    refs: list[tuple[str, str, str]],
) -> list[tuple[str, str]]:
    """Deduplicate (catalog, schema) pairs from table references."""
    pairs: dict[tuple[str, str], None] = {}
    for cat, sch, _tbl in refs:
        if cat and sch:
            pairs[(cat, sch)] = None
    return list(pairs.keys())


# ---------------------------------------------------------------------------
# REST API helpers (preferred – no Spark, no information_schema permission)
# ---------------------------------------------------------------------------


def get_columns_for_tables_rest(
    w: "WorkspaceClient",
    refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Fetch column metadata via the Unity Catalog REST API.

    Calls ``w.tables.get()`` per table and extracts column info from the
    ``TableInfo.columns`` list.  Returns dicts with keys matching the Spark
    query output: ``table_name``, ``column_name``, ``data_type``, ``comment``.
    """
    rows: list[dict] = []
    failed: list[str] = []
    for cat, sch, tbl in refs:
        if not (cat and sch and tbl):
            continue
        full_name = f"{cat}.{sch}.{tbl}"
        try:
            table_info = w.tables.get(full_name=full_name)
        except Exception as exc:
            failed.append(f"{full_name}: {type(exc).__name__}: {exc}")
            continue
        if not table_info.columns:
            continue
        for col in table_info.columns:
            rows.append({
                "table_name": tbl,
                "column_name": getattr(col, "name", ""),
                "data_type": getattr(col, "type_text", ""),
                "comment": getattr(col, "comment", None) or "",
            })
    summary = (
        f"[UC_METADATA] REST get_columns_for_tables_rest: "
        f"{len(refs)} refs, {len(refs) - len(failed)} succeeded, {len(rows)} column rows"
    )
    print(summary, flush=True)
    if failed:
        for f in failed:
            print(f"[UC_METADATA]   FAILED: {f}", flush=True)
    return rows


def get_routines_for_schemas_rest(
    w: "WorkspaceClient",
    refs: list[tuple[str, str, str]],
) -> list[dict]:
    """Fetch function/routine metadata via the Unity Catalog REST API.

    Calls ``w.functions.list()`` per unique (catalog, schema) and returns
    dicts matching the Spark output: ``routine_name``, ``routine_type``,
    ``routine_definition``, ``return_type``, ``routine_schema``.
    """
    schemas = get_unique_schemas(refs)
    rows: list[dict] = []
    for cat, sch in schemas:
        try:
            for fn in w.functions.list(catalog_name=cat, schema_name=sch):
                rows.append({
                    "routine_name": getattr(fn, "name", ""),
                    "routine_type": getattr(fn, "routine_type", None)
                    or getattr(fn, "function_type", ""),
                    "routine_definition": getattr(fn, "routine_definition", "") or "",
                    "return_type": (
                        getattr(fn, "data_type", None)
                        or getattr(fn, "return_type", None)
                        or ""
                    ),
                    "routine_schema": sch,
                })
        except Exception as exc:
            print(f"[UC_METADATA] REST functions.list failed for {cat}.{sch}: {type(exc).__name__}: {exc}", flush=True)
    print(f"[UC_METADATA] REST get_routines_for_schemas_rest: {len(schemas)} schemas, {len(rows)} routines", flush=True)
    return rows


# ---------------------------------------------------------------------------
# Spark SQL helpers (legacy fallback)
# ---------------------------------------------------------------------------


def get_columns_for_tables(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
) -> "DataFrame":
    """Fetch column metadata only for the specific tables referenced by a Genie space.

    Unlike ``get_columns`` which returns all columns in a schema, this function
    queries ``information_schema`` scoped to the exact table names.
    """
    schema_groups: dict[tuple[str, str], list[str]] = {}
    for cat, sch, tbl in refs:
        if cat and sch and tbl:
            schema_groups.setdefault((cat, sch), []).append(tbl)

    unions: list[str] = []
    for (cat, sch), tables in schema_groups.items():
        safe_tables = ", ".join(f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in tables)
        unions.append(
            f"SELECT table_name, column_name, data_type, comment "
            f"FROM {cat}.information_schema.columns "
            f"WHERE table_schema = '{sch}' AND table_name IN ({safe_tables})"
        )
    if not unions:
        return spark.sql("SELECT CAST(NULL AS STRING) AS table_name, CAST(NULL AS STRING) AS column_name, CAST(NULL AS STRING) AS data_type, CAST(NULL AS STRING) AS comment WHERE 1=0")
    return spark.sql(" UNION ALL ".join(unions))


def get_tags_for_tables(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
) -> "DataFrame":
    """Fetch tags only for the specific tables referenced by a Genie space."""
    schema_groups: dict[tuple[str, str], list[str]] = {}
    for cat, sch, tbl in refs:
        if cat and sch and tbl:
            schema_groups.setdefault((cat, sch), []).append(tbl)

    unions: list[str] = []
    for (cat, sch), tables in schema_groups.items():
        safe_tables = ", ".join(f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in tables)
        unions.append(
            f"SELECT * FROM {cat}.information_schema.table_tags "
            f"WHERE schema_name = '{sch}' AND table_name IN ({safe_tables})"
        )
    if not unions:
        return spark.sql("SELECT CAST(NULL AS STRING) AS schema_name WHERE 1=0")
    return spark.sql(" UNION ALL ".join(unions))


def get_routines_for_schemas(
    spark: "SparkSession",
    refs: list[tuple[str, str, str]],
) -> "DataFrame":
    """Fetch routines for schemas that contain Genie space table references."""
    schemas = get_unique_schemas(refs)
    unions: list[str] = []
    for cat, sch in schemas:
        unions.append(
            f"SELECT routine_name, routine_type, routine_definition, "
            f"data_type AS return_type, routine_schema "
            f"FROM {cat}.INFORMATION_SCHEMA.ROUTINES "
            f"WHERE routine_schema = '{sch}'"
        )
    if not unions:
        return spark.sql("SELECT CAST(NULL AS STRING) AS routine_name WHERE 1=0")
    return spark.sql(" UNION ALL ".join(unions))


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
