"""Lazy DatabricksSession factory for backend route handlers.

Uses ``databricks.connect`` to create a remote SparkSession that
communicates with the Databricks workspace. Cached so only one
session exists per process.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@lru_cache(maxsize=1)
def get_spark() -> SparkSession:
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless(True).getOrCreate()
