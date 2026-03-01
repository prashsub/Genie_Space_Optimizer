"""Lazy DatabricksSession factory for backend route handlers.

Uses ``databricks.connect`` to create a remote SparkSession that
communicates with the Databricks workspace.  The session is cached
but will be automatically recreated when stale credentials are
detected (e.g. expired AWS STS tokens on the server side).
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_session: SparkSession | None = None

_CREDENTIAL_ERROR_MARKERS = (
    "InvalidAccessKeyId",
    "AccessDenied",
    "ExpiredToken",
    "s3:ListBucket",
    "AmazonS3Exception",
    "not authorized to perform",
)


def _is_credential_error(exc: BaseException) -> bool:
    msg = str(exc)
    return any(marker in msg for marker in _CREDENTIAL_ERROR_MARKERS)


def _new_session(force_new: bool = False) -> SparkSession:
    from databricks.connect import DatabricksSession

    builder = DatabricksSession.builder.serverless(True)
    if force_new:
        return builder.create()
    return builder.getOrCreate()


def get_spark() -> SparkSession:
    """Return a live serverless SparkSession, creating one if needed."""
    global _session
    with _lock:
        if _session is None:
            logger.info("Creating new serverless Spark session")
            _session = _new_session()
        return _session


def reset_spark() -> SparkSession:
    """Force-close the current session and create a completely new one.

    Uses ``.create()`` instead of ``.getOrCreate()`` to ensure the
    server-side session is not reused (stale credentials, etc.).
    """
    global _session
    with _lock:
        if _session is not None:
            try:
                _session.stop()
            except Exception:
                pass
            import time
            time.sleep(2)
            _session = None
        logger.info("Creating FRESH serverless Spark session (force_new=True)")
        _session = _new_session(force_new=True)
        return _session


def spark_with_retry(fn, *args, _max_retries: int = 2, **kwargs):
    """Execute *fn(spark, ...)* with automatic session reset on credential errors.

    Usage::

        df = spark_with_retry(lambda spark: spark.sql("SELECT 1"))
    """
    last_exc: BaseException | None = None
    for attempt in range(_max_retries):
        spark = get_spark()
        try:
            return fn(spark, *args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if _is_credential_error(exc) and attempt < _max_retries - 1:
                logger.warning(
                    "Spark credential error on attempt %d/%d — resetting session: %s",
                    attempt + 1, _max_retries, str(exc)[:200],
                )
                reset_spark()
                continue
            raise
    raise last_exc  # type: ignore[misc]
