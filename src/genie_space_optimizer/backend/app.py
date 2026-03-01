import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from .core import create_app
from .core._base import LifespanDependency
from .router import router

# Import route modules so their @router decorators register on the singleton.
# The singleton router (from create_router()) collects all routes under /api/genie.
from .routes import spaces as _spaces  # noqa: F401
from .routes import runs as _runs  # noqa: F401
from .routes import activity as _activity  # noqa: F401
from .routes import settings as _settings  # noqa: F401
from .routes import trigger as _trigger  # noqa: F401

logger = logging.getLogger(__name__)


class _WheelHealthCheck(LifespanDependency):
    """Log the resolved wheel at startup for immediate deploy verification."""

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        try:
            from .job_launcher import _find_wheel

            wheel = _find_wheel()
            logger.info(
                "Wheel health: %s (size=%d bytes)",
                wheel.name,
                wheel.stat().st_size,
            )
        except Exception:
            logger.warning("Wheel health check failed — no wheel found", exc_info=True)
        yield

    @staticmethod
    def __call__() -> None:
        return None


class _DeltaTableBootstrap(LifespanDependency):
    """Create optimization Delta tables on app startup so read paths never 404."""

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        try:
            from ._spark import get_spark
            from genie_space_optimizer.optimization.state import ensure_optimization_tables

            config = app.state.config
            spark = get_spark()
            ensure_optimization_tables(spark, config.catalog, config.schema_name)
            logger.info("Optimization Delta tables verified at startup")
        except Exception:
            logger.warning(
                "Could not verify optimization tables at startup — will create on first use",
                exc_info=True,
            )
        yield

    @staticmethod
    def __call__() -> None:
        return None


app = create_app(routers=[router])
