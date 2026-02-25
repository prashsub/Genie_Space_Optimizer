"""
Scorer assembly — 8 judges for Genie Space evaluation.

Scorers that depend on runtime context (``spark``, ``WorkspaceClient``)
expose ``_make_*`` factory functions.  Use ``make_all_scorers()`` to get
a fully bound ``all_scorers`` list ready for ``mlflow.genai.evaluate()``.

Stateless scorers (``asset_routing_scorer``, ``result_correctness_scorer``)
are importable directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .asset_routing import asset_routing_scorer
from .repeatability import repeatability_scorer
from .result_correctness import result_correctness_scorer

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession


def make_all_scorers(
    w: WorkspaceClient,
    spark: SparkSession,
    catalog: str,
    schema: str,
    loaded_prompts: dict[str, str] | None = None,
) -> list:
    """Assemble all 8 scorers with bound runtime context.

    Returns an ordered list suitable for passing to
    ``mlflow.genai.evaluate(scorers=...)``.
    """
    from .arbiter import _make_arbiter_scorer
    from .completeness import _make_completeness_judge
    from .logical_accuracy import _make_logical_accuracy_judge
    from .schema_accuracy import _make_schema_accuracy_judge
    from .semantic_equivalence import _make_semantic_equivalence_judge
    from .syntax_validity import _make_syntax_validity_scorer

    return [
        _make_syntax_validity_scorer(spark, catalog, schema),
        _make_schema_accuracy_judge(w, catalog, schema),
        _make_logical_accuracy_judge(w, catalog, schema),
        _make_semantic_equivalence_judge(w, catalog, schema),
        _make_completeness_judge(w, catalog, schema),
        asset_routing_scorer,
        result_correctness_scorer,
        _make_arbiter_scorer(w, catalog, schema, loaded_prompts),
    ]


def make_repeatability_scorers() -> list:
    """Return scorers for repeatability-only evaluation runs.

    Uses the repeatability scorer plus result_correctness (to track whether
    the answer is still correct) and asset_routing.
    """
    return [
        repeatability_scorer,
        result_correctness_scorer,
        asset_routing_scorer,
    ]


__all__ = [
    "asset_routing_scorer",
    "repeatability_scorer",
    "result_correctness_scorer",
    "make_all_scorers",
    "make_repeatability_scorers",
]
