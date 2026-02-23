"""Syntax validity scorer — Layer 1 CODE judge.

Validates generated SQL by executing ``EXPLAIN`` via Spark.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.genie_client import sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    _extract_response_text,
    build_asi_metadata,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _make_syntax_validity_scorer(spark: SparkSession):
    """Factory that binds ``spark`` into the scorer closure."""

    @scorer
    def syntax_validity_scorer(inputs: dict, outputs: dict) -> Feedback:
        """Check SQL syntax by running EXPLAIN."""
        sql = sanitize_sql(_extract_response_text(outputs))
        if not sql or not sql.strip():
            return Feedback(
                name="syntax_validity",
                value="no",
                rationale="No SQL generated.",
                source=CODE_SOURCE,
                metadata=build_asi_metadata(
                    failure_type="other",
                    severity="critical",
                    confidence=1.0,
                    missing_metadata="Genie returned no SQL",
                    counterfactual_fix="Check Genie Space instructions and data asset visibility",
                ),
            )
        try:
            spark.sql(f"EXPLAIN {sql}")
            return Feedback(
                name="syntax_validity",
                value="yes",
                rationale="SQL parses successfully via EXPLAIN.",
                source=CODE_SOURCE,
            )
        except Exception as e:
            error_msg = str(e)[:200]
            return Feedback(
                name="syntax_validity",
                value="no",
                rationale=f"EXPLAIN failed: {error_msg}",
                source=CODE_SOURCE,
                metadata=build_asi_metadata(
                    failure_type="other",
                    severity="critical",
                    confidence=0.9,
                    wrong_clause="SELECT",
                    actual_value=sql[:100],
                    counterfactual_fix="Fix SQL syntax in generated query",
                ),
            )

    return syntax_validity_scorer
