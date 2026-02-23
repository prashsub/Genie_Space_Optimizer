"""Unit tests for genie_space_optimizer.optimization.scorers — individual judge scorers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.evaluation import (
    build_asi_metadata,
    _extract_response_text,
)


class TestExtractResponseText:
    def test_string_passthrough(self):
        assert _extract_response_text("SELECT 1") == "SELECT 1"

    def test_dict_with_response(self):
        assert _extract_response_text({"response": "SELECT 1"}) == "SELECT 1"

    def test_dict_with_output(self):
        outputs = {"output": [{"content": [{"text": "SELECT 1"}]}]}
        assert _extract_response_text(outputs) == "SELECT 1"

    def test_empty_dict(self):
        assert _extract_response_text({}) == ""

    def test_none_like(self):
        assert _extract_response_text(0) == ""


class TestBuildAsiMetadata:
    def test_default_values(self):
        meta = build_asi_metadata()
        assert meta["failure_type"] == "other"
        assert meta["severity"] == "minor"
        assert meta["confidence"] == 0.5
        assert meta["blame_set"] == []
        assert meta["ambiguity_detected"] is False

    def test_custom_values(self):
        meta = build_asi_metadata(
            failure_type="wrong_column",
            severity="major",
            confidence=0.9,
            blame_set=["orders.amount"],
            counterfactual_fix="Use 'revenue' instead of 'amount'",
        )
        assert meta["failure_type"] == "wrong_column"
        assert meta["severity"] == "major"
        assert meta["confidence"] == 0.9
        assert "orders.amount" in meta["blame_set"]
        assert meta["counterfactual_fix"] == "Use 'revenue' instead of 'amount'"

    def test_invalid_failure_type_falls_back(self):
        meta = build_asi_metadata(failure_type="completely_invalid")
        assert meta["failure_type"] == "other"

    def test_optional_fields_none(self):
        meta = build_asi_metadata()
        assert meta["wrong_clause"] is None
        assert meta["expected_value"] is None
        assert meta["actual_value"] is None
        assert meta["missing_metadata"] is None
        assert meta["quoted_metadata_text"] is None

    def test_ambiguity_flag(self):
        meta = build_asi_metadata(ambiguity_detected=True)
        assert meta["ambiguity_detected"] is True


class TestScorerImports:
    """Verify that all scorer modules can be imported without error."""

    def test_import_syntax_validity(self):
        from genie_space_optimizer.optimization.scorers import syntax_validity  # noqa: F401

    def test_import_schema_accuracy(self):
        from genie_space_optimizer.optimization.scorers import schema_accuracy  # noqa: F401

    def test_import_logical_accuracy(self):
        from genie_space_optimizer.optimization.scorers import logical_accuracy  # noqa: F401

    def test_import_semantic_equivalence(self):
        from genie_space_optimizer.optimization.scorers import semantic_equivalence  # noqa: F401

    def test_import_completeness(self):
        from genie_space_optimizer.optimization.scorers import completeness  # noqa: F401

    def test_import_result_correctness(self):
        from genie_space_optimizer.optimization.scorers import result_correctness  # noqa: F401

    def test_import_asset_routing(self):
        from genie_space_optimizer.optimization.scorers import asset_routing  # noqa: F401

    def test_import_arbiter(self):
        from genie_space_optimizer.optimization.scorers import arbiter  # noqa: F401
