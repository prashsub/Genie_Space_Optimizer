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


class TestResultCorrectnessScorer:
    """Functional tests for the result_correctness scorer."""

    def test_gt_zero_genie_unavailable_is_yes(self):
        """Null-result defense: GT=0 rows + Genie unavailable → yes."""
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "How many unicorns?"},
            outputs={
                "response": "SELECT COUNT(*) FROM unicorns",
                "comparison": {
                    "match": False,
                    "error": "Could not retrieve Genie query results (no statement_id)",
                    "error_type": "genie_result_unavailable",
                    "gt_rows": 0,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT COUNT(*) FROM unicorns"},
        )
        assert feedback.value == "yes"

    def test_gt_nonzero_genie_unavailable_is_no(self):
        """GT has rows but Genie unavailable → still 'no'."""
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "List all orders"},
            outputs={
                "response": "SELECT * FROM orders",
                "comparison": {
                    "match": False,
                    "error": "Could not retrieve Genie query results",
                    "error_type": "genie_result_unavailable",
                    "gt_rows": 5,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT * FROM orders"},
        )
        assert feedback.value == "no"

    def test_match_is_yes(self):
        """When comparison says match=True → yes."""
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "Total revenue"},
            outputs={
                "response": "SELECT SUM(amount) FROM orders",
                "comparison": {
                    "match": True,
                    "match_type": "exact",
                    "gt_rows": 1,
                    "genie_rows": 1,
                    "gt_hash": "abc",
                    "genie_hash": "abc",
                    "error": None,
                },
            },
            expectations={"expected_response": "SELECT SUM(amount) FROM orders"},
        )
        assert feedback.value == "yes"

    def test_mismatch_is_no(self):
        """Hash mismatch with no error → no."""
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "Total revenue"},
            outputs={
                "response": "SELECT SUM(amount) FROM orders",
                "comparison": {
                    "match": False,
                    "match_type": "mismatch",
                    "gt_rows": 10,
                    "genie_rows": 5,
                    "gt_hash": "abc",
                    "genie_hash": "xyz",
                    "error": None,
                },
            },
            expectations={"expected_response": "SELECT SUM(amount) FROM orders"},
        )
        assert feedback.value == "no"


class TestResultCorrectnessGTExclusion:
    """GT-side infrastructure failures return 'excluded'."""

    def test_infrastructure_error_is_excluded(self):
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "Total revenue"},
            outputs={
                "response": "SELECT SUM(amount) FROM orders",
                "comparison": {
                    "match": False,
                    "error": "ground_truth SQL compilation failed: PARSE_SYNTAX_ERROR",
                    "error_type": "infrastructure",
                    "gt_rows": 0,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT SUM(amount) FROM orders"},
        )
        assert feedback.value == "excluded"

    def test_permission_blocked_is_excluded(self):
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "Call custom function"},
            outputs={
                "response": "SELECT get_metric()",
                "comparison": {
                    "match": False,
                    "error": "Missing function(s) in GT SQL: get_metric",
                    "error_type": "permission_blocked",
                    "gt_rows": 0,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT get_metric()"},
        )
        assert feedback.value == "excluded"

    def test_query_execution_error_is_excluded(self):
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "Complex query"},
            outputs={
                "response": "SELECT * FROM big_table",
                "comparison": {
                    "match": False,
                    "error": "Query execution failed: out of memory",
                    "error_type": "query_execution",
                    "gt_rows": 0,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT * FROM big_table"},
        )
        assert feedback.value == "excluded"

    def test_no_genie_sql_still_no(self):
        """Genie-side failure stays 'no', not excluded."""
        from genie_space_optimizer.optimization.scorers.result_correctness import (
            result_correctness_scorer,
        )

        feedback = result_correctness_scorer._original_func(
            inputs={"question": "What is the revenue?"},
            outputs={
                "response": "",
                "comparison": {
                    "match": False,
                    "error": "Genie did not return SQL",
                    "error_type": "no_genie_sql",
                    "gt_rows": 0,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT SUM(amount) FROM orders"},
        )
        assert feedback.value == "no"


class TestArbiterScorerNullDefense:
    """Functional tests for the arbiter scorer null-result defense."""

    def test_gt_zero_genie_unavailable_is_both_correct(self):
        """Null-result defense: GT=0 + Genie unavailable → both_correct."""
        from genie_space_optimizer.optimization.scorers.arbiter import _make_arbiter_scorer

        mock_ws = MagicMock()
        scorer_fn = _make_arbiter_scorer(mock_ws, "catalog", "schema")

        feedback = scorer_fn._original_func(
            inputs={"question": "How many unicorns?"},
            outputs={
                "response": "SELECT COUNT(*) FROM unicorns",
                "comparison": {
                    "match": False,
                    "error": "Could not retrieve Genie query results (no statement_id)",
                    "error_type": "genie_result_unavailable",
                    "gt_rows": 0,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT COUNT(*) FROM unicorns"},
        )
        assert feedback.value == "both_correct"

    def test_other_error_is_skipped(self):
        """Non-null-result errors still produce 'skipped'."""
        from genie_space_optimizer.optimization.scorers.arbiter import _make_arbiter_scorer

        mock_ws = MagicMock()
        scorer_fn = _make_arbiter_scorer(mock_ws, "catalog", "schema")

        feedback = scorer_fn._original_func(
            inputs={"question": "List orders"},
            outputs={
                "response": "SELECT * FROM orders",
                "comparison": {
                    "match": False,
                    "error": "ground_truth SQL compilation failed: syntax error",
                    "error_type": "infrastructure",
                    "gt_rows": 0,
                    "genie_rows": 0,
                },
            },
            expectations={"expected_response": "SELECT * FROM orders"},
        )
        assert feedback.value == "skipped"

    def test_match_is_both_correct(self):
        """When results match, arbiter returns both_correct."""
        from genie_space_optimizer.optimization.scorers.arbiter import _make_arbiter_scorer

        mock_ws = MagicMock()
        scorer_fn = _make_arbiter_scorer(mock_ws, "catalog", "schema")

        feedback = scorer_fn._original_func(
            inputs={"question": "Total revenue"},
            outputs={
                "response": "SELECT SUM(amount) FROM orders",
                "comparison": {
                    "match": True,
                    "match_type": "exact",
                    "error": None,
                },
            },
            expectations={"expected_response": "SELECT SUM(amount) FROM orders"},
        )
        assert feedback.value == "both_correct"
