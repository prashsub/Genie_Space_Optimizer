"""Unit tests for genie_space_optimizer.optimization.evaluation — pure functions."""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.evaluation import (
    _compute_arbiter_adjusted_accuracy,
    _compute_asset_coverage,
    _detect_temporal_intent,
    _fill_coverage_gaps,
    _rewrite_temporal_dates,
    all_thresholds_met,
    build_temporal_note,
    normalize_result_df,
    normalize_scores,
    result_signature,
)


class TestNormalizeScores:
    def test_converts_0_1_to_0_100(self):
        scores = {"syntax": 0.95, "schema": 0.90}
        result = normalize_scores(scores)
        assert result["syntax"] == 95.0
        assert result["schema"] == 90.0

    def test_leaves_0_100_unchanged(self):
        scores = {"syntax": 95.0, "schema": 90.0}
        result = normalize_scores(scores)
        assert result["syntax"] == 95.0
        assert result["schema"] == 90.0

    def test_boundary_value_1(self):
        scores = {"perfect": 1.0}
        result = normalize_scores(scores)
        assert result["perfect"] == 100.0

    def test_boundary_value_0(self):
        scores = {"zero": 0.0}
        result = normalize_scores(scores)
        assert result["zero"] == 0.0

    def test_empty_dict(self):
        assert normalize_scores({}) == {}


class TestAllThresholdsMet:
    def test_all_met(self):
        scores = {
            "syntax_validity": 99.0,
            "schema_accuracy": 96.0,
            "logical_accuracy": 91.0,
            "semantic_equivalence": 91.0,
            "completeness": 91.0,
            "response_quality": 50.0,
            "result_correctness": 86.0,
            "asset_routing": 96.0,
        }
        assert all_thresholds_met(scores) is True

    def test_one_below(self):
        scores = {
            "syntax_validity": 99.0,
            "schema_accuracy": 96.0,
            "logical_accuracy": 91.0,
            "semantic_equivalence": 91.0,
            "completeness": 91.0,
            "response_quality": 50.0,
            "result_correctness": 80.0,  # below 85
            "asset_routing": 96.0,
        }
        assert all_thresholds_met(scores) is False

    def test_missing_judge_returns_false(self):
        scores = {"syntax_validity": 99.0}
        assert all_thresholds_met(scores) is False

    def test_custom_targets(self):
        scores = {"custom_metric": 75.0}
        targets = {"custom_metric": 70.0}
        assert all_thresholds_met(scores, targets) is True

    def test_empty_scores_with_defaults_fails(self):
        assert all_thresholds_met({}) is False


class TestNormalizeResultDf:
    def test_none_returns_empty_df(self):
        result = normalize_result_df(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame()
        result = normalize_result_df(df)
        assert result is not None
        assert result.empty

    def test_columns_lowercased(self):
        df = pd.DataFrame({"Col_A": [1, 2], "Col_B": ["x", "y"]})
        result = normalize_result_df(df)
        assert list(result.columns) == ["col_a", "col_b"]

    def test_columns_sorted(self):
        df = pd.DataFrame({"z_col": [1], "a_col": [2]})
        result = normalize_result_df(df)
        assert list(result.columns) == ["a_col", "z_col"]

    def test_floats_rounded(self):
        df = pd.DataFrame({"val": [1.123456789]})
        result = normalize_result_df(df)
        assert result["val"].iloc[0] == pytest.approx(1.1235, abs=1e-4)

    def test_strings_stripped(self):
        df = pd.DataFrame({"name": ["  alice  ", "bob  "]})
        result = normalize_result_df(df)
        assert result["name"].tolist() == ["alice", "bob"]

    def test_deterministic_row_order(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        result = normalize_result_df(df)
        assert result["a"].tolist() == [1, 2, 3]


class TestResultSignature:
    def test_none_returns_empty(self):
        sig = result_signature(None)
        assert sig["schema_hash"] == ""
        assert sig["row_count"] == 0

    def test_empty_df(self):
        sig = result_signature(pd.DataFrame())
        assert sig["row_count"] == 0

    def test_basic_signature(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        sig = result_signature(df)
        assert sig["row_count"] == 3
        assert sig["schema_hash"] != ""
        assert "a" in sig["numeric_sums"]
        assert sig["numeric_sums"]["a"] == 6.0

    def test_same_data_same_hash(self):
        df1 = pd.DataFrame({"x": [10, 20]})
        df2 = pd.DataFrame({"x": [10, 20]})
        assert result_signature(df1)["schema_hash"] == result_signature(df2)["schema_hash"]

    def test_different_schema_different_hash(self):
        df1 = pd.DataFrame({"x": [10]})
        df2 = pd.DataFrame({"y": [10]})
        assert result_signature(df1)["schema_hash"] != result_signature(df2)["schema_hash"]


class TestComputeArbiterAdjustedAccuracy:
    """Tests for arbiter-adjusted overall accuracy computation."""

    def test_empty_rows(self):
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy([])
        assert accuracy == 0.0
        assert correct == 0
        assert failures == []

    def test_all_correct(self):
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert accuracy == 100.0
        assert correct == 2
        assert failures == []

    def test_all_failed(self):
        rows = [
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "neither_correct",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert accuracy == 0.0
        assert correct == 0
        assert set(failures) == {"q1", "q2"}

    def test_arbiter_genie_correct_overrides_failure(self):
        """result_correctness=no but arbiter=genie_correct → counted as correct."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "genie_correct",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q3"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 2
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert failures == ["q3"]

    def test_arbiter_both_correct_overrides_failure(self):
        """result_correctness=no but arbiter=both_correct → counted as correct."""
        rows = [
            {"result_correctness/value": "no", "arbiter/value": "both_correct",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 1
        assert accuracy == 50.0
        assert failures == ["q2"]

    def test_rc_no_arbiter_skipped_is_failure(self):
        """result_correctness=no + arbiter=skipped → failure."""
        rows = [
            {"result_correctness/value": "no", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 0
        assert accuracy == 0.0
        assert failures == ["q1"]

    def test_mixed_scenario(self):
        """Realistic mix of verdicts."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "genie_correct",
             "inputs/question_id": "q3"},
            {"result_correctness/value": "no", "arbiter/value": "both_correct",
             "inputs/question_id": "q4"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q5"},
            {"result_correctness/value": "no", "arbiter/value": "neither_correct",
             "inputs/question_id": "q6"},
        ]
        accuracy, correct, failures, _excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert correct == 4
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert set(failures) == {"q5", "q6"}

    def test_excluded_rows_not_in_denominator(self):
        """Excluded rows are removed from the denominator."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q3"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q4"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert excluded == 1
        assert correct == 2
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert failures == ["q3"]

    def test_all_excluded_returns_zero(self):
        """All rows excluded → 0% accuracy, no failures."""
        rows = [
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert excluded == 2
        assert correct == 0
        assert accuracy == 0.0
        assert failures == []

    def test_mixed_with_exclusions(self):
        """Realistic mix including exclusions."""
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "skipped",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "genie_correct",
             "inputs/question_id": "q3"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q4"},
            {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct",
             "inputs/question_id": "q5"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert excluded == 2
        assert correct == 2
        assert accuracy == pytest.approx(66.67, abs=0.01)
        assert failures == ["q5"]


# ── Temporal Date Resolution ──────────────────────────────────────────────

from datetime import date

from genie_space_optimizer.optimization.evaluation import TemporalIntent


class TestDetectTemporalIntent:
    """Tests for _detect_temporal_intent()."""

    def test_this_year(self):
        intent = _detect_temporal_intent("Top 10 hosts this year", today=date(2026, 2, 28))
        assert intent is not None
        assert intent.keyword == "this_year"
        assert intent.start_date == date(2026, 1, 1)
        assert intent.end_date == date(2026, 2, 28)

    def test_ytd(self):
        intent = _detect_temporal_intent("Revenue YTD", today=date(2026, 3, 15))
        assert intent is not None
        assert intent.keyword == "ytd"
        assert intent.start_date == date(2026, 1, 1)
        assert intent.end_date == date(2026, 3, 15)

    def test_this_month(self):
        intent = _detect_temporal_intent("Bookings this month", today=date(2026, 2, 28))
        assert intent is not None
        assert intent.keyword == "this_month"
        assert intent.start_date == date(2026, 2, 1)
        assert intent.end_date == date(2026, 2, 28)

    def test_this_quarter(self):
        intent = _detect_temporal_intent("Revenue this quarter", today=date(2026, 5, 10))
        assert intent is not None
        assert intent.keyword == "this_quarter"
        assert intent.start_date == date(2026, 4, 1)
        assert intent.end_date == date(2026, 5, 10)

    def test_last_quarter(self):
        intent = _detect_temporal_intent("Revenue last quarter", today=date(2026, 5, 10))
        assert intent is not None
        assert intent.keyword == "last_quarter"
        assert intent.start_date == date(2026, 1, 1)
        assert intent.end_date == date(2026, 3, 31)

    def test_last_year(self):
        intent = _detect_temporal_intent("Revenue last year", today=date(2026, 2, 28))
        assert intent is not None
        assert intent.keyword == "last_year"
        assert intent.start_date == date(2025, 1, 1)
        assert intent.end_date == date(2025, 12, 31)

    def test_last_n_months(self):
        intent = _detect_temporal_intent("Revenue last 6 months", today=date(2026, 6, 15))
        assert intent is not None
        assert intent.keyword == "last_n_months"
        assert intent.start_date == date(2025, 12, 15)
        assert intent.end_date == date(2026, 6, 15)

    def test_last_n_days(self):
        intent = _detect_temporal_intent("Bookings last 30 days", today=date(2026, 3, 1))
        assert intent is not None
        assert intent.keyword == "last_n_days"
        assert intent.start_date == date(2026, 1, 30)
        assert intent.end_date == date(2026, 3, 1)

    def test_explicit_year_skips(self):
        intent = _detect_temporal_intent("Revenue for 2025 this year", today=date(2025, 6, 1))
        assert intent is None

    def test_no_temporal_keyword(self):
        intent = _detect_temporal_intent("Show me all properties in Tokyo")
        assert intent is None

    def test_empty_question(self):
        assert _detect_temporal_intent("") is None
        assert _detect_temporal_intent(None) is None


class TestRewriteTemporalDates:
    """Tests for _rewrite_temporal_dates()."""

    def test_rewrites_two_dates(self):
        gt_sql = "SELECT * FROM get_host_performance('2025-01-01', '2025-12-31', '10')"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2026-01-01'" in rewritten
        assert "'2026-02-28'" in rewritten
        assert "'2025-01-01'" not in rewritten
        assert meta is not None
        assert meta["keyword"] == "this_year"
        assert meta["original_dates"] == ["2025-01-01", "2025-12-31"]
        assert meta["rewritten_dates"] == ["2026-01-01", "2026-02-28"]

    def test_rewrites_single_date(self):
        gt_sql = "SELECT * FROM t WHERE d >= '2025-01-01'"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2026-01-01'" in rewritten
        assert meta is not None

    def test_no_dates_returns_unchanged(self):
        gt_sql = "SELECT * FROM t WHERE d >= CURRENT_DATE()"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert rewritten == gt_sql
        assert meta is None

    def test_same_year_skips_rewrite(self):
        gt_sql = "SELECT * FROM get_host_performance('2026-01-01', '2026-12-31', '10')"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert rewritten == gt_sql
        assert meta is None

    def test_empty_sql_returns_unchanged(self):
        rewritten, meta = _rewrite_temporal_dates("", TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28)))
        assert rewritten == ""
        assert meta is None

    def test_preserves_non_date_parameters(self):
        gt_sql = "SELECT * FROM get_host_performance('2025-01-01', '2025-12-31', '10')"
        intent = TemporalIntent("this_year", date(2026, 1, 1), date(2026, 2, 28))
        rewritten, _ = _rewrite_temporal_dates(gt_sql, intent)
        assert "'10'" in rewritten


class TestTemporalIntegration:
    """End-to-end tests combining detection + rewriting."""

    def test_host_performance_this_year(self):
        question = "Top 10 hosts by revenue this year"
        gt_sql = "SELECT * FROM get_host_performance('2025-01-01', '2025-12-31', '10')"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is not None
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2026-01-01'" in rewritten
        assert "'2026-02-28'" in rewritten
        assert meta is not None

    def test_booking_trends_explicit_year_not_rewritten(self):
        question = "Monthly booking trends for 2025"
        gt_sql = "SELECT * FROM get_booking_trends('2025-01-01', '2025-12-31', 'monthly')"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is None

    def test_last_6_months_revenue(self):
        question = "Revenue by destination for last 6 months"
        gt_sql = "SELECT * FROM get_revenue_summary('2024-08-01', '2025-02-01')"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is not None
        rewritten, meta = _rewrite_temporal_dates(gt_sql, intent)
        assert "'2025-08-28'" in rewritten
        assert "'2026-02-28'" in rewritten

    def test_no_rewrite_when_no_temporal(self):
        question = "Show me all properties in Tokyo"
        gt_sql = "SELECT * FROM dim_property WHERE city = 'Tokyo'"
        intent = _detect_temporal_intent(question, today=date(2026, 2, 28))
        assert intent is None


class TestBuildTemporalNote:
    """Tests for build_temporal_note()."""

    def test_returns_empty_when_no_rewrite(self):
        assert build_temporal_note({}) == ""
        assert build_temporal_note({"temporal_rewrite": None}) == ""

    def test_returns_note_when_rewrite_present(self):
        cmp = {
            "temporal_rewrite": {
                "keyword": "this_year",
                "original_dates": ["2025-01-01", "2025-12-31"],
                "rewritten_dates": ["2026-01-01", "2026-02-28"],
            }
        }
        note = build_temporal_note(cmp)
        assert "TEMPORAL CONTEXT" in note
        assert "this_year" in note
        assert "2025-01-01" in note
        assert "2026-01-01" in note


# ── Asset Coverage Gap-Fill ──────────────────────────────────────────────


class TestComputeAssetCoverage:
    """Tests for _compute_asset_coverage()."""

    def _config(self, tables=None, mvs=None, functions=None):
        return {
            "_tables": tables or [],
            "_metric_views": mvs or [],
            "_functions": functions or [],
        }

    def test_all_covered(self):
        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.customers"],
        )
        benchmarks = [
            {
                "required_tables": ["orders", "customers"],
                "expected_sql": "SELECT * FROM catalog.schema.orders JOIN catalog.schema.customers",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert result["uncovered_tables"] == set()
        assert result["uncovered_mvs"] == set()
        assert result["uncovered_functions"] == set()

    def test_with_gaps(self):
        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.customers", "catalog.schema.products"],
            mvs=["catalog.schema.revenue_mv"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert "customers" in result["uncovered_tables"]
        assert "products" in result["uncovered_tables"]
        assert "orders" not in result["uncovered_tables"]
        assert "revenue_mv" in result["uncovered_mvs"]

    def test_empty_benchmarks(self):
        config = self._config(
            tables=["catalog.schema.orders"],
            functions=["catalog.schema.get_revenue"],
        )
        result = _compute_asset_coverage([], config)
        assert "orders" in result["uncovered_tables"]
        assert "get_revenue" in result["uncovered_functions"]

    def test_empty_config(self):
        result = _compute_asset_coverage(
            [{"required_tables": ["orders"], "expected_sql": "SELECT 1"}],
            self._config(),
        )
        assert result["uncovered_tables"] == set()
        assert result["uncovered_mvs"] == set()
        assert result["uncovered_functions"] == set()

    def test_sql_reference_detection(self):
        """Assets referenced only in expected_sql (not required_tables) count as covered."""
        config = self._config(tables=["catalog.schema.orders"])
        benchmarks = [
            {
                "required_tables": [],
                "expected_sql": "SELECT * FROM catalog.schema.orders WHERE id = 1",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert result["uncovered_tables"] == set()

    def test_function_coverage(self):
        config = self._config(functions=["catalog.schema.get_host_performance"])
        benchmarks = [
            {
                "required_tables": ["get_host_performance"],
                "expected_sql": "SELECT * FROM catalog.schema.get_host_performance('2025-01-01')",
            },
        ]
        result = _compute_asset_coverage(benchmarks, config)
        assert result["uncovered_functions"] == set()


class TestFillCoverageGaps:
    """Tests for _fill_coverage_gaps()."""

    def _config(self, tables=None, mvs=None, functions=None):
        return {
            "_tables": tables or [],
            "_metric_views": mvs or [],
            "_functions": functions or [],
            "_instructions": [],
            "_parsed_space": {},
        }

    def _allowlist(self):
        return {"assets": set(), "columns": set(), "column_index": {}, "routines": set()}

    def test_no_gaps_returns_empty(self):
        config = self._config(tables=["catalog.schema.orders"])
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert result == []

    def test_soft_cap_skips(self):
        """When benchmark count is at the soft cap, gap-fill is skipped."""
        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.uncovered"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ] * 30  # well above TARGET_BENCHMARK_COUNT * 1.5 = 30
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert result == []

    @patch("genie_space_optimizer.optimization.evaluation._call_llm_for_scoring")
    @patch("genie_space_optimizer.optimization.evaluation._validate_benchmark_sql")
    @patch("genie_space_optimizer.optimization.evaluation._enforce_metadata_constraints")
    def test_generates_for_uncovered(
        self, mock_enforce, mock_validate, mock_llm,
    ):
        """Valid gap-fill benchmarks are returned with correct provenance."""
        mock_llm.return_value = [
            {
                "question": "What are the top products?",
                "expected_sql": "SELECT * FROM catalog.schema.products",
                "expected_asset": "TABLE",
                "category": "aggregation",
                "required_tables": ["products"],
                "required_columns": ["product_name"],
                "expected_facts": ["top products"],
            },
        ]
        mock_enforce.return_value = (True, "ok", "")
        mock_validate.return_value = (True, "")

        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.products"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert len(result) == 1
        assert result[0]["provenance"] == "coverage_gap_fill"
        assert result[0]["question"] == "What are the top products?"
        mock_llm.assert_called_once()

    @patch("genie_space_optimizer.optimization.evaluation._call_llm_for_scoring")
    @patch("genie_space_optimizer.optimization.evaluation._validate_benchmark_sql")
    @patch("genie_space_optimizer.optimization.evaluation._enforce_metadata_constraints")
    def test_deduplicates_existing_questions(
        self, mock_enforce, mock_validate, mock_llm,
    ):
        """Gap-fill benchmarks that duplicate existing questions are excluded."""
        mock_llm.return_value = [
            {
                "question": "What are the top products?",
                "expected_sql": "SELECT * FROM catalog.schema.products",
                "expected_asset": "TABLE",
                "category": "aggregation",
                "required_tables": ["products"],
                "required_columns": [],
                "expected_facts": [],
            },
        ]
        mock_enforce.return_value = (True, "ok", "")
        mock_validate.return_value = (True, "")

        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.products"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions={"what are the top products?"},
        )
        assert result == []

    @patch("genie_space_optimizer.optimization.evaluation._call_llm_for_scoring")
    @patch("genie_space_optimizer.optimization.evaluation._validate_benchmark_sql")
    @patch("genie_space_optimizer.optimization.evaluation._enforce_metadata_constraints")
    def test_invalid_benchmarks_filtered(
        self, mock_enforce, mock_validate, mock_llm,
    ):
        """Gap-fill benchmarks failing validation are discarded."""
        mock_llm.return_value = [
            {
                "question": "Bad question",
                "expected_sql": "SELECT * FROM fake_table",
                "expected_asset": "TABLE",
                "category": "aggregation",
                "required_tables": ["fake_table"],
                "required_columns": [],
                "expected_facts": [],
            },
        ]
        mock_enforce.return_value = (True, "ok", "")
        mock_validate.return_value = (False, "Table not found")

        config = self._config(
            tables=["catalog.schema.orders", "catalog.schema.products"],
        )
        benchmarks = [
            {
                "required_tables": ["orders"],
                "expected_sql": "SELECT * FROM catalog.schema.orders",
            },
        ]
        result = _fill_coverage_gaps(
            w=MagicMock(),
            config=config,
            uc_columns=[],
            uc_routines=[],
            benchmarks=benchmarks,
            catalog="catalog",
            schema="schema",
            spark=MagicMock(),
            allowlist=self._allowlist(),
            domain="test",
            existing_questions=set(),
        )
        assert result == []
