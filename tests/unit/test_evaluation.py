"""Unit tests for genie_space_optimizer.optimization.evaluation — pure functions."""

from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from genie_space_optimizer.optimization.evaluation import (
    all_thresholds_met,
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
    def test_none_returns_none(self):
        assert normalize_result_df(None) is None

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
        assert result["val"].iloc[0] == pytest.approx(1.123457, abs=1e-6)

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
