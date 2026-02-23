"""Unit tests for genie_space_optimizer.optimization.benchmarks — pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.optimization.benchmarks import (
    assign_splits,
    build_eval_records,
    load_benchmarks_from_dataset,
    resolve_sql,
    validate_ground_truth_sql,
)


class TestResolveSql:
    def test_substitutes_catalog(self):
        sql = "SELECT * FROM ${catalog}.${gold_schema}.orders"
        result = resolve_sql(sql, catalog="my_cat", gold_schema="my_schema")
        assert result == "SELECT * FROM my_cat.my_schema.orders"

    def test_no_substitution_when_missing(self):
        sql = "SELECT 1"
        assert resolve_sql(sql) == "SELECT 1"

    def test_empty_sql(self):
        assert resolve_sql("") == ""

    def test_partial_substitution(self):
        sql = "SELECT * FROM ${catalog}.gold.orders"
        result = resolve_sql(sql, catalog="test_cat")
        assert "test_cat" in result
        assert "${gold_schema}" not in result


class TestAssignSplits:
    def test_assigns_train_and_held_out(self, sample_benchmarks):
        result = assign_splits(sample_benchmarks, train_ratio=0.75, seed=42)
        splits = {b["split"] for b in result}
        assert "train" in splits
        assert "held_out" in splits

    def test_deterministic_with_same_seed(self, sample_benchmarks):
        import copy
        b1 = assign_splits(copy.deepcopy(sample_benchmarks), seed=42)
        b2 = assign_splits(copy.deepcopy(sample_benchmarks), seed=42)
        for a, b in zip(b1, b2):
            assert a["split"] == b["split"]

    def test_different_seed_may_differ(self, sample_benchmarks):
        import copy
        b1 = assign_splits(copy.deepcopy(sample_benchmarks), seed=1)
        b2 = assign_splits(copy.deepcopy(sample_benchmarks), seed=999)
        splits1 = [b["split"] for b in b1]
        splits2 = [b["split"] for b in b2]
        # With different seeds and few items, splits might differ
        # (not guaranteed, but likely)

    def test_ratio_respected(self):
        benchmarks = [{"id": f"q{i}", "question": f"Question {i}"} for i in range(100)]
        result = assign_splits(benchmarks, train_ratio=0.8, seed=42)
        train_count = sum(1 for b in result if b["split"] == "train")
        assert 75 <= train_count <= 85

    def test_empty_list(self):
        assert assign_splits([]) == []


class TestLoadBenchmarksFromDataset:
    def test_list_passthrough(self):
        data = [{"question": "test"}]
        result = load_benchmarks_from_dataset(data, "cat.schema", "domain")
        assert result == data

    def test_spark_session(self, mock_spark):
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"question": "test", "expected_sql": "SELECT 1"}
        mock_spark.table.return_value.collect.return_value = [mock_row]
        result = load_benchmarks_from_dataset(mock_spark, "cat.schema", "domain")
        assert len(result) == 1

    def test_spark_exception_returns_empty(self, mock_spark):
        mock_spark.table.side_effect = Exception("table not found")
        result = load_benchmarks_from_dataset(mock_spark, "cat.schema", "domain")
        assert result == []


class TestValidateGroundTruthSql:
    def test_empty_sql_invalid(self, mock_spark):
        valid, error = validate_ground_truth_sql("", mock_spark)
        assert valid is False
        assert "Empty" in error

    def test_valid_sql(self, mock_spark):
        valid, error = validate_ground_truth_sql("SELECT 1", mock_spark)
        assert valid is True
        assert error == ""

    def test_invalid_sql_returns_error(self, mock_spark):
        mock_spark.sql.side_effect = Exception("parse error")
        valid, error = validate_ground_truth_sql("INVALID SQL", mock_spark)
        assert valid is False
        assert "parse error" in error


class TestBuildEvalRecords:
    def test_basic_record_structure(self, sample_benchmarks):
        records = build_eval_records(sample_benchmarks)
        assert len(records) == len(sample_benchmarks)
        for r in records:
            assert "inputs" in r
            assert "expectations" in r
            assert "question" in r["inputs"]
            assert "question_id" in r["inputs"]
            assert "expected_sql" in r["expectations"]

    def test_empty_list(self):
        assert build_eval_records([]) == []

    def test_auto_generates_question_id(self):
        records = build_eval_records([{"question": "What is X?"}])
        assert records[0]["inputs"]["question_id"] != ""
