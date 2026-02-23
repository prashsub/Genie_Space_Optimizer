"""Unit tests for genie_space_optimizer.optimization.optimizer — pure functions."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.optimizer import (
    _map_to_lever,
    cluster_failures,
    detect_conflicts_and_batch,
    detect_regressions,
    validate_patch_set,
)


class TestMapToLever:
    def test_wrong_column_maps_to_lever_1(self):
        assert _map_to_lever("wrong_column") == 1

    def test_wrong_table_maps_to_lever_1(self):
        assert _map_to_lever("wrong_table") == 1

    def test_description_mismatch_maps_to_lever_1(self):
        assert _map_to_lever("description_mismatch") == 1

    def test_wrong_aggregation_maps_to_lever_2(self):
        assert _map_to_lever("wrong_aggregation") == 2

    def test_missing_filter_maps_to_lever_2(self):
        assert _map_to_lever("missing_filter") == 2

    def test_tvf_parameter_error_maps_to_lever_3(self):
        assert _map_to_lever("tvf_parameter_error") == 3

    def test_wrong_join_maps_to_lever_4(self):
        assert _map_to_lever("wrong_join") == 4

    def test_missing_synonym_maps_to_lever_5(self):
        assert _map_to_lever("missing_synonym") == 5

    def test_asset_routing_error_maps_to_lever_6(self):
        assert _map_to_lever("asset_routing_error") == 6

    def test_unknown_defaults_to_lever_6(self):
        assert _map_to_lever("unknown_cause") == 6

    def test_asi_failure_type_takes_precedence(self):
        assert _map_to_lever("wrong_column", asi_failure_type="wrong_join") == 4

    def test_repeatability_tvf_blame_goes_to_lever_6(self):
        assert _map_to_lever("repeatability_issue", blame_set="TVF_calc") == 6

    def test_repeatability_non_tvf_goes_to_lever_1(self):
        assert _map_to_lever("repeatability_issue", blame_set="TABLE_orders") == 1


class TestClusterFailures:
    def test_empty_results_returns_empty(self):
        assert cluster_failures({}, {}) == []

    def test_clusters_from_feedback_rows(self, sample_eval_results):
        clusters = cluster_failures(sample_eval_results, {})
        assert isinstance(clusters, list)
        for c in clusters:
            assert "cluster_id" in c
            assert "root_cause" in c
            assert "question_ids" in c

    def test_singletons_excluded(self):
        """Clusters with only 1 question should be excluded (< 2 threshold)."""
        eval_results = {
            "eval_results": [
                {
                    "inputs/question_id": "q1",
                    "feedback/syntax": "no",
                    "rationale/syntax": "unique error that won't cluster",
                },
            ]
        }
        clusters = cluster_failures(eval_results, {})
        for c in clusters:
            assert len(c["question_ids"]) >= 2

    def test_clusters_sorted_by_size(self, sample_eval_results):
        clusters = cluster_failures(sample_eval_results, {})
        if len(clusters) > 1:
            for i in range(len(clusters) - 1):
                assert len(clusters[i]["question_ids"]) >= len(clusters[i + 1]["question_ids"])


class TestDetectRegressions:
    def test_no_regression_when_scores_improve(self):
        prev = {"judge_a": 80.0, "judge_b": 70.0}
        curr = {"judge_a": 85.0, "judge_b": 75.0}
        assert detect_regressions(curr, prev) == []

    def test_regression_detected_on_drop(self):
        prev = {"judge_a": 80.0}
        curr = {"judge_a": 75.0}
        regressions = detect_regressions(curr, prev, threshold=2.0)
        assert len(regressions) == 1
        assert regressions[0]["judge"] == "judge_a"
        assert regressions[0]["drop"] == pytest.approx(5.0)

    def test_no_regression_within_threshold(self):
        prev = {"judge_a": 80.0}
        curr = {"judge_a": 79.0}
        assert detect_regressions(curr, prev, threshold=2.0) == []

    def test_multiple_regressions(self):
        prev = {"a": 90.0, "b": 85.0, "c": 70.0}
        curr = {"a": 80.0, "b": 75.0, "c": 70.0}
        regressions = detect_regressions(curr, prev, threshold=2.0)
        assert len(regressions) == 2


class TestDetectConflictsAndBatch:
    def test_no_conflicts_single_batch(self):
        proposals = [
            {"patch_type": "add_description"},
            {"patch_type": "add_column_description"},
        ]
        batches = detect_conflicts_and_batch(proposals)
        assert len(batches) == 1
        assert len(batches[0]) == 2

    def test_empty_proposals(self):
        assert detect_conflicts_and_batch([]) == []

    def test_single_proposal(self):
        batches = detect_conflicts_and_batch([{"patch_type": "add_instruction"}])
        assert len(batches) == 1
        assert len(batches[0]) == 1


class TestValidatePatchSet:
    def test_empty_patch_set_is_valid(self):
        valid, errors = validate_patch_set([], None)
        assert valid is True
        assert errors == []

    def test_unknown_type_flagged(self):
        patches = [{"type": "totally_made_up_type"}]
        valid, errors = validate_patch_set(patches)
        assert valid is False
        assert any("unknown type" in e for e in errors)

    def test_valid_patch_types_pass(self):
        patches = [{"type": "add_instruction", "target": "test"}]
        valid, errors = validate_patch_set(patches)
        assert valid is True

    def test_metadata_validation_missing_table(self, sample_metadata):
        patches = [{"type": "add_description", "table": "nonexistent_table"}]
        valid, errors = validate_patch_set(patches, sample_metadata)
        assert valid is False
        assert any("not found" in e for e in errors)

    def test_metadata_validation_missing_column(self, sample_metadata):
        patches = [{"type": "add_column_description", "table": "orders", "column": "no_such_col"}]
        valid, errors = validate_patch_set(patches, sample_metadata)
        assert valid is False
