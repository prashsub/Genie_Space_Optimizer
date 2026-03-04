"""Unit tests for the adaptive lever loop helpers.

Tests cover pure-Python functions: cluster_impact, rank_clusters,
format_reflection_buffer, _build_reflection_entry, _diminishing_returns,
and _filter_tried_clusters.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.optimizer import (
    cluster_impact,
    format_reflection_buffer,
    rank_clusters,
)
from genie_space_optimizer.optimization.harness import (
    _build_reflection_entry,
    _diminishing_returns,
    _filter_tried_clusters,
)


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_cluster(
    *,
    root_cause: str = "wrong_column",
    question_ids: list[str] | None = None,
    affected_judge: str = "schema_accuracy",
    asi_failure_type: str | None = None,
    asi_counterfactual_fixes: list[str] | None = None,
    asi_blame_set: str | None = None,
    cluster_id: str = "C001",
) -> dict:
    return {
        "cluster_id": cluster_id,
        "root_cause": root_cause,
        "question_ids": question_ids or ["q1", "q2"],
        "affected_judge": affected_judge,
        "affected_judges": [affected_judge],
        "asi_failure_type": asi_failure_type or root_cause,
        "asi_blame_set": asi_blame_set,
        "asi_counterfactual_fixes": asi_counterfactual_fixes or [],
    }


def _make_reflection(
    *,
    iteration: int = 1,
    accepted: bool = True,
    accuracy_delta: float = 5.0,
    score_deltas: dict | None = None,
    do_not_retry: list[str] | None = None,
) -> dict:
    return {
        "iteration": iteration,
        "ag_id": f"AG{iteration}",
        "accepted": accepted,
        "action": f"update_description on table_{iteration}",
        "levers": [1],
        "target_objects": [f"table_{iteration}"],
        "score_deltas": score_deltas or {"schema_accuracy": accuracy_delta},
        "accuracy_delta": accuracy_delta,
        "new_failures": None,
        "rollback_reason": None if accepted else "regression",
        "do_not_retry": do_not_retry or [],
    }


# ═══════════════════════════════════════════════════════════════════════
# cluster_impact
# ═══════════════════════════════════════════════════════════════════════


class TestClusterImpact:
    def test_basic_scoring(self):
        c = _make_cluster(question_ids=["q1", "q2", "q3"])
        score = cluster_impact(c)
        assert score > 0

    def test_more_questions_increases_score(self):
        c_small = _make_cluster(question_ids=["q1"])
        c_large = _make_cluster(question_ids=["q1", "q2", "q3"])
        assert cluster_impact(c_large) > cluster_impact(c_small)

    def test_upstream_judge_has_higher_weight(self):
        c_syntax = _make_cluster(affected_judge="syntax_validity")
        c_quality = _make_cluster(affected_judge="response_quality")
        assert cluster_impact(c_syntax) > cluster_impact(c_quality)

    def test_counterfactual_increases_fixability(self):
        c_no_cf = _make_cluster(asi_counterfactual_fixes=[])
        c_with_cf = _make_cluster(asi_counterfactual_fixes=["ADD COLUMN x"])
        assert cluster_impact(c_with_cf) > cluster_impact(c_no_cf)

    def test_severe_failure_type_scores_higher(self):
        c_wrong_table = _make_cluster(root_cause="wrong_table", asi_failure_type="wrong_table")
        c_perf = _make_cluster(root_cause="performance_issue", asi_failure_type="performance_issue")
        assert cluster_impact(c_wrong_table) > cluster_impact(c_perf)

    def test_empty_cluster(self):
        c = _make_cluster(question_ids=[])
        score = cluster_impact(c)
        assert score > 0  # min 1 question

    def test_unknown_judge_defaults(self):
        c = _make_cluster(affected_judge="some_future_judge")
        score = cluster_impact(c)
        assert score > 0


# ═══════════════════════════════════════════════════════════════════════
# rank_clusters
# ═══════════════════════════════════════════════════════════════════════


class TestRankClusters:
    def test_returns_sorted_by_impact(self):
        c1 = _make_cluster(cluster_id="C001", question_ids=["q1"])
        c2 = _make_cluster(cluster_id="C002", question_ids=["q1", "q2", "q3"])
        ranked = rank_clusters([c1, c2])
        assert ranked[0]["cluster_id"] == "C002"
        assert ranked[1]["cluster_id"] == "C001"

    def test_adds_rank_and_impact_score(self):
        c1 = _make_cluster()
        ranked = rank_clusters([c1])
        assert ranked[0]["rank"] == 1
        assert "impact_score" in ranked[0]
        assert isinstance(ranked[0]["impact_score"], float)

    def test_does_not_mutate_original(self):
        c1 = _make_cluster()
        original_keys = set(c1.keys())
        rank_clusters([c1])
        assert set(c1.keys()) == original_keys

    def test_empty_input(self):
        assert rank_clusters([]) == []


# ═══════════════════════════════════════════════════════════════════════
# format_reflection_buffer
# ═══════════════════════════════════════════════════════════════════════


class TestFormatReflectionBuffer:
    def test_empty_buffer(self):
        result = format_reflection_buffer([])
        assert "first attempt" in result.lower()

    def test_single_entry_full_detail(self):
        r = _make_reflection(iteration=1, accepted=True, accuracy_delta=5.0)
        result = format_reflection_buffer([r], full_window=3)
        assert "ITERATION 1" in result
        assert "ACCEPTED" in result

    def test_old_entries_compressed(self):
        entries = [_make_reflection(iteration=i) for i in range(1, 6)]
        result = format_reflection_buffer(entries, full_window=2)
        assert "ITERATION 4" in result
        assert "ITERATION 5" in result
        for line in result.split("\n"):
            if line.startswith("Iter "):
                assert "ACCEPTED" in line or "ROLLED_BACK" in line

    def test_do_not_retry_section(self):
        r1 = _make_reflection(
            iteration=1,
            accepted=False,
            do_not_retry=["update_description on orders"],
        )
        result = format_reflection_buffer([r1], full_window=3)
        assert "DO NOT RETRY" in result
        assert "update_description on orders" in result

    def test_score_deltas_shown(self):
        r = _make_reflection(
            iteration=1,
            accepted=True,
            score_deltas={"schema_accuracy": 5.0, "syntax_validity": -2.0},
        )
        result = format_reflection_buffer([r], full_window=3)
        assert "schema_accuracy" in result


# ═══════════════════════════════════════════════════════════════════════
# _build_reflection_entry
# ═══════════════════════════════════════════════════════════════════════


class TestBuildReflectionEntry:
    def test_accepted_entry(self):
        entry = _build_reflection_entry(
            iteration=1,
            ag_id="AG1",
            accepted=True,
            levers=[1, 5],
            target_objects=["catalog.schema.orders"],
            prev_scores={"schema_accuracy": 60.0, "syntax_validity": 80.0},
            new_scores={"schema_accuracy": 70.0, "syntax_validity": 80.0},
            rollback_reason=None,
            patches=[
                {"patch_type": "update_description", "target_object": "catalog.schema.orders"},
            ],
        )
        assert entry["accepted"] is True
        assert entry["iteration"] == 1
        assert entry["accuracy_delta"] == 5.0  # (70+80)/2 - (60+80)/2 = 5.0
        assert entry["do_not_retry"] == []

    def test_rolled_back_entry_has_do_not_retry(self):
        entry = _build_reflection_entry(
            iteration=2,
            ag_id="AG2",
            accepted=False,
            levers=[4],
            target_objects=["catalog.schema.orders"],
            prev_scores={"schema_accuracy": 70.0},
            new_scores={"schema_accuracy": 60.0},
            rollback_reason="regression",
            patches=[
                {"patch_type": "add_join_spec", "target_object": "orders_items"},
            ],
        )
        assert entry["accepted"] is False
        assert len(entry["do_not_retry"]) == 1
        assert "add_join_spec on orders_items" in entry["do_not_retry"]

    def test_score_deltas_computed(self):
        entry = _build_reflection_entry(
            iteration=1, ag_id="AG1", accepted=True, levers=[1],
            target_objects=[],
            prev_scores={"a": 50.0, "b": 60.0},
            new_scores={"a": 55.0, "b": 65.0},
            rollback_reason=None, patches=[],
        )
        assert entry["score_deltas"]["a"] == 5.0
        assert entry["score_deltas"]["b"] == 5.0


# ═══════════════════════════════════════════════════════════════════════
# _diminishing_returns
# ═══════════════════════════════════════════════════════════════════════


class TestDiminishingReturns:
    def test_not_enough_accepted_entries(self):
        buf = [_make_reflection(iteration=1, accepted=True, accuracy_delta=0.5)]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False

    def test_detects_diminishing_returns(self):
        buf = [
            _make_reflection(
                iteration=i,
                accepted=True,
                accuracy_delta=0.5,
                score_deltas={"a": 0.5},
            )
            for i in range(1, 4)
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is True

    def test_no_diminishing_returns_when_large_delta(self):
        buf = [
            _make_reflection(
                iteration=1, accepted=True,
                accuracy_delta=0.5, score_deltas={"a": 0.5},
            ),
            _make_reflection(
                iteration=2, accepted=True,
                accuracy_delta=10.0, score_deltas={"a": 10.0},
            ),
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is False

    def test_ignores_rolled_back_entries(self):
        buf = [
            _make_reflection(iteration=1, accepted=True, score_deltas={"a": 0.5}),
            _make_reflection(iteration=2, accepted=False, score_deltas={"a": -5.0}),
            _make_reflection(iteration=3, accepted=True, score_deltas={"a": 0.5}),
        ]
        assert _diminishing_returns(buf, epsilon=2.0, lookback=2) is True

    def test_empty_buffer(self):
        assert _diminishing_returns([], epsilon=2.0, lookback=2) is False


# ═══════════════════════════════════════════════════════════════════════
# _filter_tried_clusters
# ═══════════════════════════════════════════════════════════════════════


class TestFilterTriedClusters:
    def test_no_filter_when_empty(self):
        clusters = [_make_cluster(), _make_cluster(cluster_id="C002")]
        result = _filter_tried_clusters(clusters, set())
        assert len(result) == 2

    def test_removes_tried_cluster(self):
        c1 = _make_cluster(
            cluster_id="C001",
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="orders.col1",
        )
        c2 = _make_cluster(
            cluster_id="C002",
            root_cause="wrong_join",
            asi_failure_type="wrong_join",
            asi_blame_set="orders|items",
        )
        tried = {("wrong_column", "orders.col1")}
        result = _filter_tried_clusters([c1, c2], tried)
        assert len(result) == 1
        assert result[0]["cluster_id"] == "C002"

    def test_keeps_clusters_with_different_blame(self):
        c1 = _make_cluster(
            cluster_id="C001",
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="orders.col1",
        )
        c2 = _make_cluster(
            cluster_id="C002",
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="orders.col2",
        )
        tried = {("wrong_column", "orders.col1")}
        result = _filter_tried_clusters([c1, c2], tried)
        assert len(result) == 1
        assert result[0]["cluster_id"] == "C002"

    def test_all_filtered_returns_empty(self):
        c = _make_cluster(
            root_cause="wrong_column",
            asi_failure_type="wrong_column",
            asi_blame_set="x",
        )
        tried = {("wrong_column", "x")}
        result = _filter_tried_clusters([c], tried)
        assert result == []
