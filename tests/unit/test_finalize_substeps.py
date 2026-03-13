"""Unit tests for finalize print-block phases within _run_finalize().

Since the finalize function uses tightly coupled heartbeat/timeout closures
that prevent clean sub-step extraction, these tests verify the phase print
blocks and the overall return value contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def _mock_finalize_deps():
    """Patch heavy finalize dependencies."""
    with patch("genie_space_optimizer.optimization.harness.write_stage") as mock_ws, \
         patch("genie_space_optimizer.optimization.harness.update_run_status") as mock_ur, \
         patch("genie_space_optimizer.optimization.harness.promote_best_model", return_value="promoted-mv") as mock_pm, \
         patch("genie_space_optimizer.optimization.harness.generate_report", return_value="/report/path") as mock_gr, \
         patch("genie_space_optimizer.optimization.harness.all_thresholds_met", return_value=True) as mock_at:
        yield {
            "write_stage": mock_ws,
            "update_run_status": mock_ur,
            "promote_best_model": mock_pm,
            "generate_report": mock_gr,
            "all_thresholds_met": mock_at,
        }


class TestFinalizePhaseBlocks:
    """Verify that _run_finalize prints each phase header."""

    def test_prints_repeatability_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "REPEATABILITY TESTING" in captured.out

    def test_prints_review_session_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "HUMAN REVIEW SESSION" in captured.out

    def test_prints_promotion_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "MODEL PROMOTION" in captured.out

    def test_prints_terminal_status_phase(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "TERMINAL STATUS" in captured.out

    def test_prints_final_status_block(self, _mock_finalize_deps, capsys):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        captured = capsys.readouterr()
        assert "FINAL STATUS" in captured.out
        assert "CONVERGED" in captured.out


class TestFinalizeReturnValue:
    """Verify _run_finalize return value contract."""

    def test_return_keys_converged(self, _mock_finalize_deps):
        from genie_space_optimizer.optimization.harness import _run_finalize

        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 95.0}, "mv-1", 2, "cat", "gold",
            run_repeatability=False, benchmarks=None,
        )
        expected_keys = {
            "status", "convergence_reason", "repeatability_pct", "report_path",
            "promoted_model", "terminal_reason", "benchmark_publish_count",
            "labeling_session", "elapsed_seconds", "heartbeat_count",
            "uc_registration",
        }
        assert set(result.keys()) == expected_keys
        assert result["status"] == "CONVERGED"

    def test_return_keys_max_iterations(self, _mock_finalize_deps):
        from genie_space_optimizer.optimization.harness import _run_finalize

        _mock_finalize_deps["all_thresholds_met"].return_value = False
        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 50.0}, "mv-1", 3, "cat", "gold",
            run_repeatability=False, benchmarks=None, max_iterations=3,
        )
        assert result["status"] == "MAX_ITERATIONS"

    def test_return_keys_stalled(self, _mock_finalize_deps):
        from genie_space_optimizer.optimization.harness import _run_finalize

        _mock_finalize_deps["all_thresholds_met"].return_value = False
        result = _run_finalize(
            MagicMock(), MagicMock(), "run-1", "space-1", "domain", "exp",
            {"syntax_validity": 50.0}, "mv-1", 1, "cat", "gold",
            run_repeatability=False, benchmarks=None, max_iterations=5,
        )
        assert result["status"] == "STALLED"
