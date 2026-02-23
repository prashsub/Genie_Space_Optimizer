"""API tests for /api/genie/runs endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestGetRun:
    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_get_run_returns_pipeline(self, mock_get_spark, api_client):
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "space_id": "space-abc",
                "domain": "revenue",
                "status": "CONVERGED",
                "started_at": "2025-01-01T00:00:00Z",
                "triggered_by": "test@example.com",
                "best_accuracy": 90.0,
                "convergence_reason": "threshold_met",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_stages",
            return_value=pd.DataFrame(
                [
                    {"stage": "PREFLIGHT_STARTED", "status": "COMPLETE", "started_at": "2025-01-01T00:00:00Z"},
                    {"stage": "BASELINE_EVAL", "status": "COMPLETE", "started_at": "2025-01-01T00:10:00Z"},
                ]
            ),
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame(
                [
                    {"iteration": 0, "overall_accuracy": 75.0, "eval_scope": "full"},
                ]
            ),
        ):
            response = api_client.get("/api/genie/runs/run-1")

        assert response.status_code == 200
        data = response.json()
        assert data["runId"] == "run-1"
        assert "steps" in data
        assert isinstance(data["steps"], list)
        assert len(data["steps"]) == 5

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_get_run_not_found(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value=None,
        ):
            response = api_client.get("/api/genie/runs/nonexistent")

        assert response.status_code == 404


class TestStageToStepMapping:
    """Verify the stage-to-step mapping produces exactly 5 steps."""

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_five_steps_always(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-2",
                "space_id": "space-x",
                "domain": "test",
                "status": "IN_PROGRESS",
                "started_at": "2025-01-01T00:00:00Z",
                "triggered_by": "user@test.com",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_stages",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/runs/run-2")

        assert response.status_code == 200
        data = response.json()
        assert len(data["steps"]) == 5
        step_names = [s["name"] for s in data["steps"]]
        assert "Configuration Analysis" in step_names
        assert "Baseline Evaluation" in step_names
        assert "Optimized Evaluation" in step_names


class TestGetComparison:
    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_get_comparison(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "space_id": "space-abc",
                "domain": "revenue",
                "config_snapshot": {
                    "instructions": {
                        "text_instructions": [{"id": "i1", "content": ["old"]}],
                        "example_question_sqls": [],
                    },
                    "data_sources": {"tables": []},
                },
            },
        ), patch(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            return_value={
                "_parsed_space": {
                    "instructions": {
                        "text_instructions": [{"id": "i1", "content": ["new"]}],
                        "example_question_sqls": [],
                    },
                    "data_sources": {"tables": []},
                }
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame(
                [
                    {
                        "iteration": 0,
                        "eval_scope": "full",
                        "overall_accuracy": 70.0,
                        "scores_json": '{"result_correctness": 70.0}',
                    },
                    {
                        "iteration": 3,
                        "eval_scope": "full",
                        "overall_accuracy": 88.0,
                        "scores_json": '{"result_correctness": 88.0}',
                    },
                ]
            ),
        ):
            response = api_client.get("/api/genie/runs/run-1/comparison")

        assert response.status_code == 200
        data = response.json()
        assert "baselineScore" in data
        assert "optimizedScore" in data
        assert "perDimensionScores" in data
        assert "original" in data
        assert "optimized" in data
