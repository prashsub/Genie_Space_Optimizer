"""Unit tests for genie_space_optimizer.optimization.optimizer — pure functions."""

from __future__ import annotations

import copy

import pytest

from genie_space_optimizer.optimization.optimizer import (
    _is_fuzzy_match,
    _map_to_lever,
    _types_compatible,
    cluster_failures,
    detect_conflicts_and_batch,
    detect_regressions,
    discover_join_candidates,
    enrich_metadata_with_uc_types,
    validate_join_spec_types,
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

    def test_missing_synonym_maps_to_lever_1(self):
        assert _map_to_lever("missing_synonym") == 1

    def test_asset_routing_error_maps_to_lever_5(self):
        assert _map_to_lever("asset_routing_error") == 5

    def test_unknown_defaults_to_lever_5(self):
        assert _map_to_lever("unknown_cause") == 5

    def test_asi_failure_type_takes_precedence(self):
        assert _map_to_lever("wrong_column", asi_failure_type="wrong_join") == 4

    def test_repeatability_tvf_blame_goes_to_lever_5(self):
        assert _map_to_lever("repeatability_issue", blame_set="TVF_calc") == 5

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


class TestDiscoverJoinCandidates:
    """Tests for the hint-based discover_join_candidates() function."""

    def _star_schema(self):
        return {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "date_key", "data_type": "INT"},
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "store_key", "data_type": "INT"},
                            {"column_name": "amount", "data_type": "DOUBLE"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_date",
                        "column_configs": [
                            {"column_name": "date_key", "data_type": "INT"},
                            {"column_name": "year", "data_type": "INT"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_product",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "product_name", "data_type": "STRING"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_store",
                        "column_configs": [
                            {"column_name": "store_key", "data_type": "INT"},
                            {"column_name": "store_name", "data_type": "STRING"},
                        ],
                    },
                ],
                "join_specs": [],
            }
        }

    def test_discovers_all_shared_key_pairs(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        table_pairs = set()
        for h in hints:
            table_pairs.add(tuple(sorted((h["left_table"], h["right_table"]))))
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_date"))) in table_pairs
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_product"))) in table_pairs
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_store"))) in table_pairs

    def test_hint_format(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        for h in hints:
            assert "left_table" in h
            assert "right_table" in h
            assert "candidate_columns" in h
            assert "type_compatible" in h
            assert isinstance(h["candidate_columns"], list)
            for cc in h["candidate_columns"]:
                assert "left_col" in cc
                assert "right_col" in cc
                assert "reason" in cc

    def test_existing_specs_are_excluded(self):
        snap = self._star_schema()
        snap["data_sources"]["join_specs"] = [
            {
                "left": {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
                "right": {"identifier": "cat.sch.dim_date", "alias": "dim_date"},
                "sql": ["`fact_sales`.`date_key` = `dim_date`.`date_key`"],
            },
        ]
        hints = discover_join_candidates(snap)
        table_pairs = set()
        for h in hints:
            table_pairs.add(tuple(sorted((h["left_table"], h["right_table"]))))
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_date"))) not in table_pairs
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_product"))) in table_pairs

    def test_no_candidates_when_no_shared_keys(self):
        snap = {
            "data_sources": {
                "tables": [
                    {"identifier": "a.b.t1", "column_configs": [{"column_name": "col_a"}]},
                    {"identifier": "a.b.t2", "column_configs": [{"column_name": "col_b"}]},
                ],
                "join_specs": [],
            }
        }
        assert discover_join_candidates(snap) == []

    def test_empty_metadata_returns_empty(self):
        assert discover_join_candidates({}) == []

    def test_candidate_columns_reported(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        for h in hints:
            assert len(h["candidate_columns"]) >= 1

    def test_type_compatible_flag(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        for h in hints:
            assert h["type_compatible"] is True

    def test_type_incompatible_flagged(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "a.b.t1",
                        "column_configs": [
                            {"column_name": "order_id", "data_type": "INT"},
                        ],
                    },
                    {
                        "identifier": "a.b.t2",
                        "column_configs": [
                            {"column_name": "order_id", "data_type": "STRING"},
                        ],
                    },
                ],
                "join_specs": [],
            }
        }
        hints = discover_join_candidates(snap)
        assert len(hints) == 1
        assert hints[0]["type_compatible"] is False

    def test_fuzzy_match_detected(self):
        """prod_key in one table matches product_key in another via stem."""
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "a.b.orders",
                        "column_configs": [
                            {"column_name": "prod_key", "data_type": "INT"},
                        ],
                    },
                    {
                        "identifier": "a.b.products",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                        ],
                    },
                ],
                "join_specs": [],
            }
        }
        hints = discover_join_candidates(snap)
        assert len(hints) == 1
        cols = hints[0]["candidate_columns"]
        assert any("fuzzy" in c["reason"] for c in cols)


class TestFuzzyMatch:
    def test_exact_match(self):
        assert _is_fuzzy_match("customer_id", "customer_id") is True

    def test_substring_match(self):
        assert _is_fuzzy_match("prod_key", "product_key") is True

    def test_stem_substring_match(self):
        assert _is_fuzzy_match("cust_id", "customer_id") is True
        assert _is_fuzzy_match("order_id", "order_key") is True
        assert _is_fuzzy_match("prod_key", "product_key") is True

    def test_no_match(self):
        assert _is_fuzzy_match("amount", "quantity") is False

    def test_same_stem_different_suffix(self):
        assert _is_fuzzy_match("product_key", "product_id") is True


class TestTypesCompatible:
    def test_same_type(self):
        assert _types_compatible("INT", "INT") is True

    def test_int_bigint(self):
        assert _types_compatible("INT", "BIGINT") is True

    def test_string_varchar(self):
        assert _types_compatible("STRING", "VARCHAR") is True

    def test_int_string_incompatible(self):
        assert _types_compatible("INT", "STRING") is False

    def test_empty_type_is_compatible(self):
        assert _types_compatible("INT", "") is True
        assert _types_compatible("", "STRING") is True

    def test_decimal_with_params(self):
        assert _types_compatible("DECIMAL(10,2)", "DOUBLE") is True

    def test_timestamp_variants(self):
        assert _types_compatible("TIMESTAMP", "TIMESTAMP_NTZ") is True


class TestEnrichMetadataWithUcTypes:
    def test_enriches_column_types(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.orders",
                        "column_configs": [
                            {"column_name": "order_id"},
                            {"column_name": "amount"},
                        ],
                    },
                ],
            }
        }
        uc_cols = [
            {"table_name": "orders", "column_name": "order_id", "data_type": "INT", "comment": "Primary key"},
            {"table_name": "orders", "column_name": "amount", "data_type": "DOUBLE", "comment": "Order total"},
        ]
        enrich_metadata_with_uc_types(snap, uc_cols)
        cc = snap["data_sources"]["tables"][0]["column_configs"]
        assert cc[0]["data_type"] == "INT"
        assert cc[0]["uc_comment"] == "Primary key"
        assert cc[1]["data_type"] == "DOUBLE"
        assert cc[1]["uc_comment"] == "Order total"

    def test_does_not_overwrite_existing_type(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.orders",
                        "column_configs": [
                            {"column_name": "order_id", "data_type": "BIGINT"},
                        ],
                    },
                ],
            }
        }
        uc_cols = [
            {"table_name": "orders", "column_name": "order_id", "data_type": "INT", "comment": "PK"},
        ]
        enrich_metadata_with_uc_types(snap, uc_cols)
        assert snap["data_sources"]["tables"][0]["column_configs"][0]["data_type"] == "BIGINT"

    def test_empty_uc_columns_no_op(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t1",
                        "column_configs": [{"column_name": "col_a"}],
                    },
                ],
            }
        }
        original = copy.deepcopy(snap)
        enrich_metadata_with_uc_types(snap, [])
        assert snap == original

    def test_unmatched_uc_rows_ignored(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.orders",
                        "column_configs": [{"column_name": "amount"}],
                    },
                ],
            }
        }
        uc_cols = [
            {"table_name": "customers", "column_name": "name", "data_type": "STRING", "comment": ""},
        ]
        enrich_metadata_with_uc_types(snap, uc_cols)
        assert "data_type" not in snap["data_sources"]["tables"][0]["column_configs"][0]


class TestValidateJoinSpecTypes:
    def _make_snap_with_types(self):
        return {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "amount", "data_type": "DOUBLE"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_product",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "product_name", "data_type": "STRING"},
                        ],
                    },
                ],
            }
        }

    def test_compatible_types_pass(self):
        snap = self._make_snap_with_types()
        spec = {
            "left": {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
            "right": {"identifier": "cat.sch.dim_product", "alias": "dim_product"},
            "sql": ["`fact_sales`.`product_key` = `dim_product`.`product_key`"],
        }
        valid, reason = validate_join_spec_types(spec, snap)
        assert valid is True

    def test_incompatible_types_rejected(self):
        snap = self._make_snap_with_types()
        spec = {
            "left": {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
            "right": {"identifier": "cat.sch.dim_product", "alias": "dim_product"},
            "sql": ["`fact_sales`.`amount` = `dim_product`.`product_name`"],
        }
        valid, reason = validate_join_spec_types(spec, snap)
        assert valid is False
        assert "incompatible" in reason.lower()

    def test_missing_types_pass(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t1",
                        "column_configs": [{"column_name": "id"}],
                    },
                    {
                        "identifier": "cat.sch.t2",
                        "column_configs": [{"column_name": "id"}],
                    },
                ],
            }
        }
        spec = {
            "left": {"identifier": "cat.sch.t1", "alias": "t1"},
            "right": {"identifier": "cat.sch.t2", "alias": "t2"},
            "sql": ["`t1`.`id` = `t2`.`id`"],
        }
        valid, reason = validate_join_spec_types(spec, snap)
        assert valid is True

    def test_empty_sql_passes(self):
        valid, reason = validate_join_spec_types({"sql": []}, {})
        assert valid is True

    def test_unparseable_condition_passes(self):
        spec = {
            "left": {"identifier": "a.b.c", "alias": "c"},
            "right": {"identifier": "a.b.d", "alias": "d"},
            "sql": ["c.id = d.id"],
        }
        valid, reason = validate_join_spec_types(spec, {})
        assert valid is True
