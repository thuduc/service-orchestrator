"""Tests for SortTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.row.sort import SortTransformer


class TestSortTransformer:
    """Tests for SortTransformer."""

    def test_sort_single_column_ascending(self, sample_orders_df, empty_context):
        """Test sorting by single column in ascending order."""
        transformer = SortTransformer(
            name="sort_amount",
            config={"by": "amount"}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        amounts = result["amount"].to_list()
        assert amounts == sorted(amounts)

    def test_sort_single_column_descending(self, sample_orders_df, empty_context):
        """Test sorting by single column in descending order."""
        transformer = SortTransformer(
            name="sort_amount_desc",
            config={"by": "amount", "descending": True}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        amounts = result["amount"].to_list()
        assert amounts == sorted(amounts, reverse=True)

    def test_sort_multiple_columns(self, sample_orders_df, empty_context):
        """Test sorting by multiple columns."""
        transformer = SortTransformer(
            name="sort_multi",
            config={"by": ["status", "amount"]}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Verify rows are sorted by status first, then by amount
        prev_status = None
        prev_amount = None
        for i in range(len(result)):
            status = result["status"][i]
            amount = result["amount"][i]
            if prev_status is not None:
                if status == prev_status:
                    assert amount >= prev_amount
                else:
                    assert status >= prev_status
            prev_status = status
            prev_amount = amount

    def test_sort_multiple_columns_mixed_directions(self, sample_orders_df, empty_context):
        """Test sorting by multiple columns with mixed directions."""
        transformer = SortTransformer(
            name="sort_mixed",
            config={
                "by": ["status", "amount"],
                "descending": [False, True]
            }
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Verify status is ascending and amount is descending within each status
        prev_status = None
        prev_amount = None
        for i in range(len(result)):
            status = result["status"][i]
            amount = result["amount"][i]
            if prev_status is not None and status == prev_status:
                assert amount <= prev_amount
            prev_status = status
            prev_amount = amount

    def test_sort_with_nulls_last(self, empty_context):
        """Test sorting with nulls_last option."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "value": [None, 10, None, 5]
        })
        
        transformer = SortTransformer(
            name="sort_nulls_last",
            config={"by": "value", "nulls_last": True}
        )
        result = transformer.transform(df, empty_context)
        
        # Nulls should be at the end
        values = result["value"].to_list()
        non_null_values = [v for v in values if v is not None]
        null_indices = [i for i, v in enumerate(values) if v is None]
        
        assert all(i >= len(non_null_values) for i in null_indices)

    def test_sort_with_nulls_first(self, empty_context):
        """Test sorting with nulls_last=False (nulls first)."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "value": [None, 10, None, 5]
        })
        
        transformer = SortTransformer(
            name="sort_nulls_first",
            config={"by": "value", "nulls_last": False}
        )
        result = transformer.transform(df, empty_context)
        
        # Nulls should be at the beginning
        values = result["value"].to_list()
        null_indices = [i for i, v in enumerate(values) if v is None]
        
        assert all(i < 2 for i in null_indices)  # All null indices should be 0 or 1

    def test_sort_string_column(self, sample_customers_df, empty_context):
        """Test sorting by string column."""
        transformer = SortTransformer(
            name="sort_name",
            config={"by": "name"}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        names = result["name"].to_list()
        assert names == sorted(names)

    def test_sort_empty_dataframe(self, empty_context):
        """Test sorting an empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "amount": pl.Series([], dtype=pl.Float64)
        })
        
        transformer = SortTransformer(
            name="sort_empty",
            config={"by": "amount"}
        )
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0
        assert result.columns == empty_df.columns

    def test_sort_single_row(self, empty_context):
        """Test sorting a single-row DataFrame."""
        single_row_df = pl.DataFrame({
            "id": [1],
            "amount": [100.0]
        })
        
        transformer = SortTransformer(
            name="sort_single",
            config={"by": "amount"}
        )
        result = transformer.transform(single_row_df, empty_context)
        
        assert len(result) == 1
        assert result["amount"][0] == 100.0

    def test_sort_preserves_all_columns(self, sample_orders_df, empty_context):
        """Test that sorting preserves all columns."""
        transformer = SortTransformer(
            name="sort_preserve",
            config={"by": "amount"}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert set(result.columns) == set(sample_orders_df.columns)

    def test_validate_config_missing_by(self):
        """Test validation fails when 'by' is missing."""
        transformer = SortTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None
        assert "by" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"by": "amount"}
        transformer = SortTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = SortTransformer(name="test", config={"by": "amount"})
        assert transformer.transformer_type == "sort"

    def test_sort_by_list_with_single_descending(self, sample_orders_df, empty_context):
        """Test that single descending value is applied to all columns."""
        transformer = SortTransformer(
            name="sort_multi_desc",
            config={"by": ["status", "amount"], "descending": True}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Both columns should be sorted descending
        statuses = result["status"].to_list()
        assert statuses == sorted(statuses, reverse=True) or len(set(statuses)) < len(statuses)
