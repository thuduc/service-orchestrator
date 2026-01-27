"""Tests for TailTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.row.tail import TailTransformer


class TestTailTransformer:
    """Tests for TailTransformer."""

    def test_tail_default_n(self, sample_orders_df, empty_context):
        """Test tail with default n=5."""
        transformer = TailTransformer(name="tail_default", config={})
        result = transformer.transform(sample_orders_df, empty_context)
        
        expected_len = min(5, len(sample_orders_df))
        assert len(result) == expected_len
        # Verify it's the last rows
        start_idx = len(sample_orders_df) - expected_len
        for i in range(expected_len):
            assert result["order_id"][i] == sample_orders_df["order_id"][start_idx + i]

    def test_tail_custom_n(self, sample_orders_df, empty_context):
        """Test tail with custom n value."""
        transformer = TailTransformer(name="tail_custom", config={"n": 3})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 3
        start_idx = len(sample_orders_df) - 3
        for i in range(3):
            assert result["order_id"][i] == sample_orders_df["order_id"][start_idx + i]

    def test_tail_n_larger_than_rows(self, sample_orders_df, empty_context):
        """Test tail when n is larger than DataFrame rows."""
        transformer = TailTransformer(name="tail_large", config={"n": 1000})
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Should return all rows
        assert len(result) == len(sample_orders_df)

    def test_tail_n_zero(self, sample_orders_df, empty_context):
        """Test tail with n=0."""
        transformer = TailTransformer(name="tail_zero", config={"n": 0})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 0
        assert result.columns == sample_orders_df.columns

    def test_tail_n_one(self, sample_orders_df, empty_context):
        """Test tail with n=1."""
        transformer = TailTransformer(name="tail_one", config={"n": 1})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 1
        # Should be the last row
        assert result["order_id"][0] == sample_orders_df["order_id"][-1]

    def test_tail_empty_dataframe(self, empty_context):
        """Test tail on empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "value": pl.Series([], dtype=pl.Float64)
        })
        
        transformer = TailTransformer(name="tail_empty", config={"n": 10})
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0
        assert result.columns == empty_df.columns

    def test_tail_preserves_columns(self, sample_orders_df, empty_context):
        """Test that tail preserves all columns."""
        transformer = TailTransformer(name="tail_columns", config={"n": 2})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert set(result.columns) == set(sample_orders_df.columns)

    def test_tail_preserves_dtypes(self, sample_orders_df, empty_context):
        """Test that tail preserves column dtypes."""
        transformer = TailTransformer(name="tail_dtypes", config={"n": 2})
        result = transformer.transform(sample_orders_df, empty_context)
        
        for col in sample_orders_df.columns:
            assert result[col].dtype == sample_orders_df[col].dtype

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = TailTransformer(name="test", config={})
        assert transformer.transformer_type == "tail"

    def test_tail_with_various_n_values(self, sample_customers_df, empty_context):
        """Test tail with various n values."""
        for n in [1, 2, 3, 4, 5]:
            transformer = TailTransformer(name=f"tail_{n}", config={"n": n})
            result = transformer.transform(sample_customers_df, empty_context)
            expected_len = min(n, len(sample_customers_df))
            assert len(result) == expected_len

    def test_head_vs_tail_difference(self, sample_orders_df, empty_context):
        """Test that head and tail return different rows."""
        from frameworks.data_transformation.transformers.row.head import HeadTransformer
        
        head_transformer = HeadTransformer(name="head", config={"n": 2})
        tail_transformer = TailTransformer(name="tail", config={"n": 2})
        
        head_result = head_transformer.transform(sample_orders_df, empty_context)
        tail_result = tail_transformer.transform(sample_orders_df, empty_context)
        
        # They should return different rows (assuming more than 4 rows in df)
        if len(sample_orders_df) > 4:
            assert head_result["order_id"][0] != tail_result["order_id"][0]
