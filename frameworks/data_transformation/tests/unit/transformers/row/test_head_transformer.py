"""Tests for HeadTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.row.head import HeadTransformer


class TestHeadTransformer:
    """Tests for HeadTransformer."""

    def test_head_default_n(self, sample_orders_df, empty_context):
        """Test head with default n=5."""
        transformer = HeadTransformer(name="head_default", config={})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == min(5, len(sample_orders_df))
        # Verify it's the first rows
        for i in range(len(result)):
            assert result["order_id"][i] == sample_orders_df["order_id"][i]

    def test_head_custom_n(self, sample_orders_df, empty_context):
        """Test head with custom n value."""
        transformer = HeadTransformer(name="head_custom", config={"n": 3})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 3
        for i in range(3):
            assert result["order_id"][i] == sample_orders_df["order_id"][i]

    def test_head_n_larger_than_rows(self, sample_orders_df, empty_context):
        """Test head when n is larger than DataFrame rows."""
        transformer = HeadTransformer(name="head_large", config={"n": 1000})
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Should return all rows
        assert len(result) == len(sample_orders_df)

    def test_head_n_zero(self, sample_orders_df, empty_context):
        """Test head with n=0."""
        transformer = HeadTransformer(name="head_zero", config={"n": 0})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 0
        assert result.columns == sample_orders_df.columns

    def test_head_n_one(self, sample_orders_df, empty_context):
        """Test head with n=1."""
        transformer = HeadTransformer(name="head_one", config={"n": 1})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 1
        assert result["order_id"][0] == sample_orders_df["order_id"][0]

    def test_head_empty_dataframe(self, empty_context):
        """Test head on empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "value": pl.Series([], dtype=pl.Float64)
        })
        
        transformer = HeadTransformer(name="head_empty", config={"n": 10})
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0
        assert result.columns == empty_df.columns

    def test_head_preserves_columns(self, sample_orders_df, empty_context):
        """Test that head preserves all columns."""
        transformer = HeadTransformer(name="head_columns", config={"n": 2})
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert set(result.columns) == set(sample_orders_df.columns)

    def test_head_preserves_dtypes(self, sample_orders_df, empty_context):
        """Test that head preserves column dtypes."""
        transformer = HeadTransformer(name="head_dtypes", config={"n": 2})
        result = transformer.transform(sample_orders_df, empty_context)
        
        for col in sample_orders_df.columns:
            assert result[col].dtype == sample_orders_df[col].dtype

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = HeadTransformer(name="test", config={})
        assert transformer.transformer_type == "head"

    def test_head_with_various_n_values(self, sample_customers_df, empty_context):
        """Test head with various n values."""
        for n in [1, 2, 3, 4, 5]:
            transformer = HeadTransformer(name=f"head_{n}", config={"n": n})
            result = transformer.transform(sample_customers_df, empty_context)
            expected_len = min(n, len(sample_customers_df))
            assert len(result) == expected_len
