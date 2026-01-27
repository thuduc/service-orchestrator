"""Tests for SliceTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.row.slice import SliceTransformer


class TestSliceTransformer:
    """Tests for SliceTransformer."""

    def test_slice_basic(self, sample_orders_df, empty_context):
        """Test basic slice operation."""
        transformer = SliceTransformer(
            name="slice_basic",
            config={"offset": 1, "length": 3}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 3
        # Verify it starts at the correct offset
        for i in range(3):
            assert result["order_id"][i] == sample_orders_df["order_id"][1 + i]

    def test_slice_from_beginning(self, sample_orders_df, empty_context):
        """Test slice from the beginning of DataFrame."""
        transformer = SliceTransformer(
            name="slice_begin",
            config={"offset": 0, "length": 2}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 2
        assert result["order_id"][0] == sample_orders_df["order_id"][0]
        assert result["order_id"][1] == sample_orders_df["order_id"][1]

    def test_slice_middle(self, sample_orders_df, empty_context):
        """Test slice from middle of DataFrame."""
        mid_offset = len(sample_orders_df) // 2
        transformer = SliceTransformer(
            name="slice_middle",
            config={"offset": mid_offset, "length": 2}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 2
        assert result["order_id"][0] == sample_orders_df["order_id"][mid_offset]

    def test_slice_exceeds_length(self, sample_orders_df, empty_context):
        """Test slice when length exceeds remaining rows."""
        transformer = SliceTransformer(
            name="slice_exceed",
            config={"offset": 2, "length": 1000}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Should return remaining rows from offset
        expected_len = len(sample_orders_df) - 2
        assert len(result) == expected_len

    def test_slice_zero_length(self, sample_orders_df, empty_context):
        """Test slice with zero length."""
        transformer = SliceTransformer(
            name="slice_zero",
            config={"offset": 0, "length": 0}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 0
        assert result.columns == sample_orders_df.columns

    def test_slice_offset_at_end(self, sample_orders_df, empty_context):
        """Test slice when offset is at the end of DataFrame."""
        transformer = SliceTransformer(
            name="slice_end",
            config={"offset": len(sample_orders_df) - 1, "length": 1}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 1
        assert result["order_id"][0] == sample_orders_df["order_id"][-1]

    def test_slice_offset_beyond_end(self, sample_orders_df, empty_context):
        """Test slice when offset is beyond DataFrame length."""
        transformer = SliceTransformer(
            name="slice_beyond",
            config={"offset": 1000, "length": 10}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 0

    def test_slice_empty_dataframe(self, empty_context):
        """Test slice on empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "value": pl.Series([], dtype=pl.Float64)
        })
        
        transformer = SliceTransformer(
            name="slice_empty",
            config={"offset": 0, "length": 10}
        )
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0

    def test_slice_preserves_columns(self, sample_orders_df, empty_context):
        """Test that slice preserves all columns."""
        transformer = SliceTransformer(
            name="slice_columns",
            config={"offset": 1, "length": 2}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert set(result.columns) == set(sample_orders_df.columns)

    def test_slice_preserves_dtypes(self, sample_orders_df, empty_context):
        """Test that slice preserves column dtypes."""
        transformer = SliceTransformer(
            name="slice_dtypes",
            config={"offset": 1, "length": 2}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        for col in sample_orders_df.columns:
            assert result[col].dtype == sample_orders_df[col].dtype

    def test_validate_config_missing_offset(self):
        """Test validation fails when offset is missing."""
        transformer = SliceTransformer(
            name="test",
            config={"length": 10}
        )
        error = transformer.validate_config({"length": 10})
        
        assert error is not None
        assert "offset" in error.lower()

    def test_validate_config_missing_length(self):
        """Test validation fails when length is missing."""
        transformer = SliceTransformer(
            name="test",
            config={"offset": 0}
        )
        error = transformer.validate_config({"offset": 0})
        
        assert error is not None
        assert "length" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"offset": 0, "length": 10}
        transformer = SliceTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = SliceTransformer(
            name="test",
            config={"offset": 0, "length": 10}
        )
        assert transformer.transformer_type == "slice"

    def test_slice_single_row(self, empty_context):
        """Test slice on single-row DataFrame."""
        single_df = pl.DataFrame({
            "id": [1],
            "value": [100]
        })
        
        transformer = SliceTransformer(
            name="slice_single",
            config={"offset": 0, "length": 1}
        )
        result = transformer.transform(single_df, empty_context)
        
        assert len(result) == 1
        assert result["id"][0] == 1
