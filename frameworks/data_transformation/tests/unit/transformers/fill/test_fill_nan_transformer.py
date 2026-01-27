"""Tests for FillNanTransformer."""

import math
import polars as pl
import pytest

from frameworks.data_transformation.transformers.fill.fill_nan import FillNanTransformer


class TestFillNanTransformer:
    """Tests for FillNanTransformer."""

    def test_fill_nan_basic(self, empty_context):
        """Test basic NaN filling."""
        df = pl.DataFrame({
            "value": [1.0, float("nan"), 3.0, float("nan"), 5.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_basic",
            config={"value": 0.0}
        )
        result = transformer.transform(df, empty_context)
        
        # All NaN should be replaced with 0
        assert not any(math.isnan(v) for v in result["value"].to_list())
        assert result["value"][1] == 0.0
        assert result["value"][3] == 0.0

    def test_fill_nan_with_specific_value(self, empty_context):
        """Test filling NaN with a specific value."""
        df = pl.DataFrame({
            "temperature": [20.5, float("nan"), 22.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_specific",
            config={"value": -999.0}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["temperature"][1] == -999.0

    def test_fill_nan_specific_columns(self, empty_context):
        """Test filling NaN in specific columns only."""
        df = pl.DataFrame({
            "a": [1.0, float("nan"), 3.0],
            "b": [10.0, float("nan"), 30.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_cols",
            config={"value": 0.0, "columns": ["a"]}
        )
        result = transformer.transform(df, empty_context)
        
        # 'a' should have no NaN
        assert not any(math.isnan(v) if isinstance(v, float) else False 
                       for v in result["a"].to_list())
        # 'b' should still have NaN
        assert math.isnan(result["b"][1])

    def test_fill_nan_multiple_columns(self, empty_context):
        """Test filling NaN in multiple specific columns."""
        df = pl.DataFrame({
            "a": [1.0, float("nan"), 3.0],
            "b": [10.0, float("nan"), 30.0],
            "c": [100.0, float("nan"), 300.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_multi",
            config={"value": 0.0, "columns": ["a", "b"]}
        )
        result = transformer.transform(df, empty_context)
        
        # 'a' and 'b' should have no NaN
        assert result["a"][1] == 0.0
        assert result["b"][1] == 0.0
        # 'c' should still have NaN
        assert math.isnan(result["c"][1])

    def test_fill_nan_no_nans(self, empty_context):
        """Test when DataFrame has no NaN values."""
        df = pl.DataFrame({
            "value": [1.0, 2.0, 3.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_none",
            config={"value": 0.0}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["value"].to_list() == [1.0, 2.0, 3.0]

    def test_fill_nan_all_nans(self, empty_context):
        """Test when all values are NaN."""
        df = pl.DataFrame({
            "value": [float("nan"), float("nan"), float("nan")]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_all",
            config={"value": -1.0}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["value"].to_list() == [-1.0, -1.0, -1.0]

    def test_fill_nan_empty_dataframe(self, empty_context):
        """Test filling empty DataFrame."""
        empty_df = pl.DataFrame({
            "value": pl.Series([], dtype=pl.Float64)
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_empty",
            config={"value": 0.0}
        )
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0

    def test_fill_nan_preserves_nulls(self, empty_context):
        """Test that filling NaN preserves null values."""
        df = pl.DataFrame({
            "value": [1.0, float("nan"), None, 4.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_preserve_null",
            config={"value": 0.0}
        )
        result = transformer.transform(df, empty_context)
        
        # NaN should be replaced
        assert result["value"][1] == 0.0
        # Null should be preserved
        assert result["value"][2] is None

    def test_fill_nan_integer_columns_unchanged(self, empty_context):
        """Test that integer columns (which can't have NaN) are unchanged."""
        df = pl.DataFrame({
            "int_col": [1, 2, 3],
            "float_col": [1.0, float("nan"), 3.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_int",
            config={"value": 0.0}
        )
        result = transformer.transform(df, empty_context)
        
        # Integer column unchanged
        assert result["int_col"].to_list() == [1, 2, 3]
        # Float column NaN replaced
        assert result["float_col"][1] == 0.0

    def test_fill_nan_with_negative_value(self, empty_context):
        """Test filling NaN with negative value."""
        df = pl.DataFrame({
            "value": [1.0, float("nan"), 3.0]
        })
        
        transformer = FillNanTransformer(
            name="fill_nan_neg",
            config={"value": -99.9}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["value"][1] == -99.9

    def test_validate_config_missing_value(self):
        """Test validation fails when value is missing."""
        transformer = FillNanTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None
        assert "value" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"value": 0.0}
        transformer = FillNanTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_validate_config_valid_with_columns(self):
        """Test validation passes with columns specified."""
        config = {"value": 0.0, "columns": ["a", "b"]}
        transformer = FillNanTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = FillNanTransformer(
            name="test",
            config={"value": 0.0}
        )
        assert transformer.transformer_type == "fill_nan"

    def test_fill_nan_vs_null(self, empty_context):
        """Test that NaN and null are handled differently."""
        # NaN and null are different in Polars
        df = pl.DataFrame({
            "value": [1.0, float("nan"), None]
        })
        
        # Fill NaN only
        transformer = FillNanTransformer(
            name="fill_nan_only",
            config={"value": -1.0}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["value"][0] == 1.0
        assert result["value"][1] == -1.0  # NaN replaced
        assert result["value"][2] is None  # Null preserved
