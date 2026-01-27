"""Tests for FillNullTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.fill.fill_null import FillNullTransformer
from frameworks.data_transformation.exceptions import ConfigurationError


class TestFillNullTransformer:
    """Tests for FillNullTransformer."""

    def test_fill_null_with_value(self, df_with_nulls, empty_context):
        """Test filling nulls with a literal value."""
        transformer = FillNullTransformer(
            name="fill_value",
            config={"value": 0}
        )
        result = transformer.transform(df_with_nulls, empty_context)
        
        # All nulls should be replaced
        assert result["value"].null_count() == 0

    def test_fill_null_with_string_value(self, empty_context):
        """Test filling string nulls with a string value."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"]
        })
        
        transformer = FillNullTransformer(
            name="fill_string",
            config={"value": "Unknown"}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["name"].null_count() == 0
        assert result["name"][1] == "Unknown"

    def test_fill_null_specific_columns(self, df_with_nulls, empty_context):
        """Test filling nulls in specific columns only."""
        transformer = FillNullTransformer(
            name="fill_specific",
            config={"value": "N/A", "columns": ["name"]}
        )
        result = transformer.transform(df_with_nulls, empty_context)
        
        # name column should have no nulls
        assert result["name"].null_count() == 0
        # value column should still have nulls
        assert result["value"].null_count() == 1

    def test_fill_null_forward_strategy(self, empty_context):
        """Test filling nulls with forward strategy."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "value": [10.0, None, None, 40.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_forward",
            config={"strategy": "forward"}
        )
        result = transformer.transform(df, empty_context)
        
        # Forward fill: 10, 10, 10, 40
        assert result["value"].to_list() == [10.0, 10.0, 10.0, 40.0]

    def test_fill_null_backward_strategy(self, empty_context):
        """Test filling nulls with backward strategy."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "value": [10.0, None, None, 40.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_backward",
            config={"strategy": "backward"}
        )
        result = transformer.transform(df, empty_context)
        
        # Backward fill: 10, 40, 40, 40
        assert result["value"].to_list() == [10.0, 40.0, 40.0, 40.0]

    def test_fill_null_min_strategy(self, empty_context):
        """Test filling nulls with min strategy."""
        df = pl.DataFrame({
            "value": [10.0, None, 30.0, None, 50.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_min",
            config={"strategy": "min"}
        )
        result = transformer.transform(df, empty_context)
        
        # Min value is 10
        assert result["value"].null_count() == 0
        assert result["value"][1] == 10.0
        assert result["value"][3] == 10.0

    def test_fill_null_max_strategy(self, empty_context):
        """Test filling nulls with max strategy."""
        df = pl.DataFrame({
            "value": [10.0, None, 30.0, None, 50.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_max",
            config={"strategy": "max"}
        )
        result = transformer.transform(df, empty_context)
        
        # Max value is 50
        assert result["value"].null_count() == 0
        assert result["value"][1] == 50.0
        assert result["value"][3] == 50.0

    def test_fill_null_mean_strategy(self, empty_context):
        """Test filling nulls with mean strategy."""
        df = pl.DataFrame({
            "value": [10.0, None, 30.0, None, 50.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_mean",
            config={"strategy": "mean"}
        )
        result = transformer.transform(df, empty_context)
        
        # Mean is (10 + 30 + 50) / 3 = 30
        assert result["value"].null_count() == 0
        assert result["value"][1] == 30.0

    def test_fill_null_zero_strategy(self, empty_context):
        """Test filling nulls with zero strategy."""
        df = pl.DataFrame({
            "value": [10.0, None, 30.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_zero",
            config={"strategy": "zero"}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["value"].null_count() == 0
        assert result["value"][1] == 0.0

    def test_fill_null_one_strategy(self, empty_context):
        """Test filling nulls with one strategy."""
        df = pl.DataFrame({
            "value": [10.0, None, 30.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_one",
            config={"strategy": "one"}
        )
        result = transformer.transform(df, empty_context)
        
        assert result["value"].null_count() == 0
        assert result["value"][1] == 1.0

    def test_fill_null_strategy_with_columns(self, empty_context):
        """Test filling with strategy for specific columns."""
        df = pl.DataFrame({
            "a": [1.0, None, 3.0],
            "b": [10.0, None, 30.0]
        })
        
        transformer = FillNullTransformer(
            name="fill_strategy_cols",
            config={"strategy": "mean", "columns": ["a"]}
        )
        result = transformer.transform(df, empty_context)
        
        # 'a' should have no nulls
        assert result["a"].null_count() == 0
        # 'b' should still have null
        assert result["b"].null_count() == 1

    def test_fill_null_no_nulls(self, sample_customers_df, empty_context):
        """Test filling when DataFrame has no nulls."""
        transformer = FillNullTransformer(
            name="fill_no_nulls",
            config={"value": 0}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        # Should be unchanged
        assert result.equals(sample_customers_df)

    def test_fill_null_empty_dataframe(self, empty_context):
        """Test filling empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "value": pl.Series([], dtype=pl.Float64)
        })
        
        transformer = FillNullTransformer(
            name="fill_empty",
            config={"value": 0}
        )
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0

    def test_fill_null_missing_config(self, df_with_nulls, empty_context):
        """Test error when neither value nor strategy is provided."""
        transformer = FillNullTransformer(name="fill_invalid", config={})
        
        with pytest.raises(ConfigurationError) as exc_info:
            transformer.transform(df_with_nulls, empty_context)
        assert "value" in str(exc_info.value).lower() or "strategy" in str(exc_info.value).lower()

    def test_fill_null_both_value_and_strategy(self, df_with_nulls, empty_context):
        """Test error when both value and strategy are provided."""
        transformer = FillNullTransformer(
            name="fill_both",
            config={"value": 0, "strategy": "forward"}
        )
        
        with pytest.raises(ConfigurationError):
            transformer.transform(df_with_nulls, empty_context)

    def test_fill_null_invalid_strategy(self, df_with_nulls, empty_context):
        """Test error with invalid strategy."""
        transformer = FillNullTransformer(
            name="fill_invalid_strategy",
            config={"strategy": "invalid"}
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            transformer.transform(df_with_nulls, empty_context)
        assert "invalid" in str(exc_info.value).lower()

    def test_validate_config_missing_both(self):
        """Test validation fails when both value and strategy are missing."""
        transformer = FillNullTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None

    def test_validate_config_both_present(self):
        """Test validation fails when both value and strategy are present."""
        config = {"value": 0, "strategy": "forward"}
        transformer = FillNullTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is not None
        assert "both" in error.lower()

    def test_validate_config_valid_value(self):
        """Test validation passes with valid value config."""
        config = {"value": 0}
        transformer = FillNullTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_validate_config_valid_strategy(self):
        """Test validation passes with valid strategy config."""
        config = {"strategy": "forward"}
        transformer = FillNullTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = FillNullTransformer(
            name="test",
            config={"value": 0}
        )
        assert transformer.transformer_type == "fill_null"
