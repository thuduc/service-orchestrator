"""Tests for ConcatTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.combine.concat import ConcatTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError, TransformationError


class TestConcatTransformer:
    """Tests for ConcatTransformer."""

    def test_vertical_concat(self, sample_customers_df):
        """Test vertical concatenation (stacking rows)."""
        # Create additional data
        additional_df = pl.DataFrame({
            "customer_id": [6, 7],
            "name": ["Frank", "Grace"],
            "email": ["frank@test.com", "grace@test.com"],
            "status": ["active", "inactive"],
            "age": [33, 27],
            "signup_date": ["2023-06-01", "2023-06-15"]
        })
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"additional": additional_df}
        )
        
        transformer = ConcatTransformer(
            name="concat_vertical",
            config={"datasets": ["additional"], "how": "vertical"}
        )
        result = transformer.transform(sample_customers_df, context)
        
        assert len(result) == len(sample_customers_df) + len(additional_df)
        assert set(result.columns) == set(sample_customers_df.columns)

    def test_horizontal_concat(self):
        """Test horizontal concatenation (stacking columns)."""
        df1 = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"]
        })
        df2 = pl.DataFrame({
            "value": [100, 200, 300],
            "category": ["X", "Y", "Z"]
        })
        
        context = TransformationContext(
            data=df1,
            datasets={"extra": df2}
        )
        
        transformer = ConcatTransformer(
            name="concat_horizontal",
            config={"datasets": ["extra"], "how": "horizontal"}
        )
        result = transformer.transform(df1, context)
        
        assert len(result) == 3
        assert set(result.columns) == {"id", "name", "value", "category"}

    def test_concat_multiple_datasets(self, sample_customers_df):
        """Test concatenating multiple datasets."""
        df2 = pl.DataFrame({
            "customer_id": [6],
            "name": ["Frank"],
            "email": ["frank@test.com"],
            "status": ["active"],
            "age": [33],
            "signup_date": ["2023-06-01"]
        })
        df3 = pl.DataFrame({
            "customer_id": [7],
            "name": ["Grace"],
            "email": ["grace@test.com"],
            "status": ["inactive"],
            "age": [27],
            "signup_date": ["2023-06-15"]
        })
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"df2": df2, "df3": df3}
        )
        
        transformer = ConcatTransformer(
            name="concat_multi",
            config={"datasets": ["df2", "df3"], "how": "vertical"}
        )
        result = transformer.transform(sample_customers_df, context)
        
        assert len(result) == len(sample_customers_df) + 2

    def test_concat_default_is_vertical(self, sample_customers_df):
        """Test that default concat is vertical."""
        additional_df = sample_customers_df.head(1)
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"additional": additional_df}
        )
        
        transformer = ConcatTransformer(
            name="concat_default",
            config={"datasets": ["additional"]}
        )
        result = transformer.transform(sample_customers_df, context)
        
        assert len(result) == len(sample_customers_df) + 1

    def test_concat_missing_dataset(self, sample_customers_df):
        """Test error when dataset not found in context."""
        context = TransformationContext(data=sample_customers_df)
        
        transformer = ConcatTransformer(
            name="concat_missing",
            config={"datasets": ["nonexistent"]}
        )
        
        with pytest.raises(TransformationError) as exc_info:
            transformer.transform(sample_customers_df, context)
        assert "nonexistent" in str(exc_info.value)

    def test_concat_invalid_how(self, sample_customers_df):
        """Test error with invalid 'how' parameter."""
        additional_df = sample_customers_df.head(1)
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"additional": additional_df}
        )
        
        transformer = ConcatTransformer(
            name="concat_invalid",
            config={"datasets": ["additional"], "how": "diagonal"}
        )
        
        with pytest.raises(ConfigurationError):
            transformer.transform(sample_customers_df, context)

    def test_concat_empty_dataset(self, sample_customers_df):
        """Test concatenating with empty dataset."""
        empty_df = pl.DataFrame({
            "customer_id": pl.Series([], dtype=pl.Int64),
            "name": pl.Series([], dtype=pl.Utf8),
            "email": pl.Series([], dtype=pl.Utf8),
            "status": pl.Series([], dtype=pl.Utf8),
            "age": pl.Series([], dtype=pl.Int64),
            "signup_date": pl.Series([], dtype=pl.Utf8)
        })
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"empty": empty_df}
        )
        
        transformer = ConcatTransformer(
            name="concat_empty",
            config={"datasets": ["empty"], "how": "vertical"}
        )
        result = transformer.transform(sample_customers_df, context)
        
        assert len(result) == len(sample_customers_df)

    def test_get_required_datasets(self):
        """Test get_required_datasets method."""
        transformer = ConcatTransformer(
            name="test",
            config={"datasets": ["df1", "df2"]}
        )
        
        assert transformer.get_required_datasets() == ["df1", "df2"]

    def test_validate_config_missing_datasets(self):
        """Test validation fails when datasets is missing."""
        transformer = ConcatTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None
        assert "datasets" in error.lower()

    def test_validate_config_datasets_not_list(self):
        """Test validation fails when datasets is not a list."""
        transformer = ConcatTransformer(
            name="test",
            config={"datasets": "single"}
        )
        error = transformer.validate_config({"datasets": "single"})
        
        assert error is not None
        assert "list" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"datasets": ["df1", "df2"]}
        transformer = ConcatTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = ConcatTransformer(
            name="test",
            config={"datasets": ["df1"]}
        )
        assert transformer.transformer_type == "concat"

    def test_input_type(self):
        """Test input_type property returns 'multi'."""
        transformer = ConcatTransformer(
            name="test",
            config={"datasets": ["df1"]}
        )
        assert transformer.input_type == "multi"
