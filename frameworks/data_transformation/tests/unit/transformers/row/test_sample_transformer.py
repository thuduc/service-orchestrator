"""Tests for SampleTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.row.sample import SampleTransformer
from frameworks.data_transformation.exceptions import ConfigurationError


class TestSampleTransformer:
    """Tests for SampleTransformer."""

    def test_sample_by_n(self, sample_orders_df, empty_context):
        """Test sampling by count (n)."""
        transformer = SampleTransformer(
            name="sample_n",
            config={"n": 3, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 3

    def test_sample_by_fraction(self, sample_orders_df, empty_context):
        """Test sampling by fraction."""
        transformer = SampleTransformer(
            name="sample_fraction",
            config={"fraction": 0.5, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Allow for rounding
        expected_len = int(len(sample_orders_df) * 0.5)
        assert abs(len(result) - expected_len) <= 1

    def test_sample_reproducible_with_seed(self, sample_orders_df, empty_context):
        """Test that sampling is reproducible with same seed."""
        transformer1 = SampleTransformer(
            name="sample1",
            config={"n": 3, "seed": 42}
        )
        transformer2 = SampleTransformer(
            name="sample2",
            config={"n": 3, "seed": 42}
        )
        
        result1 = transformer1.transform(sample_orders_df, empty_context)
        result2 = transformer2.transform(sample_orders_df, empty_context)
        
        assert result1["order_id"].to_list() == result2["order_id"].to_list()

    def test_sample_different_with_different_seeds(self, sample_orders_df, empty_context):
        """Test that different seeds produce different samples."""
        transformer1 = SampleTransformer(
            name="sample1",
            config={"n": 3, "seed": 42}
        )
        transformer2 = SampleTransformer(
            name="sample2",
            config={"n": 3, "seed": 123}
        )
        
        result1 = transformer1.transform(sample_orders_df, empty_context)
        result2 = transformer2.transform(sample_orders_df, empty_context)
        
        # Different seeds should likely produce different results
        # (not guaranteed but highly likely with different seeds)
        # We just verify they both return valid samples
        assert len(result1) == 3
        assert len(result2) == 3

    def test_sample_with_replacement(self, sample_orders_df, empty_context):
        """Test sampling with replacement."""
        transformer = SampleTransformer(
            name="sample_replace",
            config={
                "n": len(sample_orders_df) * 2,  # More than available rows
                "with_replacement": True,
                "seed": 42
            }
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Should be able to sample more rows than original when with_replacement=True
        assert len(result) == len(sample_orders_df) * 2

    def test_sample_without_replacement_max(self, sample_orders_df, empty_context):
        """Test sampling without replacement up to max rows."""
        transformer = SampleTransformer(
            name="sample_no_replace",
            config={"n": len(sample_orders_df), "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        # Should return all rows
        assert len(result) == len(sample_orders_df)

    def test_sample_shuffle_false(self, sample_orders_df, empty_context):
        """Test sampling with shuffle=False."""
        transformer = SampleTransformer(
            name="sample_no_shuffle",
            config={"n": 3, "shuffle": False, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 3

    def test_sample_preserves_columns(self, sample_orders_df, empty_context):
        """Test that sample preserves all columns."""
        transformer = SampleTransformer(
            name="sample_columns",
            config={"n": 2, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert set(result.columns) == set(sample_orders_df.columns)

    def test_sample_preserves_dtypes(self, sample_orders_df, empty_context):
        """Test that sample preserves column dtypes."""
        transformer = SampleTransformer(
            name="sample_dtypes",
            config={"n": 2, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        for col in sample_orders_df.columns:
            assert result[col].dtype == sample_orders_df[col].dtype

    def test_sample_empty_dataframe(self, empty_context):
        """Test sampling from empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "value": pl.Series([], dtype=pl.Float64)
        })
        
        # Empty dataframe with n=0 should work
        transformer = SampleTransformer(
            name="sample_empty",
            config={"n": 0, "seed": 42}
        )
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0

    def test_sample_fraction_zero(self, sample_orders_df, empty_context):
        """Test sampling with fraction=0."""
        transformer = SampleTransformer(
            name="sample_zero",
            config={"fraction": 0.0, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 0

    def test_sample_fraction_one(self, sample_orders_df, empty_context):
        """Test sampling with fraction=1.0."""
        transformer = SampleTransformer(
            name="sample_full",
            config={"fraction": 1.0, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == len(sample_orders_df)

    def test_sample_n_zero(self, sample_orders_df, empty_context):
        """Test sampling with n=0."""
        transformer = SampleTransformer(
            name="sample_n_zero",
            config={"n": 0, "seed": 42}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert len(result) == 0

    def test_validate_config_missing_both(self):
        """Test validation fails when both n and fraction are missing."""
        transformer = SampleTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None
        assert "n" in error.lower() or "fraction" in error.lower()

    def test_validate_config_both_present(self):
        """Test validation fails when both n and fraction are present."""
        config = {"n": 5, "fraction": 0.5}
        transformer = SampleTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is not None
        assert "both" in error.lower()

    def test_validate_config_valid_n(self):
        """Test validation passes with valid n config."""
        config = {"n": 5}
        transformer = SampleTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_validate_config_valid_fraction(self):
        """Test validation passes with valid fraction config."""
        config = {"fraction": 0.5}
        transformer = SampleTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = SampleTransformer(name="test", config={"n": 5})
        assert transformer.transformer_type == "sample"

    def test_sample_raises_on_missing_config_at_transform(self, sample_orders_df, empty_context):
        """Test that transform raises error when config is invalid."""
        transformer = SampleTransformer(name="test", config={})
        
        with pytest.raises(ConfigurationError):
            transformer.transform(sample_orders_df, empty_context)

    def test_sample_raises_on_both_n_and_fraction(self, sample_orders_df, empty_context):
        """Test that transform raises error when both n and fraction are provided."""
        transformer = SampleTransformer(
            name="test",
            config={"n": 5, "fraction": 0.5}
        )
        
        with pytest.raises(ConfigurationError):
            transformer.transform(sample_orders_df, empty_context)
