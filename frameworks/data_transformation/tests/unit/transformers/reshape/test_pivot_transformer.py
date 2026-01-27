"""Tests for PivotTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.reshape.pivot import PivotTransformer
from frameworks.data_transformation.exceptions import ConfigurationError


class TestPivotTransformer:
    """Tests for PivotTransformer."""

    def test_basic_pivot(self, empty_context):
        """Test basic pivot operation."""
        df = pl.DataFrame({
            "product": ["A", "A", "B", "B"],
            "month": ["Jan", "Feb", "Jan", "Feb"],
            "sales": [100, 150, 200, 250]
        })
        
        transformer = PivotTransformer(
            name="pivot_basic",
            config={
                "on": "month",
                "index": "product",
                "values": "sales"
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert "product" in result.columns
        assert "Jan" in result.columns
        assert "Feb" in result.columns
        assert len(result) == 2

    def test_pivot_with_sum_aggregate(self, empty_context):
        """Test pivot with sum aggregation."""
        df = pl.DataFrame({
            "product": ["A", "A", "A", "B"],
            "month": ["Jan", "Jan", "Feb", "Jan"],
            "sales": [100, 50, 150, 200]
        })
        
        transformer = PivotTransformer(
            name="pivot_sum",
            config={
                "on": "month",
                "index": "product",
                "values": "sales",
                "aggregate_function": "sum"
            }
        )
        result = transformer.transform(df, empty_context)
        
        # Product A in Jan should sum to 150 (100 + 50)
        a_row = result.filter(pl.col("product") == "A")
        assert a_row["Jan"][0] == 150

    def test_pivot_with_mean_aggregate(self, empty_context):
        """Test pivot with mean aggregation."""
        df = pl.DataFrame({
            "product": ["A", "A", "B"],
            "month": ["Jan", "Jan", "Jan"],
            "sales": [100.0, 200.0, 300.0]
        })
        
        transformer = PivotTransformer(
            name="pivot_mean",
            config={
                "on": "month",
                "index": "product",
                "values": "sales",
                "aggregate_function": "mean"
            }
        )
        result = transformer.transform(df, empty_context)
        
        # Product A mean should be 150
        a_row = result.filter(pl.col("product") == "A")
        assert a_row["Jan"][0] == 150.0

    def test_pivot_with_count_aggregate(self, empty_context):
        """Test pivot with count aggregation."""
        df = pl.DataFrame({
            "product": ["A", "A", "A", "B"],
            "month": ["Jan", "Jan", "Feb", "Jan"],
            "sales": [100, 50, 150, 200]
        })
        
        transformer = PivotTransformer(
            name="pivot_count",
            config={
                "on": "month",
                "index": "product",
                "values": "sales",
                "aggregate_function": "count"
            }
        )
        result = transformer.transform(df, empty_context)
        
        # Product A has 2 Jan entries
        a_row = result.filter(pl.col("product") == "A")
        assert a_row["Jan"][0] == 2

    def test_pivot_with_min_aggregate(self, empty_context):
        """Test pivot with min aggregation."""
        df = pl.DataFrame({
            "product": ["A", "A"],
            "month": ["Jan", "Jan"],
            "sales": [100, 200]
        })
        
        transformer = PivotTransformer(
            name="pivot_min",
            config={
                "on": "month",
                "index": "product",
                "values": "sales",
                "aggregate_function": "min"
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert result["Jan"][0] == 100

    def test_pivot_with_max_aggregate(self, empty_context):
        """Test pivot with max aggregation."""
        df = pl.DataFrame({
            "product": ["A", "A"],
            "month": ["Jan", "Jan"],
            "sales": [100, 200]
        })
        
        transformer = PivotTransformer(
            name="pivot_max",
            config={
                "on": "month",
                "index": "product",
                "values": "sales",
                "aggregate_function": "max"
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert result["Jan"][0] == 200

    def test_pivot_default_first_aggregate(self, empty_context):
        """Test pivot uses first as default aggregate."""
        df = pl.DataFrame({
            "product": ["A", "A"],
            "month": ["Jan", "Jan"],
            "sales": [100, 200]
        })
        
        transformer = PivotTransformer(
            name="pivot_default",
            config={
                "on": "month",
                "index": "product",
                "values": "sales"
            }
        )
        result = transformer.transform(df, empty_context)
        
        # First value should be taken (100)
        assert result["Jan"][0] == 100

    def test_pivot_multiple_on_values(self, empty_context):
        """Test pivot creates columns for each unique 'on' value."""
        df = pl.DataFrame({
            "product": ["A", "A", "A"],
            "month": ["Jan", "Feb", "Mar"],
            "sales": [100, 150, 200]
        })
        
        transformer = PivotTransformer(
            name="pivot_multi_on",
            config={
                "on": "month",
                "index": "product",
                "values": "sales"
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert set(result.columns) == {"product", "Jan", "Feb", "Mar"}

    def test_pivot_invalid_aggregate_function(self, empty_context):
        """Test pivot raises error for invalid aggregate function."""
        df = pl.DataFrame({
            "product": ["A"],
            "month": ["Jan"],
            "sales": [100]
        })
        
        transformer = PivotTransformer(
            name="pivot_invalid",
            config={
                "on": "month",
                "index": "product",
                "values": "sales",
                "aggregate_function": "invalid"
            }
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            transformer.transform(df, empty_context)
        assert "invalid" in str(exc_info.value).lower()

    def test_validate_config_missing_on(self):
        """Test validation fails when 'on' is missing."""
        transformer = PivotTransformer(
            name="test",
            config={"index": "product", "values": "sales"}
        )
        error = transformer.validate_config({"index": "product", "values": "sales"})
        
        assert error is not None
        assert "on" in error.lower()

    def test_validate_config_missing_index(self):
        """Test validation fails when 'index' is missing."""
        transformer = PivotTransformer(
            name="test",
            config={"on": "month", "values": "sales"}
        )
        error = transformer.validate_config({"on": "month", "values": "sales"})
        
        assert error is not None
        assert "index" in error.lower()

    def test_validate_config_missing_values(self):
        """Test validation fails when 'values' is missing."""
        transformer = PivotTransformer(
            name="test",
            config={"on": "month", "index": "product"}
        )
        error = transformer.validate_config({"on": "month", "index": "product"})
        
        assert error is not None
        assert "values" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"on": "month", "index": "product", "values": "sales"}
        transformer = PivotTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = PivotTransformer(
            name="test",
            config={"on": "month", "index": "product", "values": "sales"}
        )
        assert transformer.transformer_type == "pivot"

    def test_pivot_with_nulls(self, empty_context):
        """Test pivot handles null values correctly."""
        df = pl.DataFrame({
            "product": ["A", "A", "B"],
            "month": ["Jan", "Feb", "Jan"],
            "sales": [100, None, 200]
        })
        
        transformer = PivotTransformer(
            name="pivot_nulls",
            config={
                "on": "month",
                "index": "product",
                "values": "sales"
            }
        )
        result = transformer.transform(df, empty_context)
        
        # A's Feb should be null
        a_row = result.filter(pl.col("product") == "A")
        assert a_row["Feb"][0] is None
