"""Tests for UnpivotTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.reshape.unpivot import UnpivotTransformer


class TestUnpivotTransformer:
    """Tests for UnpivotTransformer."""

    def test_basic_unpivot(self, empty_context):
        """Test basic unpivot operation."""
        df = pl.DataFrame({
            "product": ["A", "B"],
            "jan_sales": [100, 200],
            "feb_sales": [150, 250]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_basic",
            config={
                "on": ["jan_sales", "feb_sales"],
                "index": ["product"]
            }
        )
        result = transformer.transform(df, empty_context)
        
        # Should have 4 rows (2 products x 2 months)
        assert len(result) == 4
        assert "product" in result.columns
        assert "variable" in result.columns
        assert "value" in result.columns

    def test_unpivot_with_custom_names(self, empty_context):
        """Test unpivot with custom variable and value names."""
        df = pl.DataFrame({
            "product": ["A", "B"],
            "jan_sales": [100, 200],
            "feb_sales": [150, 250]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_custom",
            config={
                "on": ["jan_sales", "feb_sales"],
                "index": ["product"],
                "variable_name": "month",
                "value_name": "sales"
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert "month" in result.columns
        assert "sales" in result.columns

    def test_unpivot_without_index(self, empty_context):
        """Test unpivot without specifying index."""
        df = pl.DataFrame({
            "jan": [100],
            "feb": [150],
            "mar": [200]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_no_index",
            config={"on": ["jan", "feb", "mar"]}
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 3
        assert set(result["variable"].to_list()) == {"jan", "feb", "mar"}

    def test_unpivot_preserves_values(self, empty_context):
        """Test that unpivot preserves correct values."""
        df = pl.DataFrame({
            "id": [1, 2],
            "q1": [100, 200],
            "q2": [150, 250]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_values",
            config={
                "on": ["q1", "q2"],
                "index": ["id"]
            }
        )
        result = transformer.transform(df, empty_context)
        
        # Check specific values
        id1_q1 = result.filter(
            (pl.col("id") == 1) & (pl.col("variable") == "q1")
        )["value"]
        assert id1_q1[0] == 100

    def test_unpivot_multiple_index_columns(self, empty_context):
        """Test unpivot with multiple index columns."""
        df = pl.DataFrame({
            "product": ["A", "B"],
            "region": ["North", "South"],
            "jan": [100, 200],
            "feb": [150, 250]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_multi_index",
            config={
                "on": ["jan", "feb"],
                "index": ["product", "region"]
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 4
        assert "product" in result.columns
        assert "region" in result.columns

    def test_unpivot_single_column(self, empty_context):
        """Test unpivot with a single column."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [100, 200, 300]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_single",
            config={
                "on": ["value"],
                "index": ["id"]
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 3
        assert all(v == "value" for v in result["variable"].to_list())

    def test_unpivot_empty_dataframe(self, empty_context):
        """Test unpivot on empty DataFrame."""
        df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "jan": pl.Series([], dtype=pl.Float64),
            "feb": pl.Series([], dtype=pl.Float64)
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_empty",
            config={
                "on": ["jan", "feb"],
                "index": ["id"]
            }
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 0

    def test_unpivot_with_nulls(self, empty_context):
        """Test unpivot handles null values correctly."""
        df = pl.DataFrame({
            "id": [1, 2],
            "jan": [100, None],
            "feb": [None, 200]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_nulls",
            config={
                "on": ["jan", "feb"],
                "index": ["id"]
            }
        )
        result = transformer.transform(df, empty_context)
        
        # Nulls should be preserved
        assert len(result) == 4
        null_count = result["value"].null_count()
        assert null_count == 2

    def test_validate_config_missing_on(self):
        """Test validation fails when 'on' is missing."""
        transformer = UnpivotTransformer(
            name="test",
            config={"index": ["id"]}
        )
        error = transformer.validate_config({"index": ["id"]})
        
        assert error is not None
        assert "on" in error.lower()

    def test_validate_config_on_not_list(self):
        """Test validation fails when 'on' is not a list."""
        transformer = UnpivotTransformer(
            name="test",
            config={"on": "jan"}
        )
        error = transformer.validate_config({"on": "jan"})
        
        assert error is not None
        assert "list" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"on": ["jan", "feb"]}
        transformer = UnpivotTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = UnpivotTransformer(
            name="test",
            config={"on": ["jan", "feb"]}
        )
        assert transformer.transformer_type == "unpivot"

    def test_default_variable_and_value_names(self, empty_context):
        """Test default variable_name and value_name."""
        df = pl.DataFrame({
            "id": [1],
            "a": [10],
            "b": [20]
        })
        
        transformer = UnpivotTransformer(
            name="unpivot_defaults",
            config={"on": ["a", "b"], "index": ["id"]}
        )
        result = transformer.transform(df, empty_context)
        
        # Default names should be 'variable' and 'value'
        assert "variable" in result.columns
        assert "value" in result.columns
