"""Tests for SelectTransformer."""

import pytest
import polars as pl

from frameworks.data_transformation.transformers.column.select import SelectTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class TestSelectTransformer:
    """Tests for SelectTransformer."""
    
    def test_select_single_column(self, sample_customers_df, empty_context):
        """Test selecting a single column."""
        transformer = SelectTransformer(
            name="select_name",
            config={"columns": ["name"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result.columns == ["name"]
        assert len(result) == len(sample_customers_df)
    
    def test_select_multiple_columns(self, sample_customers_df, empty_context):
        """Test selecting multiple columns."""
        transformer = SelectTransformer(
            name="select_cols",
            config={"columns": ["customer_id", "name", "email"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result.columns == ["customer_id", "name", "email"]
        assert len(result) == len(sample_customers_df)
    
    def test_select_reorders_columns(self, sample_customers_df, empty_context):
        """Test that select reorders columns as specified."""
        transformer = SelectTransformer(
            name="select_reorder",
            config={"columns": ["email", "name", "customer_id"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result.columns == ["email", "name", "customer_id"]
    
    def test_select_missing_column_raises_error(
        self, sample_customers_df, empty_context
    ):
        """Test that selecting non-existent column raises error."""
        transformer = SelectTransformer(
            name="select_missing",
            config={"columns": ["nonexistent"]}
        )
        
        with pytest.raises(Exception):  # Polars will raise
            transformer.transform(sample_customers_df, empty_context)
    
    def test_validate_config_missing_columns(self):
        """Test config validation catches missing columns."""
        transformer = SelectTransformer(name="test", config={})
        
        error = transformer.validate_config({})
        
        assert error is not None
        assert "columns" in error.lower()
    
    def test_validate_config_empty_columns(self):
        """Test config validation catches empty columns list."""
        transformer = SelectTransformer(name="test", config={"columns": []})
        
        error = transformer.validate_config({"columns": []})
        
        assert error is not None
        assert "empty" in error.lower()
    
    def test_validate_config_columns_not_list(self):
        """Test config validation catches non-list columns."""
        transformer = SelectTransformer(
            name="test", 
            config={"columns": "name"}
        )
        
        error = transformer.validate_config({"columns": "name"})
        
        assert error is not None
        assert "list" in error.lower()
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = SelectTransformer(
            name="test",
            config={"columns": ["name"]}
        )
        
        assert transformer.transformer_type == "select"
