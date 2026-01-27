"""Tests for DropTransformer."""

import pytest
import polars as pl

from frameworks.data_transformation.transformers.column.drop import DropTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class TestDropTransformer:
    """Tests for DropTransformer."""
    
    def test_drop_single_column(self, sample_customers_df, empty_context):
        """Test dropping a single column."""
        transformer = DropTransformer(
            name="drop_email",
            config={"columns": ["email"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "email" not in result.columns
        assert len(result.columns) == len(sample_customers_df.columns) - 1
        assert len(result) == len(sample_customers_df)
    
    def test_drop_multiple_columns(self, sample_customers_df, empty_context):
        """Test dropping multiple columns."""
        transformer = DropTransformer(
            name="drop_cols",
            config={"columns": ["email", "signup_date", "status"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "email" not in result.columns
        assert "signup_date" not in result.columns
        assert "status" not in result.columns
        assert len(result.columns) == len(sample_customers_df.columns) - 3
    
    def test_drop_preserves_other_columns(self, sample_customers_df, empty_context):
        """Test that non-dropped columns are preserved."""
        transformer = DropTransformer(
            name="drop_some",
            config={"columns": ["email"]}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "customer_id" in result.columns
        assert "name" in result.columns
        assert "age" in result.columns
    
    def test_drop_nonexistent_column_raises_error(
        self, sample_customers_df, empty_context
    ):
        """Test that dropping non-existent column raises error."""
        transformer = DropTransformer(
            name="drop_missing",
            config={"columns": ["nonexistent"]}
        )
        
        with pytest.raises(Exception):  # Polars will raise
            transformer.transform(sample_customers_df, empty_context)
    
    def test_validate_config_missing_columns(self):
        """Test config validation catches missing columns."""
        transformer = DropTransformer(name="test", config={})
        
        error = transformer.validate_config({})
        
        assert error is not None
        assert "columns" in error.lower()
    
    def test_validate_config_columns_not_list(self):
        """Test config validation catches non-list columns."""
        transformer = DropTransformer(name="test", config={"columns": "email"})
        
        error = transformer.validate_config({"columns": "email"})
        
        assert error is not None
        assert "list" in error.lower()
    
    def test_validate_config_valid(self):
        """Test config validation passes with valid config."""
        config = {"columns": ["email", "status"]}
        transformer = DropTransformer(name="test", config=config)
        
        error = transformer.validate_config(config)
        
        assert error is None
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = DropTransformer(name="test", config={"columns": ["a"]})
        
        assert transformer.transformer_type == "drop"
