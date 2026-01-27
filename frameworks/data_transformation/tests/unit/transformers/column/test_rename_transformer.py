"""Tests for RenameTransformer."""

import pytest
import polars as pl

from frameworks.data_transformation.transformers.column.rename import RenameTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class TestRenameTransformer:
    """Tests for RenameTransformer."""
    
    def test_rename_single_column(self, sample_customers_df, empty_context):
        """Test renaming a single column."""
        transformer = RenameTransformer(
            name="rename_id",
            config={"mapping": {"customer_id": "id"}}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "id" in result.columns
        assert "customer_id" not in result.columns
        assert len(result.columns) == len(sample_customers_df.columns)
    
    def test_rename_multiple_columns(self, sample_customers_df, empty_context):
        """Test renaming multiple columns."""
        transformer = RenameTransformer(
            name="rename_cols",
            config={"mapping": {
                "customer_id": "id",
                "name": "customer_name",
                "email": "contact_email"
            }}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "id" in result.columns
        assert "customer_name" in result.columns
        assert "contact_email" in result.columns
        assert "customer_id" not in result.columns
        assert "name" not in result.columns
        assert "email" not in result.columns
    
    def test_rename_preserves_data(self, sample_customers_df, empty_context):
        """Test that renaming preserves data values."""
        transformer = RenameTransformer(
            name="rename",
            config={"mapping": {"name": "customer_name"}}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result["customer_name"].to_list() == sample_customers_df["name"].to_list()
    
    def test_rename_preserves_column_order(self, sample_customers_df, empty_context):
        """Test that renaming preserves column order."""
        original_columns = sample_customers_df.columns
        transformer = RenameTransformer(
            name="rename",
            config={"mapping": {"name": "customer_name"}}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        # Column order should be same, with renamed column in same position
        expected_columns = [
            "customer_name" if c == "name" else c 
            for c in original_columns
        ]
        assert result.columns == expected_columns
    
    def test_rename_nonexistent_column_raises_error(
        self, sample_customers_df, empty_context
    ):
        """Test that renaming non-existent column raises error."""
        transformer = RenameTransformer(
            name="rename_missing",
            config={"mapping": {"nonexistent": "new_name"}}
        )
        
        with pytest.raises(Exception):  # Polars will raise
            transformer.transform(sample_customers_df, empty_context)
    
    def test_validate_config_missing_mapping(self):
        """Test config validation catches missing mapping."""
        transformer = RenameTransformer(name="test", config={})
        
        error = transformer.validate_config({})
        
        assert error is not None
        assert "mapping" in error.lower()
    
    def test_validate_config_mapping_not_dict(self):
        """Test config validation catches non-dict mapping."""
        transformer = RenameTransformer(
            name="test", 
            config={"mapping": ["a", "b"]}
        )
        
        error = transformer.validate_config({"mapping": ["a", "b"]})
        
        assert error is not None
        assert "dictionary" in error.lower()
    
    def test_validate_config_valid(self):
        """Test config validation passes with valid config."""
        config = {"mapping": {"old": "new"}}
        transformer = RenameTransformer(name="test", config=config)
        
        error = transformer.validate_config(config)
        
        assert error is None
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = RenameTransformer(
            name="test", 
            config={"mapping": {"a": "b"}}
        )
        
        assert transformer.transformer_type == "rename"
    
    def test_rename_empty_mapping(self, sample_customers_df, empty_context):
        """Test renaming with empty mapping (no-op)."""
        transformer = RenameTransformer(
            name="no_op",
            config={"mapping": {}}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert result.columns == sample_customers_df.columns
