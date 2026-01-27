"""Tests for UnionTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.combine.union import UnionTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import TransformationError


class TestUnionTransformer:
    """Tests for UnionTransformer."""

    def test_basic_union(self, sample_customers_df):
        """Test basic union operation."""
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
        
        transformer = UnionTransformer(
            name="union_basic",
            config={"dataset": "additional"}
        )
        result = transformer.transform(sample_customers_df, context)
        
        # All rows should be unique after union
        assert len(result) == len(sample_customers_df) + len(additional_df)

    def test_union_removes_duplicates(self, sample_customers_df):
        """Test that union removes duplicate rows."""
        # Create dataset with some duplicate rows
        duplicate_df = sample_customers_df.head(2)
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"duplicates": duplicate_df}
        )
        
        transformer = UnionTransformer(
            name="union_dedup",
            config={"dataset": "duplicates"}
        )
        result = transformer.transform(sample_customers_df, context)
        
        # Should have same number of rows as original (duplicates removed)
        assert len(result) == len(sample_customers_df)

    def test_union_partial_overlap(self, sample_customers_df):
        """Test union with partial overlap."""
        # Create dataset with one duplicate and one new row
        partial_df = pl.DataFrame({
            "customer_id": [1, 8],  # customer_id 1 exists, 8 is new
            "name": ["Alice", "Henry"],  # Alice exists
            "email": ["alice@test.com", "henry@test.com"],
            "status": ["active", "active"],
            "age": [25, 29],
            "signup_date": ["2023-01-15", "2023-07-01"]
        })
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"partial": partial_df}
        )
        
        transformer = UnionTransformer(
            name="union_partial",
            config={"dataset": "partial"}
        )
        result = transformer.transform(sample_customers_df, context)
        
        # Should have original + 1 new row (duplicate removed)
        assert len(result) == len(sample_customers_df) + 1

    def test_union_empty_dataset(self, sample_customers_df):
        """Test union with empty dataset."""
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
        
        transformer = UnionTransformer(
            name="union_empty",
            config={"dataset": "empty"}
        )
        result = transformer.transform(sample_customers_df, context)
        
        assert len(result) == len(sample_customers_df)

    def test_union_with_empty_source(self):
        """Test union when source DataFrame is empty."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "value": pl.Series([], dtype=pl.Utf8)
        })
        other_df = pl.DataFrame({
            "id": [1, 2],
            "value": ["a", "b"]
        })
        
        context = TransformationContext(
            data=empty_df,
            datasets={"other": other_df}
        )
        
        transformer = UnionTransformer(
            name="union_empty_source",
            config={"dataset": "other"}
        )
        result = transformer.transform(empty_df, context)
        
        assert len(result) == 2

    def test_union_missing_dataset(self, sample_customers_df):
        """Test error when dataset not found in context."""
        context = TransformationContext(data=sample_customers_df)
        
        transformer = UnionTransformer(
            name="union_missing",
            config={"dataset": "nonexistent"}
        )
        
        with pytest.raises(TransformationError) as exc_info:
            transformer.transform(sample_customers_df, context)
        assert "nonexistent" in str(exc_info.value)

    def test_union_all_duplicates(self):
        """Test union where all rows are duplicates."""
        df1 = pl.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"]
        })
        df2 = pl.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"]
        })
        
        context = TransformationContext(
            data=df1,
            datasets={"same": df2}
        )
        
        transformer = UnionTransformer(
            name="union_all_dup",
            config={"dataset": "same"}
        )
        result = transformer.transform(df1, context)
        
        # All duplicates removed
        assert len(result) == 3

    def test_get_required_datasets(self):
        """Test get_required_datasets method."""
        transformer = UnionTransformer(
            name="test",
            config={"dataset": "other_df"}
        )
        
        assert transformer.get_required_datasets() == ["other_df"]

    def test_get_required_datasets_empty(self):
        """Test get_required_datasets when not configured."""
        transformer = UnionTransformer(name="test", config={})
        
        assert transformer.get_required_datasets() == []

    def test_validate_config_missing_dataset(self):
        """Test validation fails when dataset is missing."""
        transformer = UnionTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None
        assert "dataset" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"dataset": "other_df"}
        transformer = UnionTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = UnionTransformer(
            name="test",
            config={"dataset": "other"}
        )
        assert transformer.transformer_type == "union"

    def test_input_type(self):
        """Test input_type property returns 'multi'."""
        transformer = UnionTransformer(
            name="test",
            config={"dataset": "other"}
        )
        assert transformer.input_type == "multi"
