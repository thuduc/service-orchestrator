"""Tests for UniqueTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.row.unique import UniqueTransformer


class TestUniqueTransformer:
    """Tests for UniqueTransformer."""

    def test_unique_all_columns(self, empty_context):
        """Test removing duplicates based on all columns."""
        df = pl.DataFrame({
            "id": [1, 2, 2, 3, 3, 3],
            "name": ["Alice", "Bob", "Bob", "Charlie", "Charlie", "Charlie"]
        })
        
        transformer = UniqueTransformer(name="unique_all", config={})
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 3
        assert set(result["id"].to_list()) == {1, 2, 3}

    def test_unique_subset_columns(self, empty_context):
        """Test removing duplicates based on subset of columns."""
        df = pl.DataFrame({
            "customer_id": [1, 1, 2, 2, 3],
            "order_id": [100, 101, 200, 201, 300],
            "amount": [10.0, 20.0, 30.0, 40.0, 50.0]
        })
        
        transformer = UniqueTransformer(
            name="unique_subset",
            config={"subset": ["customer_id"]}
        )
        result = transformer.transform(df, empty_context)
        
        # Should have one row per unique customer_id
        assert len(result) == 3
        assert set(result["customer_id"].to_list()) == {1, 2, 3}

    def test_unique_keep_first(self, empty_context):
        """Test keeping the first occurrence of duplicates."""
        df = pl.DataFrame({
            "id": [1, 1, 1],
            "value": ["first", "second", "third"]
        })
        
        transformer = UniqueTransformer(
            name="unique_first",
            config={"subset": ["id"], "keep": "first", "maintain_order": True}
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 1
        assert result["value"][0] == "first"

    def test_unique_keep_last(self, empty_context):
        """Test keeping the last occurrence of duplicates."""
        df = pl.DataFrame({
            "id": [1, 1, 1],
            "value": ["first", "second", "third"]
        })
        
        transformer = UniqueTransformer(
            name="unique_last",
            config={"subset": ["id"], "keep": "last", "maintain_order": True}
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 1
        assert result["value"][0] == "third"

    def test_unique_keep_none(self, empty_context):
        """Test removing all duplicates (keep='none')."""
        df = pl.DataFrame({
            "id": [1, 2, 2, 3, 3, 4],
            "value": ["a", "b", "c", "d", "e", "f"]
        })
        
        transformer = UniqueTransformer(
            name="unique_none",
            config={"subset": ["id"], "keep": "none"}
        )
        result = transformer.transform(df, empty_context)
        
        # Only non-duplicated rows should remain (id=1 and id=4)
        assert len(result) == 2
        assert set(result["id"].to_list()) == {1, 4}

    def test_unique_maintain_order_true(self, empty_context):
        """Test that maintain_order=True preserves original order."""
        df = pl.DataFrame({
            "id": [3, 1, 2, 1, 3],
            "value": ["a", "b", "c", "d", "e"]
        })
        
        transformer = UniqueTransformer(
            name="unique_ordered",
            config={"subset": ["id"], "keep": "first", "maintain_order": True}
        )
        result = transformer.transform(df, empty_context)
        
        # Order should be maintained: 3, 1, 2
        assert result["id"].to_list() == [3, 1, 2]

    def test_unique_no_duplicates(self, sample_customers_df, empty_context):
        """Test with DataFrame that has no duplicates."""
        transformer = UniqueTransformer(name="unique_none_found", config={})
        result = transformer.transform(sample_customers_df, empty_context)
        
        # Should have same number of rows
        assert len(result) == len(sample_customers_df)

    def test_unique_empty_dataframe(self, empty_context):
        """Test with empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "name": pl.Series([], dtype=pl.Utf8)
        })
        
        transformer = UniqueTransformer(name="unique_empty", config={})
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0

    def test_unique_single_row(self, empty_context):
        """Test with single-row DataFrame."""
        single_df = pl.DataFrame({
            "id": [1],
            "name": ["Alice"]
        })
        
        transformer = UniqueTransformer(name="unique_single", config={})
        result = transformer.transform(single_df, empty_context)
        
        assert len(result) == 1

    def test_unique_all_same_values(self, empty_context):
        """Test with DataFrame where all rows are identical."""
        df = pl.DataFrame({
            "id": [1, 1, 1],
            "name": ["same", "same", "same"]
        })
        
        transformer = UniqueTransformer(name="unique_all_same", config={})
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 1

    def test_unique_multiple_subset_columns(self, empty_context):
        """Test uniqueness based on multiple columns."""
        df = pl.DataFrame({
            "category": ["A", "A", "A", "B", "B"],
            "subcategory": ["X", "X", "Y", "X", "X"],
            "value": [1, 2, 3, 4, 5]
        })
        
        transformer = UniqueTransformer(
            name="unique_multi_subset",
            config={"subset": ["category", "subcategory"], "keep": "first"}
        )
        result = transformer.transform(df, empty_context)
        
        # Unique combinations: (A, X), (A, Y), (B, X)
        assert len(result) == 3

    def test_unique_with_nulls(self, empty_context):
        """Test handling of null values in unique operation."""
        df = pl.DataFrame({
            "id": [1, None, 1, None, 2],
            "name": ["a", "b", "c", "d", "e"]
        })
        
        transformer = UniqueTransformer(
            name="unique_nulls",
            config={"subset": ["id"], "keep": "first"}
        )
        result = transformer.transform(df, empty_context)
        
        # Should have: 1, None, 2 (null treated as a unique value)
        assert len(result) == 3

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = UniqueTransformer(name="test", config={})
        assert transformer.transformer_type == "unique"

    def test_default_values(self, empty_context):
        """Test that default values are applied correctly."""
        df = pl.DataFrame({
            "id": [1, 1, 2],
            "name": ["a", "b", "c"]
        })
        
        # Using all defaults: subset=None, keep='first', maintain_order=True
        transformer = UniqueTransformer(name="unique_defaults", config={})
        result = transformer.transform(df, empty_context)
        
        # All rows are unique when considering all columns
        assert len(result) == 3
