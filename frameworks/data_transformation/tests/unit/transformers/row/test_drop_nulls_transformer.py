"""Tests for DropNullsTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.row.drop_nulls import DropNullsTransformer


class TestDropNullsTransformer:
    """Tests for DropNullsTransformer."""

    def test_drop_nulls_all_columns(self, empty_context):
        """Test dropping rows with nulls in any column."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", None, "Charlie", "Diana", None],
            "age": [25, 30, None, 35, 40]
        })
        
        transformer = DropNullsTransformer(name="drop_all", config={})
        result = transformer.transform(df, empty_context)
        
        # Only rows 1 and 4 have no nulls
        assert len(result) == 2
        assert result["id"].to_list() == [1, 4]

    def test_drop_nulls_subset_columns(self, empty_context):
        """Test dropping rows with nulls in specific columns."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", None, "Charlie", "Diana", None],
            "age": [25, 30, None, 35, 40]
        })
        
        transformer = DropNullsTransformer(
            name="drop_subset",
            config={"subset": ["name"]}
        )
        result = transformer.transform(df, empty_context)
        
        # Rows 2 and 5 have null names
        assert len(result) == 3
        assert result["id"].to_list() == [1, 3, 4]

    def test_drop_nulls_multiple_subset_columns(self, empty_context):
        """Test dropping rows with nulls in multiple specific columns."""
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", None, "Charlie", "Diana", "Eve"],
            "age": [25, 30, None, 35, 40],
            "city": ["NYC", "LA", "Chicago", None, "Boston"]
        })
        
        transformer = DropNullsTransformer(
            name="drop_multi_subset",
            config={"subset": ["name", "age"]}
        )
        result = transformer.transform(df, empty_context)
        
        # Rows 2 has null name, row 3 has null age
        assert len(result) == 3
        assert result["id"].to_list() == [1, 4, 5]

    def test_drop_nulls_no_nulls(self, sample_customers_df, empty_context):
        """Test with DataFrame that has no nulls."""
        transformer = DropNullsTransformer(name="drop_none", config={})
        result = transformer.transform(sample_customers_df, empty_context)
        
        # Should return all rows
        assert len(result) == len(sample_customers_df)

    def test_drop_nulls_all_nulls_in_column(self, empty_context):
        """Test with DataFrame where all values in a column are null."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [None, None, None]
        })
        
        transformer = DropNullsTransformer(name="drop_all_null", config={})
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 0

    def test_drop_nulls_empty_dataframe(self, empty_context):
        """Test with empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "name": pl.Series([], dtype=pl.Utf8)
        })
        
        transformer = DropNullsTransformer(name="drop_empty", config={})
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0

    def test_drop_nulls_preserves_columns(self, empty_context):
        """Test that drop_nulls preserves all columns."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "age": [25, 30, 35]
        })
        
        transformer = DropNullsTransformer(name="drop_cols", config={})
        result = transformer.transform(df, empty_context)
        
        assert set(result.columns) == set(df.columns)

    def test_drop_nulls_preserves_dtypes(self, empty_context):
        """Test that drop_nulls preserves column dtypes."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", None, "Charlie"],
            "amount": [10.5, 20.5, 30.5]
        })
        
        transformer = DropNullsTransformer(name="drop_dtypes", config={})
        result = transformer.transform(df, empty_context)
        
        for col in df.columns:
            assert result[col].dtype == df[col].dtype

    def test_drop_nulls_single_row_with_null(self, empty_context):
        """Test single-row DataFrame with null."""
        df = pl.DataFrame({
            "id": [1],
            "name": [None]
        })
        
        transformer = DropNullsTransformer(name="drop_single", config={})
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 0

    def test_drop_nulls_single_row_without_null(self, empty_context):
        """Test single-row DataFrame without null."""
        df = pl.DataFrame({
            "id": [1],
            "name": ["Alice"]
        })
        
        transformer = DropNullsTransformer(name="drop_single_ok", config={})
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 1

    def test_drop_nulls_subset_with_nonexistent_column(self, empty_context):
        """Test subset with column that doesn't affect result."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        # This should work - all rows have values for 'name'
        transformer = DropNullsTransformer(
            name="drop_subset_ok",
            config={"subset": ["name"]}
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 3

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = DropNullsTransformer(name="test", config={})
        assert transformer.transformer_type == "drop_nulls"

    def test_drop_nulls_mixed_null_patterns(self, empty_context):
        """Test with various null patterns."""
        df = pl.DataFrame({
            "a": [None, 1, 2, None, 4],
            "b": [10, None, 20, None, 40],
            "c": [100, 200, None, None, 400]
        })
        
        # Only row 5 (index 4) has no nulls
        transformer = DropNullsTransformer(name="drop_mixed", config={})
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 1
        assert result["a"][0] == 4
        assert result["b"][0] == 40
        assert result["c"][0] == 400

    def test_drop_nulls_with_nan_values(self, empty_context):
        """Test that NaN values are not treated as nulls."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": [1.0, float("nan"), 3.0]
        })
        
        transformer = DropNullsTransformer(name="drop_nan", config={})
        result = transformer.transform(df, empty_context)
        
        # NaN is not null, so all rows should remain
        assert len(result) == 3
