"""Tests for ExplodeTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.reshape.explode import ExplodeTransformer


class TestExplodeTransformer:
    """Tests for ExplodeTransformer."""

    def test_explode_single_list_column(self, df_with_lists, empty_context):
        """Test exploding a single list column."""
        transformer = ExplodeTransformer(
            name="explode_tags",
            config={"columns": "tags"}
        )
        result = transformer.transform(df_with_lists, empty_context)
        
        # Original has 3 rows with lists of length 2, 2, 3 = 7 total rows
        assert len(result) == 7
        assert "tags" in result.columns
        # Tags should now be individual values, not lists
        assert result["tags"].dtype == pl.Utf8

    def test_explode_list_column_as_list(self, df_with_lists, empty_context):
        """Test exploding with column specified as list."""
        transformer = ExplodeTransformer(
            name="explode_list",
            config={"columns": ["tags"]}
        )
        result = transformer.transform(df_with_lists, empty_context)
        
        assert len(result) == 7

    def test_explode_preserves_other_columns(self, df_with_lists, empty_context):
        """Test that explode preserves other columns."""
        transformer = ExplodeTransformer(
            name="explode_preserve",
            config={"columns": "tags"}
        )
        result = transformer.transform(df_with_lists, empty_context)
        
        assert "id" in result.columns
        assert "name" in result.columns
        
        # Check Alice's rows
        alice_rows = result.filter(pl.col("name") == "Alice")
        assert len(alice_rows) == 2  # Alice has 2 tags
        assert set(alice_rows["tags"].to_list()) == {"python", "data"}

    def test_explode_empty_list(self, empty_context):
        """Test exploding with empty list values."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "tags": [["a", "b"], [], ["c"]]
        })
        
        transformer = ExplodeTransformer(
            name="explode_empty",
            config={"columns": "tags"}
        )
        result = transformer.transform(df, empty_context)
        
        # In Polars, empty list explodes to a single row with null value
        # Total: 2 + 1 (null for empty list) + 1 = 4 rows
        assert len(result) == 4
        # id=2 should have a null tag value
        id2_row = result.filter(pl.col("id") == 2)
        assert len(id2_row) == 1
        assert id2_row["tags"][0] is None

    def test_explode_single_element_lists(self, empty_context):
        """Test exploding lists with single elements."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "tags": [["a"], ["b"], ["c"]]
        })
        
        transformer = ExplodeTransformer(
            name="explode_single",
            config={"columns": "tags"}
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 3
        assert result["tags"].to_list() == ["a", "b", "c"]

    def test_explode_empty_dataframe(self, empty_context):
        """Test exploding empty DataFrame."""
        df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "tags": pl.Series([], dtype=pl.List(pl.Utf8))
        })
        
        transformer = ExplodeTransformer(
            name="explode_empty_df",
            config={"columns": "tags"}
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 0

    def test_explode_with_null_lists(self, empty_context):
        """Test exploding with null list values."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "tags": [["a", "b"], None, ["c"]]
        })
        
        transformer = ExplodeTransformer(
            name="explode_null",
            config={"columns": "tags"}
        )
        result = transformer.transform(df, empty_context)
        
        # null list becomes a null value
        assert len(result) == 4  # 2 + 1 (null) + 1 = 4
        assert result["tags"].null_count() == 1

    def test_explode_integer_list(self, empty_context):
        """Test exploding list of integers."""
        df = pl.DataFrame({
            "id": [1, 2],
            "numbers": [[10, 20, 30], [40, 50]]
        })
        
        transformer = ExplodeTransformer(
            name="explode_int",
            config={"columns": "numbers"}
        )
        result = transformer.transform(df, empty_context)
        
        assert len(result) == 5
        assert result["numbers"].dtype == pl.Int64
        assert set(result["numbers"].to_list()) == {10, 20, 30, 40, 50}

    def test_explode_maintains_row_order(self, empty_context):
        """Test that explode maintains row order."""
        df = pl.DataFrame({
            "id": [1, 2],
            "items": [["a", "b", "c"], ["d", "e"]]
        })
        
        transformer = ExplodeTransformer(
            name="explode_order",
            config={"columns": "items"}
        )
        result = transformer.transform(df, empty_context)
        
        # Order should be: a, b, c, d, e
        assert result["items"].to_list() == ["a", "b", "c", "d", "e"]
        assert result["id"].to_list() == [1, 1, 1, 2, 2]

    def test_validate_config_missing_columns(self):
        """Test validation fails when columns is missing."""
        transformer = ExplodeTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None
        assert "columns" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"columns": "tags"}
        transformer = ExplodeTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_validate_config_valid_list(self):
        """Test validation passes with columns as list."""
        config = {"columns": ["tags", "categories"]}
        transformer = ExplodeTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = ExplodeTransformer(name="test", config={"columns": "tags"})
        assert transformer.transformer_type == "explode"
