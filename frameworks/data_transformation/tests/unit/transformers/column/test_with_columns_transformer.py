"""Tests for WithColumnsTransformer."""

import polars as pl
import pytest

from frameworks.data_transformation.transformers.column.with_columns import WithColumnsTransformer


class TestWithColumnsTransformer:
    """Tests for WithColumnsTransformer."""

    def test_add_literal_string_column(self, sample_customers_df, empty_context):
        """Test adding a column with a literal string value using lit()."""
        transformer = WithColumnsTransformer(
            name="add_tier",
            config={"columns": {"tier": "lit('standard')"}}  # Use lit() for string literals
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "tier" in result.columns
        assert result["tier"].to_list() == ["standard"] * len(result)

    def test_add_literal_integer_column(self, sample_customers_df, empty_context):
        """Test adding a column with a literal integer value."""
        transformer = WithColumnsTransformer(
            name="add_score",
            config={"columns": {"score": 100}}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "score" in result.columns
        assert result["score"].to_list() == [100] * len(result)

    def test_add_column_from_expression(self, sample_customers_df, empty_context):
        """Test adding a column using a Polars expression."""
        transformer = WithColumnsTransformer(
            name="add_name_upper",
            config={"columns": {"name_upper": "col('name').str.to_uppercase()"}}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "name_upper" in result.columns
        # Verify the values are uppercase
        for i, name in enumerate(sample_customers_df["name"].to_list()):
            assert result["name_upper"][i] == name.upper()

    def test_add_computed_column(self, sample_orders_df, empty_context):
        """Test adding a computed column based on other columns."""
        transformer = WithColumnsTransformer(
            name="add_doubled_amount",
            config={"columns": {"doubled_amount": "col('amount') * 2"}}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert "doubled_amount" in result.columns
        for i, amount in enumerate(sample_orders_df["amount"].to_list()):
            assert result["doubled_amount"][i] == amount * 2

    def test_add_multiple_columns(self, sample_customers_df, empty_context):
        """Test adding multiple columns at once."""
        transformer = WithColumnsTransformer(
            name="add_multiple",
            config={
                "columns": {
                    "tier": "lit('premium')",  # Use lit() for string literal
                    "priority": 1,  # literal int (non-string, handled as literal)
                    "name_length": "col('name').str.len_chars()"  # expression
                }
            }
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "tier" in result.columns
        assert "priority" in result.columns
        assert "name_length" in result.columns

    def test_modify_existing_column(self, sample_customers_df, empty_context):
        """Test modifying an existing column."""
        transformer = WithColumnsTransformer(
            name="modify_name",
            config={"columns": {"name": "col('name').str.to_uppercase()"}}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        # Same number of columns
        assert len(result.columns) == len(sample_customers_df.columns)
        # Name column should now be uppercase
        for i, name in enumerate(sample_customers_df["name"].to_list()):
            assert result["name"][i] == name.upper()

    def test_add_boolean_column(self, sample_orders_df, empty_context):
        """Test adding a column with boolean literal."""
        transformer = WithColumnsTransformer(
            name="add_flag",
            config={"columns": {"processed": True}}
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert "processed" in result.columns
        assert all(result["processed"].to_list())

    def test_add_column_with_conditional_expression(self, sample_orders_df, empty_context):
        """Test adding a column with a conditional expression."""
        transformer = WithColumnsTransformer(
            name="add_category",
            config={
                "columns": {
                    "is_large_order": "col('amount') > 200"
                }
            }
        )
        result = transformer.transform(sample_orders_df, empty_context)
        
        assert "is_large_order" in result.columns
        # Check values are correctly computed
        for i in range(len(result)):
            expected = sample_orders_df["amount"][i] > 200
            assert result["is_large_order"][i] == expected

    def test_empty_dataframe(self, empty_context):
        """Test with empty DataFrame."""
        empty_df = pl.DataFrame({
            "id": pl.Series([], dtype=pl.Int64),
            "name": pl.Series([], dtype=pl.Utf8)
        })
        
        transformer = WithColumnsTransformer(
            name="add_column",
            config={"columns": {"status": "lit('active')"}}  # Use lit() for string literal
        )
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0
        assert "status" in result.columns

    def test_validate_config_missing_columns(self):
        """Test validation fails when columns is missing."""
        transformer = WithColumnsTransformer(name="test", config={})
        error = transformer.validate_config({})
        
        assert error is not None
        assert "columns" in error.lower()

    def test_validate_config_columns_not_dict(self):
        """Test validation fails when columns is not a dictionary."""
        transformer = WithColumnsTransformer(name="test", config={"columns": ["a", "b"]})
        error = transformer.validate_config({"columns": ["a", "b"]})
        
        assert error is not None
        assert "dictionary" in error.lower()

    def test_validate_config_valid(self):
        """Test validation passes with valid config."""
        config = {"columns": {"new_col": "'value'"}}
        transformer = WithColumnsTransformer(name="test", config=config)
        error = transformer.validate_config(config)
        
        assert error is None

    def test_transformer_type(self):
        """Test transformer_type property returns correct value."""
        transformer = WithColumnsTransformer(
            name="test",
            config={"columns": {"col": "'value'"}}
        )
        assert transformer.transformer_type == "with_columns"

    def test_add_null_literal(self, sample_customers_df, empty_context):
        """Test adding a column with null literal."""
        transformer = WithColumnsTransformer(
            name="add_null",
            config={"columns": {"nullable_field": None}}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "nullable_field" in result.columns
        assert all(v is None for v in result["nullable_field"].to_list())

    def test_add_float_literal(self, sample_customers_df, empty_context):
        """Test adding a column with float literal."""
        transformer = WithColumnsTransformer(
            name="add_float",
            config={"columns": {"rate": 0.15}}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "rate" in result.columns
        assert all(v == 0.15 for v in result["rate"].to_list())

    def test_column_with_polars_expr_object(self, sample_customers_df, empty_context):
        """Test adding a column using a Polars expression object directly."""
        # Create transformer with a string expression (normal use case)
        transformer = WithColumnsTransformer(
            name="add_expr",
            config={"columns": {"customer_id_doubled": "col('customer_id') * 2"}}
        )
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert "customer_id_doubled" in result.columns
        for i, id_val in enumerate(sample_customers_df["customer_id"].to_list()):
            assert result["customer_id_doubled"][i] == id_val * 2
