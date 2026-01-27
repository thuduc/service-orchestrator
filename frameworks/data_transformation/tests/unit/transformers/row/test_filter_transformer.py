"""Tests for FilterTransformer."""

import pytest
import polars as pl

from frameworks.data_transformation.transformers.row.filter import FilterTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class TestFilterTransformer:
    """Tests for FilterTransformer."""
    
    def test_filter_string_equals(self, sample_customers_df, empty_context):
        """Test filtering with string equals condition."""
        transformer = FilterTransformer(
            name="filter_active",
            config={"condition": "col('status') == 'active'"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert len(result) == 3  # Alice, Diana active; Bob, Charlie, Eve inactive -> 3 active
        assert all(s == "active" for s in result["status"].to_list())
    
    def test_filter_numeric_comparison(self, sample_customers_df, empty_context):
        """Test filtering with numeric comparison."""
        transformer = FilterTransformer(
            name="filter_age",
            config={"condition": "col('age') >= 30"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert all(age >= 30 for age in result["age"].to_list())
    
    def test_filter_greater_than(self, sample_customers_df, empty_context):
        """Test filtering with greater than."""
        transformer = FilterTransformer(
            name="filter_age_gt",
            config={"condition": "col('age') > 30"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert all(age > 30 for age in result["age"].to_list())
    
    def test_filter_less_than(self, sample_customers_df, empty_context):
        """Test filtering with less than."""
        transformer = FilterTransformer(
            name="filter_age_lt",
            config={"condition": "col('age') < 30"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert all(age < 30 for age in result["age"].to_list())
    
    def test_filter_not_equals(self, sample_customers_df, empty_context):
        """Test filtering with not equals."""
        transformer = FilterTransformer(
            name="filter_not_inactive",
            config={"condition": "col('status') != 'inactive'"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert all(status != "inactive" for status in result["status"].to_list())
    
    def test_filter_combined_and_conditions(self, sample_customers_df, empty_context):
        """Test filtering with combined AND conditions."""
        transformer = FilterTransformer(
            name="filter_combined",
            config={"condition": "(col('status') == 'active') & (col('age') > 25)"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        for row in result.iter_rows(named=True):
            assert row["status"] == "active"
            assert row["age"] > 25
    
    def test_filter_combined_or_conditions(self, sample_customers_df, empty_context):
        """Test filtering with combined OR conditions."""
        transformer = FilterTransformer(
            name="filter_or",
            config={"condition": "(col('age') < 26) | (col('age') > 40)"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        for row in result.iter_rows(named=True):
            assert row["age"] < 26 or row["age"] > 40
    
    def test_filter_is_not_null(self, df_with_nulls, empty_context):
        """Test filtering with is_not_null."""
        # Update context with the null-containing df
        context = TransformationContext(data=df_with_nulls)
        
        transformer = FilterTransformer(
            name="filter_not_null",
            config={"condition": "col('name').is_not_null()"}
        )
        
        result = transformer.transform(df_with_nulls, context)
        
        assert len(result) == 3  # Alice, Charlie, Diana (not null names)
        assert not result["name"].is_null().any()
    
    def test_filter_is_null(self, df_with_nulls, empty_context):
        """Test filtering with is_null."""
        context = TransformationContext(data=df_with_nulls)
        
        transformer = FilterTransformer(
            name="filter_null",
            config={"condition": "col('name').is_null()"}
        )
        
        result = transformer.transform(df_with_nulls, context)
        
        assert len(result) == 2  # 2 null names
        assert result["name"].is_null().all()
    
    def test_filter_with_polars_expression(self, sample_customers_df, empty_context):
        """Test filtering with Polars expression object."""
        transformer = FilterTransformer(
            name="filter_polars_expr",
            config={"condition": pl.col("age") > 30}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert all(age > 30 for age in result["age"].to_list())
    
    def test_filter_returns_empty_when_no_matches(
        self, sample_customers_df, empty_context
    ):
        """Test filter returns empty DataFrame when no rows match."""
        transformer = FilterTransformer(
            name="filter_none",
            config={"condition": "col('age') > 100"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert len(result) == 0
        assert result.columns == sample_customers_df.columns
    
    def test_filter_returns_all_when_all_match(
        self, sample_customers_df, empty_context
    ):
        """Test filter returns all rows when all match."""
        transformer = FilterTransformer(
            name="filter_all",
            config={"condition": "col('age') > 0"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert len(result) == len(sample_customers_df)
    
    def test_filter_on_empty_dataframe(self, empty_context):
        """Test filtering an empty DataFrame."""
        empty_df = pl.DataFrame({
            "name": [],
            "age": [],
        }).cast({"name": pl.Utf8, "age": pl.Int64})
        
        transformer = FilterTransformer(
            name="filter_empty",
            config={"condition": "col('age') > 0"}
        )
        
        result = transformer.transform(empty_df, empty_context)
        
        assert len(result) == 0
    
    def test_validate_config_missing_condition(self):
        """Test config validation catches missing condition."""
        transformer = FilterTransformer(name="test", config={})
        
        error = transformer.validate_config({})
        
        assert error is not None
        assert "condition" in error.lower()
    
    def test_validate_config_with_condition(self):
        """Test config validation passes with condition."""
        transformer = FilterTransformer(
            name="test",
            config={"condition": "col('age') > 0"}
        )
        
        error = transformer.validate_config({"condition": "col('age') > 0"})
        
        assert error is None
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = FilterTransformer(
            name="test",
            config={"condition": "col('age') > 0"}
        )
        
        assert transformer.transformer_type == "filter"
    
    def test_filter_string_contains(self, sample_customers_df, empty_context):
        """Test filtering with string contains."""
        transformer = FilterTransformer(
            name="filter_email",
            config={"condition": "col('email').str.contains('alice')"}
        )
        
        result = transformer.transform(sample_customers_df, empty_context)
        
        assert len(result) == 1
        assert result["name"][0] == "Alice"


class TestFilterTransformerOrders:
    """Tests for FilterTransformer using orders data."""
    
    def test_filter_orders_by_status(self, sample_orders_df, empty_context):
        """Test filtering orders by status."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = FilterTransformer(
            name="filter_completed",
            config={"condition": "col('status') == 'completed'"}
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert all(status == "completed" for status in result["status"].to_list())
    
    def test_filter_orders_by_amount(self, sample_orders_df, empty_context):
        """Test filtering orders by amount."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = FilterTransformer(
            name="filter_high_value",
            config={"condition": "col('amount') >= 150"}
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert all(amount >= 150 for amount in result["amount"].to_list())
    
    def test_filter_orders_complex_condition(self, sample_orders_df, empty_context):
        """Test filtering orders with complex condition."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = FilterTransformer(
            name="filter_complex",
            config={
                "condition": "(col('status') == 'completed') & (col('amount') > 100)"
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        for row in result.iter_rows(named=True):
            assert row["status"] == "completed"
            assert row["amount"] > 100
