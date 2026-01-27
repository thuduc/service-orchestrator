"""Tests for GroupByTransformer."""

import pytest
import polars as pl

from frameworks.data_transformation.transformers.aggregate.group_by import GroupByTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError


class TestGroupByTransformerBasic:
    """Basic tests for GroupByTransformer."""
    
    def test_group_by_single_column_count(self, sample_orders_df, empty_context):
        """Test group by single column with count aggregation."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="group_by_customer",
            config={
                "by": "customer_id",
                "aggregations": {
                    "order_count": {"column": "order_id", "agg": "count"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert "customer_id" in result.columns
        assert "order_count" in result.columns
        # Customer 1 has 3 orders
        customer_1_count = result.filter(pl.col("customer_id") == 1)["order_count"][0]
        assert customer_1_count == 3
    
    def test_group_by_sum(self, sample_orders_df, empty_context):
        """Test group by with sum aggregation."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="group_by_sum",
            config={
                "by": "customer_id",
                "aggregations": {
                    "total_amount": {"column": "amount", "agg": "sum"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        # Customer 1 has orders: 100 + 150 + 300 = 550
        customer_1_total = result.filter(pl.col("customer_id") == 1)["total_amount"][0]
        assert customer_1_total == 550.0
    
    def test_group_by_mean(self, sample_orders_df, empty_context):
        """Test group by with mean aggregation."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="group_by_mean",
            config={
                "by": "customer_id",
                "aggregations": {
                    "avg_amount": {"column": "amount", "agg": "mean"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        # Customer 1: (100 + 150 + 300) / 3 = 183.33...
        customer_1_avg = result.filter(pl.col("customer_id") == 1)["avg_amount"][0]
        assert abs(customer_1_avg - 183.33) < 0.5
    
    def test_group_by_min_max(self, sample_orders_df, empty_context):
        """Test group by with min and max aggregations."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="group_by_minmax",
            config={
                "by": "customer_id",
                "aggregations": {
                    "min_amount": {"column": "amount", "agg": "min"},
                    "max_amount": {"column": "amount", "agg": "max"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        customer_1 = result.filter(pl.col("customer_id") == 1)
        assert customer_1["min_amount"][0] == 100.0
        assert customer_1["max_amount"][0] == 300.0


class TestGroupByTransformerMultipleColumns:
    """Tests for grouping by multiple columns."""
    
    def test_group_by_multiple_columns(self, sample_orders_df, empty_context):
        """Test group by multiple columns."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="group_by_multi",
            config={
                "by": ["customer_id", "status"],
                "aggregations": {
                    "count": {"column": "order_id", "agg": "count"},
                    "total": {"column": "amount", "agg": "sum"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert "customer_id" in result.columns
        assert "status" in result.columns
        assert "count" in result.columns
        assert "total" in result.columns
    
    def test_group_by_string_column_converts_to_list(
        self, sample_orders_df, empty_context
    ):
        """Test that string 'by' is converted to list."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="group_by_string",
            config={
                "by": "customer_id",  # String, not list
                "aggregations": {
                    "count": {"column": "order_id", "agg": "count"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        # Should work without error
        assert "customer_id" in result.columns


class TestGroupByTransformerAggregationSpecs:
    """Tests for different aggregation specifications."""
    
    def test_aggregation_dict_spec(self, sample_orders_df, empty_context):
        """Test aggregation with dict specification."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="dict_spec",
            config={
                "by": "customer_id",
                "aggregations": {
                    "total": {"column": "amount", "agg": "sum"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert "total" in result.columns
    
    def test_aggregation_shorthand_spec(self, sample_orders_df, empty_context):
        """Test aggregation with shorthand specification."""
        context = TransformationContext(data=sample_orders_df)
        
        # Create a DataFrame where the output column name matches the aggregation
        df = pl.DataFrame({
            "category": ["A", "A", "B", "B"],
            "count": [1, 2, 3, 4],  # Column name is 'count'
        })
        context = TransformationContext(data=df)
        
        transformer = GroupByTransformer(
            name="shorthand_spec",
            config={
                "by": "category",
                "aggregations": {
                    "count": "sum",  # Shorthand: applies sum to column named "count"
                }
            }
        )
        
        result = transformer.transform(df, context)
        
        assert "count" in result.columns
        # A: 1+2=3, B: 3+4=7
        a_sum = result.filter(pl.col("category") == "A")["count"][0]
        assert a_sum == 3
    
    def test_aggregation_expression_spec(self, sample_orders_df, empty_context):
        """Test aggregation with expression string specification."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="expr_spec",
            config={
                "by": "customer_id",
                "aggregations": {
                    "total_amount": "col('amount').sum()",
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert "total_amount" in result.columns
        customer_1 = result.filter(pl.col("customer_id") == 1)["total_amount"][0]
        assert customer_1 == 550.0
    
    def test_aggregation_polars_expression(self, sample_orders_df, empty_context):
        """Test aggregation with Polars expression object."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="polars_expr",
            config={
                "by": "customer_id",
                "aggregations": {
                    "double_sum": pl.col("amount").sum() * 2,
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert "double_sum" in result.columns
        customer_1 = result.filter(pl.col("customer_id") == 1)["double_sum"][0]
        assert customer_1 == 1100.0  # 550 * 2


class TestGroupByTransformerAllAggregations:
    """Tests for all supported aggregation functions."""
    
    @pytest.fixture
    def numeric_df(self):
        """DataFrame with numeric data for testing aggregations."""
        return pl.DataFrame({
            "group": ["A", "A", "A", "B", "B"],
            "value": [10, 20, 30, 40, 50],
        })
    
    def test_sum_aggregation(self, numeric_df):
        """Test sum aggregation."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "sum"}}
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 60  # 10 + 20 + 30
    
    def test_mean_aggregation(self, numeric_df):
        """Test mean aggregation."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "mean"}}
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 20.0  # (10 + 20 + 30) / 3
    
    def test_avg_aggregation_alias(self, numeric_df):
        """Test avg aggregation (alias for mean)."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "avg"}}
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 20.0
    
    def test_min_aggregation(self, numeric_df):
        """Test min aggregation."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "min"}}
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 10
    
    def test_max_aggregation(self, numeric_df):
        """Test max aggregation."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "max"}}
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 30
    
    def test_count_aggregation(self, numeric_df):
        """Test count aggregation."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "count"}}
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 3
    
    def test_n_unique_aggregation(self, numeric_df):
        """Test n_unique aggregation."""
        # Add duplicate values
        df = pl.DataFrame({
            "group": ["A", "A", "A", "B", "B"],
            "value": [10, 10, 20, 30, 30],  # A has 2 unique, B has 1 unique
        })
        context = TransformationContext(data=df)
        
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "n_unique"}}
            }
        )
        
        result = transformer.transform(df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 2
    
    def test_first_aggregation(self, numeric_df):
        """Test first aggregation."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "first"}},
                "maintain_order": True,
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 10  # First value in group A
    
    def test_last_aggregation(self, numeric_df):
        """Test last aggregation."""
        context = TransformationContext(data=numeric_df)
        transformer = GroupByTransformer(
            name="test",
            config={
                "by": "group",
                "aggregations": {"result": {"column": "value", "agg": "last"}},
                "maintain_order": True,
            }
        )
        
        result = transformer.transform(numeric_df, context)
        a_result = result.filter(pl.col("group") == "A")["result"][0]
        assert a_result == 30  # Last value in group A


class TestGroupByTransformerValidation:
    """Tests for configuration validation."""
    
    def test_validate_missing_by(self):
        """Test validation catches missing 'by'."""
        transformer = GroupByTransformer(
            name="test",
            config={"aggregations": {}}
        )
        
        error = transformer.validate_config({"aggregations": {}})
        
        assert error is not None
        assert "by" in error.lower()
    
    def test_validate_missing_aggregations(self):
        """Test validation catches missing 'aggregations'."""
        transformer = GroupByTransformer(
            name="test",
            config={"by": "column"}
        )
        
        error = transformer.validate_config({"by": "column"})
        
        assert error is not None
        assert "aggregations" in error.lower()
    
    def test_validate_aggregations_not_dict(self):
        """Test validation catches non-dict aggregations."""
        transformer = GroupByTransformer(
            name="test",
            config={"by": "column", "aggregations": ["sum"]}
        )
        
        error = transformer.validate_config({"by": "column", "aggregations": ["sum"]})
        
        assert error is not None
        assert "dictionary" in error.lower()
    
    def test_validate_valid_config(self):
        """Test validation passes with valid config."""
        config = {
            "by": "customer_id",
            "aggregations": {
                "total": {"column": "amount", "agg": "sum"}
            }
        }
        transformer = GroupByTransformer(name="test", config=config)
        
        error = transformer.validate_config(config)
        
        assert error is None
    
    def test_invalid_aggregation_function_raises_error(
        self, sample_orders_df, empty_context
    ):
        """Test that invalid aggregation function raises error."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="invalid_agg",
            config={
                "by": "customer_id",
                "aggregations": {
                    "result": {"column": "amount", "agg": "nonexistent_agg"}
                }
            }
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            transformer.transform(sample_orders_df, context)
        
        assert "nonexistent_agg" in str(exc_info.value).lower()
    
    def test_invalid_dict_aggregation_missing_column(
        self, sample_orders_df, empty_context
    ):
        """Test that dict aggregation without column raises error."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="missing_column",
            config={
                "by": "customer_id",
                "aggregations": {
                    "result": {"agg": "sum"}  # Missing 'column'
                }
            }
        )
        
        with pytest.raises(ConfigurationError):
            transformer.transform(sample_orders_df, context)


class TestGroupByTransformerMaintainOrder:
    """Tests for maintain_order option."""
    
    def test_maintain_order_true(self, sample_orders_df, empty_context):
        """Test maintain_order=True preserves group order."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="maintain_order",
            config={
                "by": "customer_id",
                "aggregations": {
                    "count": {"column": "order_id", "agg": "count"}
                },
                "maintain_order": True,
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        # First customer in original should be first in result
        first_customer = sample_orders_df["customer_id"][0]
        assert result["customer_id"][0] == first_customer
    
    def test_maintain_order_default_true(self, sample_orders_df, empty_context):
        """Test that maintain_order defaults to True."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="default_order",
            config={
                "by": "customer_id",
                "aggregations": {
                    "count": {"column": "order_id", "agg": "count"}
                },
                # maintain_order not specified
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        # Should maintain order by default
        first_customer = sample_orders_df["customer_id"][0]
        assert result["customer_id"][0] == first_customer


class TestGroupByTransformerProperties:
    """Tests for transformer properties."""
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = GroupByTransformer(
            name="test",
            config={"by": "col", "aggregations": {}}
        )
        
        assert transformer.transformer_type == "group_by"


class TestGroupByTransformerEdgeCases:
    """Tests for edge cases."""
    
    def test_group_by_empty_dataframe(self, empty_context):
        """Test group by on empty DataFrame."""
        empty_df = pl.DataFrame({
            "group": [],
            "value": [],
        }).cast({"group": pl.Utf8, "value": pl.Int64})
        
        context = TransformationContext(data=empty_df)
        
        transformer = GroupByTransformer(
            name="empty_group",
            config={
                "by": "group",
                "aggregations": {
                    "sum": {"column": "value", "agg": "sum"}
                }
            }
        )
        
        result = transformer.transform(empty_df, context)
        
        assert len(result) == 0
    
    def test_group_by_single_group(self, empty_context):
        """Test group by with single group."""
        df = pl.DataFrame({
            "group": ["A", "A", "A"],
            "value": [1, 2, 3],
        })
        context = TransformationContext(data=df)
        
        transformer = GroupByTransformer(
            name="single_group",
            config={
                "by": "group",
                "aggregations": {
                    "sum": {"column": "value", "agg": "sum"}
                }
            }
        )
        
        result = transformer.transform(df, context)
        
        assert len(result) == 1
        assert result["sum"][0] == 6
    
    def test_multiple_aggregations_same_column(self, sample_orders_df, empty_context):
        """Test multiple aggregations on the same column."""
        context = TransformationContext(data=sample_orders_df)
        
        transformer = GroupByTransformer(
            name="multi_agg",
            config={
                "by": "customer_id",
                "aggregations": {
                    "total": {"column": "amount", "agg": "sum"},
                    "average": {"column": "amount", "agg": "mean"},
                    "minimum": {"column": "amount", "agg": "min"},
                    "maximum": {"column": "amount", "agg": "max"},
                    "count": {"column": "amount", "agg": "count"},
                }
            }
        )
        
        result = transformer.transform(sample_orders_df, context)
        
        assert "total" in result.columns
        assert "average" in result.columns
        assert "minimum" in result.columns
        assert "maximum" in result.columns
        assert "count" in result.columns
