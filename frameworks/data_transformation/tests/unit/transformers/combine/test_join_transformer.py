"""Tests for JoinTransformer."""

import pytest
import polars as pl

from frameworks.data_transformation.transformers.combine.join import JoinTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import TransformationError, ConfigurationError


class TestJoinTransformerBasic:
    """Basic tests for JoinTransformer."""
    
    def test_inner_join_on_single_column(
        self, 
        sample_customers_df, 
        sample_orders_df,
        context_with_datasets,
    ):
        """Test inner join on a single column."""
        transformer = JoinTransformer(
            name="join_orders",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "inner",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # Inner join should only include customers with orders
        assert "order_id" in result.columns
        assert "amount" in result.columns
        # All rows should have matching customer_id in both datasets
        customer_ids = result["customer_id"].unique().to_list()
        assert all(cid in [1, 2, 3, 4] for cid in customer_ids)  # Customers with orders
    
    def test_left_join_preserves_all_left_rows(
        self,
        sample_customers_df,
        sample_orders_df,
        context_with_datasets,
    ):
        """Test left join preserves all rows from left table."""
        transformer = JoinTransformer(
            name="left_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "left",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # Should have all customer_ids from original
        original_customer_ids = set(sample_customers_df["customer_id"].to_list())
        result_customer_ids = set(result["customer_id"].to_list())
        assert original_customer_ids == result_customer_ids
    
    def test_join_with_left_on_right_on(self, context_with_datasets):
        """Test join with different column names."""
        # Create a custom left DataFrame
        left_df = pl.DataFrame({
            "cust_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        })
        
        # Create right DataFrame with different key name
        right_df = pl.DataFrame({
            "customer_id": [1, 2, 4],
            "value": [100, 200, 400],
        })
        
        context = TransformationContext(
            data=left_df,
            datasets={"values": right_df},
        )
        
        transformer = JoinTransformer(
            name="join_different_keys",
            config={
                "right_dataset": "values",
                "left_on": "cust_id",
                "right_on": "customer_id",
                "how": "inner",
            }
        )
        
        result = transformer.transform(left_df, context)
        
        # Should only match cust_id 1 and 2
        assert len(result) == 2
        assert "value" in result.columns
    
    def test_join_adds_suffix_to_duplicate_columns(
        self,
        sample_customers_df,
        context_with_datasets,
    ):
        """Test that duplicate columns get suffix."""
        # Both customers and orders have 'status' column
        transformer = JoinTransformer(
            name="join_with_suffix",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "inner",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # Should have both status columns
        assert "status" in result.columns  # From left (customers)
        assert "status_right" in result.columns  # From right (orders)
    
    def test_join_custom_suffix(self, context_with_datasets, sample_customers_df):
        """Test join with custom suffix for duplicate columns."""
        transformer = JoinTransformer(
            name="join_custom_suffix",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "inner",
                "suffix": "_order",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        assert "status" in result.columns
        assert "status_order" in result.columns


class TestJoinTransformerJoinTypes:
    """Tests for different join types."""
    
    def test_inner_join(self, context_with_datasets, sample_customers_df):
        """Test inner join only includes matching rows."""
        transformer = JoinTransformer(
            name="inner_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "inner",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # All rows should have non-null order data
        assert not result["order_id"].is_null().any()
    
    def test_left_join(self, context_with_datasets, sample_customers_df):
        """Test left join includes all left rows."""
        transformer = JoinTransformer(
            name="left_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "left",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # Customer 5 (Eve) has no orders, so order_id should be null
        eve_rows = result.filter(pl.col("name") == "Eve")
        if len(eve_rows) > 0:
            assert eve_rows["order_id"].is_null().all()
    
    def test_outer_join(self, sample_customers_df, sample_orders_df):
        """Test outer join includes all rows from both tables."""
        # Create context with limited customers
        limited_customers = sample_customers_df.filter(pl.col("customer_id") <= 3)
        context = TransformationContext(
            data=limited_customers,
            datasets={"orders": sample_orders_df},
        )
        
        transformer = JoinTransformer(
            name="outer_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "full",  # Use 'full' instead of deprecated 'outer'
            }
        )
        
        result = transformer.transform(limited_customers, context)
        
        # In a full join, unmatched right rows have null on left customer_id,
        # but customer_id_right contains the value. Customer 4 is only in orders.
        # Check that we have a row with customer_id_right = 4
        assert "customer_id_right" in result.columns
        right_customer_ids = result["customer_id_right"].unique().to_list()
        assert 4 in right_customer_ids  # Customer 4 from orders only
    
    def test_semi_join(self, sample_customers_df, context_with_datasets):
        """Test semi join returns only left columns for matching rows."""
        transformer = JoinTransformer(
            name="semi_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "semi",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # Should only have left columns
        assert "order_id" not in result.columns
        assert "amount" not in result.columns
        # Should only include customers with orders
        customer_ids = result["customer_id"].to_list()
        assert 5 not in customer_ids  # Eve has no orders
    
    def test_anti_join(self, sample_customers_df, context_with_datasets):
        """Test anti join returns rows that don't match."""
        transformer = JoinTransformer(
            name="anti_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "anti",
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # Should only include customers WITHOUT orders
        customer_ids = result["customer_id"].to_list()
        assert 5 in customer_ids  # Eve has no orders
        assert 1 not in customer_ids  # Alice has orders
    
    def test_cross_join(self, sample_customers_df, sample_products_df):
        """Test cross join produces cartesian product."""
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"products": sample_products_df},
        )
        
        transformer = JoinTransformer(
            name="cross_join",
            config={
                "right_dataset": "products",
                "how": "cross",
            }
        )
        
        result = transformer.transform(sample_customers_df, context)
        
        # Cross join: 5 customers * 4 products = 20 rows
        assert len(result) == 5 * 4


class TestJoinTransformerValidation:
    """Tests for configuration validation."""
    
    def test_validate_missing_right_dataset(self):
        """Test validation catches missing right_dataset."""
        transformer = JoinTransformer(name="test", config={})
        
        error = transformer.validate_config({})
        
        assert error is not None
        assert "right_dataset" in error.lower()
    
    def test_validate_valid_config(self):
        """Test validation passes with valid config."""
        transformer = JoinTransformer(
            name="test",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
            }
        )
        
        error = transformer.validate_config({
            "right_dataset": "orders",
            "on": "customer_id",
        })
        
        assert error is None
    
    def test_invalid_join_type_raises_error(
        self,
        sample_customers_df,
        context_with_datasets,
    ):
        """Test that invalid join type raises error."""
        transformer = JoinTransformer(
            name="invalid_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "invalid_type",
            }
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            transformer.transform(sample_customers_df, context_with_datasets)
        
        assert "invalid_type" in str(exc_info.value).lower()
    
    def test_missing_dataset_raises_error(self, sample_customers_df):
        """Test that missing dataset raises error."""
        context = TransformationContext(
            data=sample_customers_df,
            datasets={},  # No datasets
        )
        
        transformer = JoinTransformer(
            name="join_missing",
            config={
                "right_dataset": "nonexistent",
                "on": "customer_id",
            }
        )
        
        with pytest.raises(TransformationError) as exc_info:
            transformer.transform(sample_customers_df, context)
        
        assert "nonexistent" in str(exc_info.value).lower()
    
    def test_missing_join_keys_raises_error(
        self,
        sample_customers_df,
        context_with_datasets,
    ):
        """Test that missing join keys raises error (for non-cross joins)."""
        transformer = JoinTransformer(
            name="no_keys",
            config={
                "right_dataset": "orders",
                "how": "inner",
                # Missing: on, left_on, right_on
            }
        )
        
        with pytest.raises(ConfigurationError) as exc_info:
            transformer.transform(sample_customers_df, context_with_datasets)
        
        assert "on" in str(exc_info.value).lower() or "left_on" in str(exc_info.value).lower()


class TestJoinTransformerProperties:
    """Tests for transformer properties."""
    
    def test_transformer_type(self):
        """Test transformer_type property."""
        transformer = JoinTransformer(
            name="test",
            config={"right_dataset": "orders", "on": "id"}
        )
        
        assert transformer.transformer_type == "join"
    
    def test_input_type_is_multi(self):
        """Test input_type is multi for joins."""
        transformer = JoinTransformer(
            name="test",
            config={"right_dataset": "orders", "on": "id"}
        )
        
        assert transformer.input_type == "multi"
    
    def test_get_required_datasets(self):
        """Test get_required_datasets returns right_dataset."""
        transformer = JoinTransformer(
            name="test",
            config={"right_dataset": "orders", "on": "id"}
        )
        
        required = transformer.get_required_datasets()
        
        assert "orders" in required


class TestJoinTransformerEdgeCases:
    """Tests for edge cases."""
    
    def test_join_on_multiple_columns(self, sample_orders_df):
        """Test join on multiple columns."""
        left_df = pl.DataFrame({
            "customer_id": [1, 1, 2],
            "order_date": ["2023-06-01", "2023-06-15", "2023-06-10"],
            "note": ["first", "second", "third"],
        })
        
        context = TransformationContext(
            data=left_df,
            datasets={"orders": sample_orders_df},
        )
        
        transformer = JoinTransformer(
            name="multi_key_join",
            config={
                "right_dataset": "orders",
                "on": ["customer_id", "order_date"],
                "how": "inner",
            }
        )
        
        result = transformer.transform(left_df, context)
        
        assert "order_id" in result.columns
        assert "note" in result.columns
    
    def test_join_empty_left_dataframe(self, sample_orders_df):
        """Test joining with empty left DataFrame."""
        empty_df = pl.DataFrame({
            "customer_id": [],
            "name": [],
        }).cast({"customer_id": pl.Int64, "name": pl.Utf8})
        
        context = TransformationContext(
            data=empty_df,
            datasets={"orders": sample_orders_df},
        )
        
        transformer = JoinTransformer(
            name="join_empty_left",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "inner",
            }
        )
        
        result = transformer.transform(empty_df, context)
        
        assert len(result) == 0
    
    def test_join_empty_right_dataframe(self, sample_customers_df):
        """Test joining with empty right DataFrame."""
        empty_orders = pl.DataFrame({
            "customer_id": [],
            "order_id": [],
            "amount": [],
        }).cast({"customer_id": pl.Int64, "order_id": pl.Int64, "amount": pl.Float64})
        
        context = TransformationContext(
            data=sample_customers_df,
            datasets={"orders": empty_orders},
        )
        
        transformer = JoinTransformer(
            name="join_empty_right",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                "how": "inner",
            }
        )
        
        result = transformer.transform(sample_customers_df, context)
        
        assert len(result) == 0
    
    def test_join_default_how_is_inner(
        self,
        sample_customers_df,
        context_with_datasets,
    ):
        """Test that default join type is inner."""
        transformer = JoinTransformer(
            name="default_join",
            config={
                "right_dataset": "orders",
                "on": "customer_id",
                # No 'how' specified
            }
        )
        
        result = transformer.transform(sample_customers_df, context_with_datasets)
        
        # Inner join behavior: no null order_ids
        assert not result["order_id"].is_null().any()
