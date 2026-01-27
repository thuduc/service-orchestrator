"""Integration tests for the Data Transformation Framework."""

import pytest
import polars as pl
import tempfile
import json
from pathlib import Path

from frameworks.data_transformation.engine.transformation_engine import TransformationEngine
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class TestEndToEndPipelines:
    """End-to-end integration tests for transformation pipelines."""
    
    @pytest.fixture
    def sales_data(self):
        """Create sample sales data for integration tests."""
        return pl.DataFrame({
            "order_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "customer_id": [101, 102, 101, 103, 102, 101, 104, 103, 102, 101],
            "product": ["Widget", "Gadget", "Widget", "Gizmo", "Widget", 
                       "Gadget", "Widget", "Gizmo", "Gadget", "Widget"],
            "amount": [100.0, 250.0, 150.0, 75.0, 200.0, 
                      300.0, 125.0, 50.0, 175.0, 225.0],
            "status": ["completed", "completed", "pending", "completed", "completed",
                      "cancelled", "completed", "completed", "pending", "completed"],
            "order_date": ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19",
                          "2024-01-20", "2024-01-21", "2024-01-22", "2024-01-23", "2024-01-24"],
        })
    
    @pytest.fixture
    def customers_data(self):
        """Create sample customer data."""
        return pl.DataFrame({
            "customer_id": [101, 102, 103, 104, 105],
            "name": ["Alice Corp", "Bob Inc", "Charlie LLC", "Diana Co", "Eve Ltd"],
            "region": ["North", "South", "East", "West", "North"],
            "tier": ["Gold", "Silver", "Gold", "Bronze", "Silver"],
        })
    
    @pytest.fixture
    def products_data(self):
        """Create sample product data."""
        return pl.DataFrame({
            "product": ["Widget", "Gadget", "Gizmo"],
            "category": ["Electronics", "Electronics", "Accessories"],
            "cost": [40.0, 100.0, 25.0],
        })
    
    def test_customer_order_analysis_pipeline(
        self, 
        sales_data, 
        customers_data,
        products_data,
    ):
        """Test a complete customer order analysis pipeline."""
        engine = TransformationEngine()
        
        # Define a pipeline that:
        # 1. Filters to completed orders
        # 2. Joins with customers to get customer info
        # 3. Joins with products to get product category
        # 4. Groups by customer and calculates metrics
        # 5. Sorts by total spent descending
        engine.add_pipeline("customer_order_analysis", {
            "steps": [
                {
                    "name": "filter_completed",
                    "type": "filter",
                    "config": {"condition": "col('status') == 'completed'"}
                },
                {
                    "name": "join_customers",
                    "type": "join",
                    "config": {
                        "right_dataset": "customers",
                        "on": "customer_id",
                        "how": "left"
                    }
                },
                {
                    "name": "join_products",
                    "type": "join",
                    "config": {
                        "right_dataset": "products",
                        "on": "product",
                        "how": "left"
                    }
                },
                {
                    "name": "calculate_profit",
                    "type": "with_columns",
                    "config": {
                        "columns": {
                            "profit": "col('amount') - col('cost')"
                        }
                    }
                },
                {
                    "name": "aggregate_by_customer",
                    "type": "group_by",
                    "config": {
                        "by": ["customer_id", "name", "region", "tier"],
                        "aggregations": {
                            "total_orders": {"column": "order_id", "agg": "count"},
                            "total_spent": {"column": "amount", "agg": "sum"},
                            "total_profit": {"column": "profit", "agg": "sum"},
                            "avg_order_value": {"column": "amount", "agg": "mean"},
                        }
                    }
                },
                {
                    "name": "sort_by_spending",
                    "type": "sort",
                    "config": {
                        "by": "total_spent",
                        "descending": True
                    }
                }
            ]
        })
        
        result = engine.transform(
            pipeline_id="customer_order_analysis",
            data=sales_data,
            datasets={
                "customers": customers_data,
                "products": products_data,
            }
        )
        
        # Verify pipeline success
        assert result.success is True
        assert result.data is not None
        
        # Verify expected columns
        expected_columns = {"customer_id", "name", "region", "tier", 
                           "total_orders", "total_spent", "total_profit", "avg_order_value"}
        assert expected_columns.issubset(set(result.data.columns))
        
        # Verify sorting (first customer should have highest total_spent)
        totals = result.data["total_spent"].to_list()
        assert totals == sorted(totals, reverse=True)
        
        # Verify step results
        assert len(result.step_results) == 6
        assert all(step.success for step in result.step_results)
    
    def test_data_cleaning_pipeline(self, sales_data):
        """Test a data cleaning pipeline with multiple transformations."""
        engine = TransformationEngine()
        
        # Add some null values to test data cleaning
        dirty_data = sales_data.with_columns([
            pl.when(pl.col("order_id").is_in([3, 6]))
              .then(None)
              .otherwise(pl.col("amount"))
              .alias("amount"),
        ])
        
        engine.add_pipeline("clean_data", {
            "steps": [
                {
                    "name": "remove_cancelled",
                    "type": "filter",
                    "config": {"condition": "col('status') != 'cancelled'"}
                },
                {
                    "name": "fill_null_amounts",
                    "type": "fill_null",
                    "config": {
                        "column": "amount",
                        "value": 0.0
                    }
                },
                {
                    "name": "drop_nulls_critical",
                    "type": "drop_nulls",
                    "config": {
                        "columns": ["customer_id", "product"]
                    }
                },
                {
                    "name": "select_columns",
                    "type": "select",
                    "config": {
                        "columns": ["order_id", "customer_id", "product", "amount", "status"]
                    }
                }
            ]
        })
        
        result = engine.transform(
            pipeline_id="clean_data",
            data=dirty_data,
        )
        
        assert result.success is True
        # Cancelled orders removed (order 6)
        assert result.data["status"].is_in(["cancelled"]).sum() == 0
        # Null amounts filled
        assert not result.data["amount"].is_null().any()
        # Only specified columns
        assert result.data.columns == ["order_id", "customer_id", "product", "amount", "status"]
    
    def test_aggregation_pipeline(self, sales_data, customers_data):
        """Test aggregation with multiple group by columns."""
        engine = TransformationEngine()
        
        engine.add_pipeline("sales_summary", {
            "steps": [
                {
                    "name": "join_region",
                    "type": "join",
                    "config": {
                        "right_dataset": "customers",
                        "on": "customer_id",
                        "how": "left"
                    }
                },
                {
                    "name": "filter_completed",
                    "type": "filter",
                    "config": {"condition": "col('status') == 'completed'"}
                },
                {
                    "name": "group_by_region_product",
                    "type": "group_by",
                    "config": {
                        "by": ["region", "product"],
                        "aggregations": {
                            "total_sales": {"column": "amount", "agg": "sum"},
                            "order_count": {"column": "order_id", "agg": "count"},
                            "avg_sale": {"column": "amount", "agg": "mean"},
                        }
                    }
                },
                {
                    "name": "sort_results",
                    "type": "sort",
                    "config": {
                        "by": ["region", "total_sales"],
                        "descending": [False, True]
                    }
                }
            ]
        })
        
        result = engine.transform(
            pipeline_id="sales_summary",
            data=sales_data,
            datasets={"customers": customers_data},
        )
        
        assert result.success is True
        assert "region" in result.data.columns
        assert "product" in result.data.columns
        assert "total_sales" in result.data.columns
        assert "order_count" in result.data.columns
        assert "avg_sale" in result.data.columns


class TestConfigFileLoading:
    """Tests for loading pipelines from config files."""
    
    def test_load_pipeline_from_json(self, sample_customers_df):
        """Test loading pipeline configuration from JSON file."""
        config = {
            "pipelines": {
                "simple_select": {
                    "steps": [
                        {
                            "name": "select_name_email",
                            "type": "select",
                            "config": {"columns": ["customer_id", "name", "email"]}
                        }
                    ]
                },
                "filter_active": {
                    "steps": [
                        {
                            "name": "filter_active",
                            "type": "filter",
                            "config": {"condition": "col('status') == 'active'"}
                        },
                        {
                            "name": "select_fields",
                            "type": "select",
                            "config": {"columns": ["customer_id", "name"]}
                        }
                    ]
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            config_path = f.name
        
        try:
            engine = TransformationEngine(pipeline_config_path=config_path)
            
            # Verify pipelines loaded
            assert "simple_select" in engine.list_pipelines()
            assert "filter_active" in engine.list_pipelines()
            
            # Execute simple_select
            result = engine.transform(
                pipeline_id="simple_select",
                data=sample_customers_df,
            )
            
            assert result.success is True
            assert result.data.columns == ["customer_id", "name", "email"]
            
            # Execute filter_active
            result = engine.transform(
                pipeline_id="filter_active",
                data=sample_customers_df,
            )
            
            assert result.success is True
            assert result.data.columns == ["customer_id", "name"]
        
        finally:
            Path(config_path).unlink()


class TestPipelineComposition:
    """Tests for composing and chaining pipeline operations."""
    
    def test_reuse_intermediate_results(self, sample_customers_df, sample_orders_df):
        """Test reusing intermediate results across pipeline runs."""
        engine = TransformationEngine()
        
        # First pipeline: prepare customer data
        engine.add_pipeline("prepare_customers", {
            "steps": [
                {
                    "name": "filter_active",
                    "type": "filter",
                    "config": {"condition": "col('status') == 'active'"}
                },
                {
                    "name": "select_fields",
                    "type": "select",
                    "config": {"columns": ["customer_id", "name"]}
                }
            ]
        })
        
        # Second pipeline: analyze orders for active customers
        engine.add_pipeline("analyze_orders", {
            "steps": [
                {
                    "name": "join_active_customers",
                    "type": "join",
                    "config": {
                        "right_dataset": "active_customers",
                        "on": "customer_id",
                        "how": "inner"
                    }
                },
                {
                    "name": "group_by_customer",
                    "type": "group_by",
                    "config": {
                        "by": ["customer_id", "name"],
                        "aggregations": {
                            "total_orders": {"column": "order_id", "agg": "count"},
                            "total_amount": {"column": "amount", "agg": "sum"},
                        }
                    }
                }
            ]
        })
        
        # Run first pipeline
        customers_result = engine.transform(
            pipeline_id="prepare_customers",
            data=sample_customers_df,
        )
        
        assert customers_result.success is True
        active_customers = customers_result.data
        
        # Run second pipeline using result from first
        orders_result = engine.transform(
            pipeline_id="analyze_orders",
            data=sample_orders_df,
            datasets={"active_customers": active_customers},
        )
        
        assert orders_result.success is True
        assert "total_orders" in orders_result.data.columns
        assert "total_amount" in orders_result.data.columns


class TestErrorRecoveryAndReporting:
    """Tests for error handling and reporting."""
    
    def test_detailed_error_reporting(self, sample_customers_df):
        """Test that errors include detailed step information."""
        engine = TransformationEngine()
        
        engine.add_pipeline("error_pipeline", {
            "steps": [
                {
                    "name": "good_step",
                    "type": "select",
                    "config": {"columns": ["customer_id", "name"]}
                },
                {
                    "name": "bad_step",
                    "type": "select",
                    "config": {"columns": ["nonexistent_column"]}
                }
            ]
        })
        
        result = engine.transform(
            pipeline_id="error_pipeline",
            data=sample_customers_df,
        )
        
        assert result.success is False
        assert result.error_message is not None
        
        # First step should have succeeded
        assert result.step_results[0].success is True
        assert result.step_results[0].step_name == "good_step"
        
        # Second step should have failed
        assert result.step_results[1].success is False
        assert result.step_results[1].step_name == "bad_step"
        assert result.step_results[1].error_message is not None
    
    def test_pipeline_preserves_original_data_on_failure(self, sample_customers_df):
        """Test that original data is not modified when pipeline fails."""
        engine = TransformationEngine()
        original_shape = sample_customers_df.shape
        original_columns = sample_customers_df.columns.copy()
        
        engine.add_pipeline("failing_pipeline", {
            "steps": [
                {
                    "name": "select",
                    "type": "select",
                    "config": {"columns": ["nonexistent"]}
                }
            ]
        })
        
        result = engine.transform(
            pipeline_id="failing_pipeline",
            data=sample_customers_df,
        )
        
        assert result.success is False
        
        # Original data should be unchanged
        assert sample_customers_df.shape == original_shape
        assert sample_customers_df.columns == original_columns


class TestTransformationMetrics:
    """Tests for transformation metrics and monitoring."""
    
    def test_metrics_collection(self, sample_customers_df, sample_orders_df):
        """Test that metrics are properly collected during transformation."""
        engine = TransformationEngine()
        
        engine.add_pipeline("metrics_pipeline", {
            "steps": [
                {
                    "name": "filter_active",
                    "type": "filter",
                    "config": {"condition": "col('status') == 'active'"}
                },
                {
                    "name": "join_orders",
                    "type": "join",
                    "config": {
                        "right_dataset": "orders",
                        "on": "customer_id",
                        "how": "inner"
                    }
                },
                {
                    "name": "select_columns",
                    "type": "select",
                    "config": {"columns": ["customer_id", "name", "order_id", "amount"]}
                }
            ]
        })
        
        result = engine.transform(
            pipeline_id="metrics_pipeline",
            data=sample_customers_df,
            datasets={"orders": sample_orders_df},
        )
        
        assert result.success is True
        
        # Check overall metrics
        assert result.total_execution_time_ms > 0
        assert result.rows_in == 5  # Original customer count
        
        # Check step-level metrics
        for step in result.step_results:
            assert step.execution_time_ms >= 0
            assert step.rows_in >= 0
            assert step.rows_out >= 0
            assert step.columns_in > 0
            assert step.columns_out > 0
        
        # Filter should reduce rows
        filter_step = result.step_results[0]
        assert filter_step.rows_out <= filter_step.rows_in
        
        # Select should reduce columns
        select_step = result.step_results[2]
        assert select_step.columns_out == 4
