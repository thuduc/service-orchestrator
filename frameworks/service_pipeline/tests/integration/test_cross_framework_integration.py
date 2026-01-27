"""
Cross-Framework Integration Tests

Tests the complete integration of components from multiple frameworks working together
in a Service Pipeline:
- Data Validation Framework (DataValidationComponent)
- Data Transformation Framework (DataTransformationComponent)  
- Service Pipeline Framework (built-in components)

This demonstrates the pluggable architecture where specialized frameworks can be
composed into cohesive data processing pipelines.
"""
import pytest
import json
import tempfile
import os
from typing import Any, Dict

import polars as pl

from frameworks.service_pipeline.orchestration.service_registry import ServiceRegistry
from frameworks.service_pipeline.orchestration.service_entrypoint import ServiceEntrypoint


class TestCrossFrameworkIntegration:
    """
    Integration tests demonstrating components from all frameworks working together.
    
    Pipeline Flow:
    1. DataValidationComponent (Data Validation Framework) - Validates order data
    2. DataTransformationComponent (Data Transformation Framework) - Enriches orders
    3. TransformationComponent (Service Pipeline) - Uppercase product names
    4. DataTransformationComponent (Data Transformation Framework) - Aggregates by category
    """

    @pytest.fixture
    def sample_orders_data(self) -> Dict[str, Any]:
        """Sample order data for testing."""
        return {
            "order_id": [1001, 1002, 1003, 1004, 1005],
            "customer_id": [101, 102, 101, 103, 102],
            "product_name": ["laptop", "mouse", "keyboard", "monitor", "webcam"],
            "category": ["electronics", "accessories", "accessories", "electronics", "accessories"],
            "quantity": [1, 2, 1, 1, 3],
            "unit_price": [999.99, 29.99, 79.99, 299.99, 49.99],
        }

    @pytest.fixture
    def category_reference_data(self) -> Dict[str, Any]:
        """Reference data for category enrichment."""
        return {
            "category": ["electronics", "accessories"],
            "category_display_name": ["Consumer Electronics", "Computer Accessories"],
            "tax_rate": [0.08, 0.05],
        }

    @pytest.fixture
    def cross_framework_configs(self, sample_orders_data, category_reference_data):
        """Create configuration files for cross-framework integration test."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # ============================================================
            # Create validation pipeline configuration
            # ============================================================
            validation_pipelines_config = {
                "validation_pipelines": {
                    "order_validation": {
                        "description": "Validates incoming order data",
                        "on_failure": "collect_all",
                        "stages": [
                            {
                                "name": "schema_validation",
                                "type": "schema_validation",
                                "config": {
                                    "columns": {
                                        "order_id": {"dtype": "Int64", "nullable": False},
                                        "customer_id": {"dtype": "Int64", "nullable": False},
                                        "product_name": {"dtype": "Utf8", "nullable": False},
                                        "category": {"dtype": "Utf8", "nullable": False},
                                        "quantity": {
                                            "dtype": "Int64",
                                            "nullable": False,
                                            "checks": [
                                                {"builtin": "greater_than", "min_value": 0}
                                            ]
                                        },
                                        "unit_price": {
                                            "dtype": "Float64",
                                            "nullable": False,
                                            "checks": [
                                                {"builtin": "greater_than", "min_value": 0}
                                            ]
                                        },
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            
            validation_config_path = os.path.join(temp_dir, "validation_pipelines.json")
            with open(validation_config_path, 'w') as f:
                json.dump(validation_pipelines_config, f)

            # ============================================================
            # Create transformation pipeline configurations
            # ============================================================
            transformation_pipelines_config = {
                "pipelines": {
                    "order_enrichment": {
                        "description": "Enriches orders with calculated fields and category info",
                        "steps": [
                            {
                                "name": "calculate_line_total",
                                "type": "with_columns",
                                "config": {
                                    "columns": {
                                        "line_total": "col('quantity') * col('unit_price')"
                                    }
                                }
                            },
                            {
                                "name": "cast_category_for_join",
                                "type": "cast",
                                "config": {
                                    "schema": {
                                        "category": "Utf8"
                                    }
                                }
                            },
                            {
                                "name": "join_category_info",
                                "type": "join",
                                "config": {
                                    "right_dataset": "categories",
                                    "on": "category",
                                    "how": "left"
                                }
                            },
                            {
                                "name": "calculate_tax",
                                "type": "with_columns",
                                "config": {
                                    "columns": {
                                        "tax_amount": "col('line_total') * col('tax_rate')",
                                        "total_with_tax": "col('line_total') * (1 + col('tax_rate'))"
                                    }
                                }
                            }
                        ]
                    },
                    "order_aggregation": {
                        "description": "Aggregates orders by category",
                        "steps": [
                            {
                                "name": "aggregate_by_category",
                                "type": "group_by",
                                "config": {
                                    "by": ["category_display_name"],
                                    "aggregations": {
                                        "total_orders": {"column": "order_id", "agg": "count"},
                                        "total_quantity": {"column": "quantity", "agg": "sum"},
                                        "total_revenue": {"column": "total_with_tax", "agg": "sum"},
                                        "avg_order_value": {"column": "total_with_tax", "agg": "mean"}
                                    }
                                }
                            },
                            {
                                "name": "sort_by_revenue",
                                "type": "sort",
                                "config": {
                                    "by": ["total_revenue"],
                                    "descending": True
                                }
                            }
                        ]
                    }
                }
            }
            
            transformation_config_path = os.path.join(temp_dir, "transformation_pipelines.json")
            with open(transformation_config_path, 'w') as f:
                json.dump(transformation_pipelines_config, f)

            # ============================================================
            # Create services configuration with 4-step pipeline
            # ============================================================
            services_config = {
                "services": {
                    "order_processing_pipeline": {
                        "description": "Complete order processing with validation, enrichment, and aggregation",
                        "steps": [
                            # Step 1: Data Validation (Data Validation Framework)
                            {
                                "name": "validate_orders",
                                "module": "frameworks.data_validation.adapters.pipeline_adapter",
                                "class": "DataValidationComponent",
                                "config": {
                                    "pipeline_config_path": validation_config_path,
                                    "pipeline_id": "order_validation",
                                    "input_key": "orders_data",
                                    "output_key": "validated_orders",
                                    "fail_on_validation_error": False
                                }
                            },
                            # Step 2: Data Transformation - Enrichment (Data Transformation Framework)
                            {
                                "name": "enrich_orders",
                                "module": "frameworks.data_transformation.adapters.pipeline_adapter",
                                "class": "DataTransformationComponent",
                                "config": {
                                    "pipeline_config_path": transformation_config_path,
                                    "pipeline_id": "order_enrichment",
                                    "input_key": "validated_orders",
                                    "output_key": "enriched_orders",
                                    "datasets_key": "reference_data",
                                    "fail_on_error": True
                                }
                            },
                            # Step 3: Simple Transformation (Service Pipeline Framework)
                            {
                                "name": "format_product_names",
                                "module": "frameworks.service_pipeline.implementation.components.transformation",
                                "class": "TransformationComponent",
                                "config": {
                                    "transform_type": "uppercase"
                                }
                            },
                            # Step 4: Data Transformation - Aggregation (Data Transformation Framework)
                            {
                                "name": "aggregate_orders",
                                "module": "frameworks.data_transformation.adapters.pipeline_adapter",
                                "class": "DataTransformationComponent",
                                "config": {
                                    "pipeline_config_path": transformation_config_path,
                                    "pipeline_id": "order_aggregation",
                                    "input_key": "enriched_orders",
                                    "output_key": "order_summary",
                                    "result_key": "aggregation_metrics",
                                    "fail_on_error": True
                                }
                            }
                        ]
                    }
                }
            }

            services_file = os.path.join(temp_dir, "services.json")
            with open(services_file, 'w') as f:
                json.dump(services_config, f)

            # ============================================================
            # Create interceptors configuration (logging only)
            # ============================================================
            interceptors_config = {
                "interceptors": {
                    "logging": {
                        "module": "frameworks.service_pipeline.implementation.interceptors.logging",
                        "class": "LoggingInterceptor",
                        "enabled": True,
                        "order": 10,
                        "config": {
                            "log_level": "DEBUG",
                            "destinations": ["file"],
                            "file_path": os.path.join(temp_dir, "pipeline.log")
                        }
                    }
                }
            }

            interceptors_file = os.path.join(temp_dir, "interceptors.json")
            with open(interceptors_file, 'w') as f:
                json.dump(interceptors_config, f)

            yield {
                "temp_dir": temp_dir,
                "services_file": services_file,
                "interceptors_file": interceptors_file,
                "validation_config_path": validation_config_path,
                "transformation_config_path": transformation_config_path,
                "log_file": os.path.join(temp_dir, "pipeline.log"),
            }

    def test_full_cross_framework_pipeline_success(
        self, 
        cross_framework_configs, 
        sample_orders_data,
        category_reference_data
    ):
        """
        Test complete pipeline execution with all 4 steps from different frameworks.
        
        This test verifies:
        1. DataValidationComponent validates the order data
        2. DataTransformationComponent enriches orders with calculations and joins
        3. TransformationComponent (Service Pipeline) applies uppercase formatting
        4. DataTransformationComponent aggregates data by category
        """
        # Initialize service registry and entrypoint
        service_registry = ServiceRegistry(cross_framework_configs["services_file"])
        entrypoint = ServiceEntrypoint(
            service_registry,
            interceptor_config_path=cross_framework_configs["interceptors_file"]
        )

        # Prepare context with input data
        context = {
            "service_id": "order_processing_pipeline",
            "request_id": "cross_framework_test_001",
            "orders_data": sample_orders_data,
            "reference_data": {
                "categories": category_reference_data
            }
        }

        # Execute the pipeline
        result = entrypoint.execute(context)

        # ============================================================
        # Verify Step 1: Data Validation passed
        # ============================================================
        assert result.get("validation_passed") is True, "Validation should pass for valid data"
        assert "validated_orders" in result, "Validated orders should be in context"
        
        validated_df = result["validated_orders"]
        assert isinstance(validated_df, pl.DataFrame), "Validated data should be a Polars DataFrame"
        assert len(validated_df) == 5, "All 5 orders should be validated"

        # ============================================================
        # Verify Step 2: Data Transformation (Enrichment) succeeded
        # ============================================================
        assert result.get("transformation_success") is True, "Enrichment transformation should succeed"
        assert "enriched_orders" in result, "Enriched orders should be in context"
        
        enriched_df = result["enriched_orders"]
        assert isinstance(enriched_df, pl.DataFrame), "Enriched data should be a Polars DataFrame"
        
        # Verify calculated fields were added
        assert "line_total" in enriched_df.columns, "line_total should be calculated"
        assert "tax_amount" in enriched_df.columns, "tax_amount should be calculated"
        assert "total_with_tax" in enriched_df.columns, "total_with_tax should be calculated"
        assert "category_display_name" in enriched_df.columns, "category_display_name should be joined"
        assert "tax_rate" in enriched_df.columns, "tax_rate should be joined"

        # Verify calculations are correct for first order (laptop: 1 * 999.99 = 999.99)
        laptop_row = enriched_df.filter(pl.col("product_name") == "laptop")
        assert len(laptop_row) == 1
        line_total = laptop_row["line_total"][0]
        assert abs(line_total - 999.99) < 0.01, f"Line total should be 999.99, got {line_total}"

        # ============================================================
        # Verify Step 3: Service Pipeline TransformationComponent executed
        # ============================================================
        # The TransformationComponent adds transformed_data with uppercase strings
        assert "transformed_data" in result, "transformed_data should be present from step 3"
        assert result.get("transform_type") == "uppercase", "Transform type should be uppercase"

        # ============================================================
        # Verify Step 4: Data Transformation (Aggregation) succeeded
        # ============================================================
        assert "order_summary" in result, "Order summary should be in context"
        
        summary_df = result["order_summary"]
        assert isinstance(summary_df, pl.DataFrame), "Summary should be a Polars DataFrame"
        
        # Should have 2 category groups (Consumer Electronics, Computer Accessories)
        assert len(summary_df) == 2, "Should have 2 category groups"
        
        # Verify aggregation columns exist
        assert "category_display_name" in summary_df.columns
        assert "total_orders" in summary_df.columns
        assert "total_quantity" in summary_df.columns
        assert "total_revenue" in summary_df.columns
        assert "avg_order_value" in summary_df.columns
        
        # Verify aggregation metrics are present
        assert "aggregation_metrics" in result, "Aggregation metrics should be in context"
        metrics = result["aggregation_metrics"]
        assert metrics["success"] is True
        assert metrics["rows_in"] == 5  # 5 input orders
        assert metrics["rows_out"] == 2  # 2 category groups

        # ============================================================
        # Verify logging captured all steps
        # ============================================================
        log_file = cross_framework_configs["log_file"]
        assert os.path.exists(log_file), "Log file should exist"
        
        with open(log_file, 'r') as f:
            log_content = f.read()
        
        assert "cross_framework_test_001" in log_content, "Request ID should be in logs"

    def test_cross_framework_pipeline_with_validation_failure(
        self,
        cross_framework_configs,
        category_reference_data
    ):
        """
        Test pipeline behavior when validation fails.
        
        When validation fails (fail_on_validation_error=False), the pipeline
        should continue but mark validation as failed.
        """
        service_registry = ServiceRegistry(cross_framework_configs["services_file"])
        entrypoint = ServiceEntrypoint(
            service_registry,
            interceptor_config_path=cross_framework_configs["interceptors_file"]
        )

        # Invalid data: negative quantity and price
        invalid_orders_data = {
            "order_id": [1001, 1002],
            "customer_id": [101, 102],
            "product_name": ["laptop", "mouse"],
            "category": ["electronics", "accessories"],
            "quantity": [-1, 0],  # Invalid: not positive
            "unit_price": [999.99, -10.00],  # Invalid: second price is negative
        }

        context = {
            "service_id": "order_processing_pipeline",
            "request_id": "validation_failure_test_001",
            "orders_data": invalid_orders_data,
            "reference_data": {
                "categories": category_reference_data
            }
        }

        result = entrypoint.execute(context)

        # Validation should fail due to business rules
        assert result.get("validation_passed") is False, "Validation should fail for invalid data"
        assert "validation_errors" in result, "Validation errors should be present"
        assert len(result["validation_errors"]) > 0, "Should have validation errors"

    def test_cross_framework_pipeline_data_flow(
        self,
        cross_framework_configs,
        sample_orders_data,
        category_reference_data
    ):
        """
        Test that data flows correctly between components from different frameworks.
        
        Verifies the data transformation chain:
        - Raw data → Validated data → Enriched data → Formatted data → Aggregated data
        """
        service_registry = ServiceRegistry(cross_framework_configs["services_file"])
        entrypoint = ServiceEntrypoint(
            service_registry,
            interceptor_config_path=cross_framework_configs["interceptors_file"]
        )

        context = {
            "service_id": "order_processing_pipeline",
            "request_id": "data_flow_test_001",
            "orders_data": sample_orders_data,
            "reference_data": {
                "categories": category_reference_data
            }
        }

        result = entrypoint.execute(context)

        # Trace data flow
        # 1. Original data had 6 columns
        original_columns = set(sample_orders_data.keys())
        assert len(original_columns) == 6

        # 2. After validation, same columns preserved
        validated_df = result["validated_orders"]
        assert all(col in validated_df.columns for col in original_columns)

        # 3. After enrichment, new columns added
        enriched_df = result["enriched_orders"]
        enriched_columns = set(enriched_df.columns)
        new_columns = enriched_columns - original_columns
        expected_new_columns = {"line_total", "category_display_name", "tax_rate", "tax_amount", "total_with_tax"}
        assert expected_new_columns.issubset(new_columns), f"Expected columns {expected_new_columns}, got {new_columns}"

        # 4. After aggregation, grouped columns
        summary_df = result["order_summary"]
        assert "category_display_name" in summary_df.columns
        assert "total_revenue" in summary_df.columns

        # Verify revenue calculation (sum of all total_with_tax values)
        total_revenue_sum = summary_df["total_revenue"].sum()
        assert total_revenue_sum > 0, "Total revenue should be positive"

    def test_cross_framework_pipeline_with_empty_data(
        self,
        cross_framework_configs,
        category_reference_data
    ):
        """
        Test pipeline behavior with empty input data.
        
        Note: Empty data with proper dtypes should pass through the pipeline.
        However, schema validation may fail if dtypes can't be inferred from empty columns.
        This test verifies the pipeline handles empty data gracefully either way.
        """
        service_registry = ServiceRegistry(cross_framework_configs["services_file"])
        entrypoint = ServiceEntrypoint(
            service_registry,
            interceptor_config_path=cross_framework_configs["interceptors_file"]
        )

        # Create empty DataFrame with proper schema to ensure dtypes are correct
        # (Polars can't infer dtypes from empty lists without explicit casting)
        import polars as pl
        empty_df = pl.DataFrame({
            "order_id": pl.Series([], dtype=pl.Int64),
            "customer_id": pl.Series([], dtype=pl.Int64),
            "product_name": pl.Series([], dtype=pl.Utf8),
            "category": pl.Series([], dtype=pl.Utf8),
            "quantity": pl.Series([], dtype=pl.Int64),
            "unit_price": pl.Series([], dtype=pl.Float64),
        })

        context = {
            "service_id": "order_processing_pipeline",
            "request_id": "empty_data_test_001",
            "orders_data": empty_df,
            "reference_data": {
                "categories": category_reference_data
            }
        }

        result = entrypoint.execute(context)

        # Empty data with correct schema should pass validation
        assert result.get("validation_passed") is True, "Empty data with correct schema should pass validation"
        
        if "validated_orders" in result:
            validated_df = result["validated_orders"]
            assert len(validated_df) == 0, "Validated data should be empty"
        
        # Verify the pipeline continues to process empty data through all stages
        assert "enriched_orders" in result, "Enriched orders should be present even if empty"
        assert len(result["enriched_orders"]) == 0, "Enriched data should be empty"


class TestCrossFrameworkProgrammaticConfiguration:
    """
    Tests for programmatic configuration of cross-framework pipelines.
    
    Demonstrates how to configure pipelines without JSON config files.
    """

    def test_programmatic_pipeline_configuration(self):
        """
        Test creating a cross-framework pipeline programmatically.
        
        This shows how components can be configured and wired together in code.
        """
        from frameworks.data_validation.adapters.pipeline_adapter import DataValidationComponent
        from frameworks.data_transformation.adapters.pipeline_adapter import DataTransformationComponent
        from frameworks.data_transformation.engine.pipeline_builder import Pipeline
        from frameworks.service_pipeline.implementation.components.transformation import TransformationComponent

        # ============================================================
        # Step 1: Configure Data Validation Component
        # ============================================================
        validation_component = DataValidationComponent({
            "pipeline_id": "inline_validation",
            "input_key": "raw_data",
            "output_key": "valid_data",
            "fail_on_validation_error": False,
        })
        
        # Add validation pipeline programmatically
        validation_component.add_pipeline("inline_validation", {
            "stages": [
                {
                    "name": "check_required",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                            "value": {"dtype": "Int64", "nullable": False}
                        }
                    }
                }
            ]
        })

        # ============================================================
        # Step 2: Configure Data Transformation Component with Pipeline Builder
        # ============================================================
        transform_component = DataTransformationComponent({
            "pipeline_id": "inline_transform",
            "input_key": "valid_data",
            "output_key": "transformed_data",
        })
        
        # Use Pipeline builder to create transformation
        transform_pipeline = (
            Pipeline("inline_transform")
            .filter("col('value') > 0")
            .with_columns({"doubled": "col('value') * 2"})
        )
        transform_component.add_pipeline_from_builder(transform_pipeline)

        # ============================================================
        # Step 3: Configure Service Pipeline Component
        # ============================================================
        format_component = TransformationComponent({
            "transform_type": "uppercase"
        })

        # ============================================================
        # Execute pipeline manually (simulating StepsExecutor behavior)
        # ============================================================
        test_data = {
            "id": [1, 2, 3, 4],
            "value": [10, -5, 20, 30],
            "name": ["alpha", "beta", "gamma", "delta"]
        }

        context = {
            "request_id": "programmatic_test_001",
            "raw_data": test_data,
        }

        # Step 1: Validation
        context = validation_component.execute(context)
        assert context.get("validation_passed") is True

        # Step 2: Transformation
        context = transform_component.execute(context)
        assert context.get("transformation_success") is True
        assert "transformed_data" in context
        
        # Verify filter removed negative values
        transformed_df = context["transformed_data"]
        assert len(transformed_df) == 3  # Filtered out value=-5
        assert "doubled" in transformed_df.columns

        # Step 3: Service Pipeline formatting
        # Note: This component works on dict data, not DataFrames
        context["data"] = {"message": "test"}  # Provide dict data for this component
        context = format_component.execute(context)
        assert context["transform_type"] == "uppercase"


class TestCrossFrameworkErrorScenarios:
    """Tests for error handling across framework boundaries."""

    def test_transformation_failure_propagation(self):
        """
        Test that errors from Data Transformation Framework propagate correctly.
        """
        from frameworks.data_transformation.adapters.pipeline_adapter import (
            DataTransformationComponent,
            DataTransformationError
        )

        component = DataTransformationComponent({
            "pipeline_id": "bad_pipeline",
            "input_key": "data",
            "fail_on_error": True,
        })

        # Add a pipeline that will fail (references non-existent column)
        component.add_pipeline("bad_pipeline", {
            "steps": [
                {
                    "name": "bad_select",
                    "type": "select",
                    "config": {"columns": ["nonexistent_column"]}
                }
            ]
        })

        context = {
            "data": {"id": [1, 2, 3], "value": [10, 20, 30]}
        }

        with pytest.raises(DataTransformationError):
            component.execute(context)

    def test_validation_failure_non_strict_mode(self):
        """
        Test that validation failures in non-strict mode don't stop the pipeline.
        """
        from frameworks.data_validation.adapters.pipeline_adapter import DataValidationComponent

        component = DataValidationComponent({
            "pipeline_id": "strict_validation",
            "input_key": "data",
            "fail_on_validation_error": False,  # Non-strict mode
        })

        component.add_pipeline("strict_validation", {
            "stages": [
                {
                    "name": "check_positive",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                            "value": {
                                "dtype": "Int64",
                                "nullable": False,
                                "checks": [
                                    {"builtin": "greater_than", "min_value": 0}
                                ]
                            }
                        }
                    }
                }
            ]
        })

        # Data with invalid values
        context = {
            "data": {"id": [1, 2], "value": [-10, 20]}  # First value is invalid
        }

        # Should not raise, but should mark validation as failed
        result = component.execute(context)
        assert result.get("validation_passed") is False
        assert len(result.get("validation_errors", [])) > 0
