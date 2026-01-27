"""Integration tests for Data Validation Framework with Service Pipeline."""

from datetime import date
from typing import Any, Dict

import polars as pl
import pytest

from frameworks.data_validation.adapters.pipeline_adapter import (
    DataValidationComponent,
    DataValidationError,
)
from frameworks.data_validation.checks.base_check import BaseCheck
from frameworks.data_validation.contract.check import CheckResult


class TestDataValidationComponentBasic:
    """Test basic DataValidationComponent functionality."""

    @pytest.fixture
    def simple_pipeline_config(self) -> Dict[str, Any]:
        """Create a simple schema validation pipeline config."""
        return {
            "description": "Simple schema validation",
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "validate_schema",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                            "name": {"dtype": "Utf8", "nullable": False},
                            "value": {"dtype": "Float64", "nullable": True},
                        },
                    },
                },
            ],
        }

    @pytest.fixture
    def component_with_pipeline(self, simple_pipeline_config) -> DataValidationComponent:
        """Create a DataValidationComponent with an inline pipeline."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
            "output_key": "validated_data",
        })
        component.add_pipeline("test_pipeline", simple_pipeline_config)
        return component

    def test_execute_with_valid_data(self, component_with_pipeline):
        """Test execution with valid data passes validation."""
        context = {
            "data": pl.DataFrame({
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, None, 30.0],
            }),
        }
        
        result = component_with_pipeline.execute(context)
        
        assert result["validation_passed"] is True
        assert result["validated_data"] is not None
        assert len(result["validation_errors"]) == 0

    def test_execute_with_dict_input(self, component_with_pipeline):
        """Test execution with dict input (list of records)."""
        context = {
            "data": [
                {"id": 1, "name": "Alice", "value": 10.5},
                {"id": 2, "name": "Bob", "value": None},
                {"id": 3, "name": "Charlie", "value": 30.0},
            ],
        }
        
        result = component_with_pipeline.execute(context)
        
        assert result["validation_passed"] is True
        assert isinstance(result["validated_data"], pl.DataFrame)

    def test_execute_with_column_oriented_dict(self, component_with_pipeline):
        """Test execution with column-oriented dict input."""
        context = {
            "data": {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, None, 30.0],
            },
        }
        
        result = component_with_pipeline.execute(context)
        
        assert result["validation_passed"] is True
        assert isinstance(result["validated_data"], pl.DataFrame)

    def test_execute_with_single_record_dict(self, component_with_pipeline):
        """Test execution with single record dict input."""
        context = {
            "data": {"id": 1, "name": "Alice", "value": 10.5},
        }
        
        result = component_with_pipeline.execute(context)
        
        assert result["validation_passed"] is True
        assert len(result["validated_data"]) == 1

    def test_execute_missing_pipeline_id(self):
        """Test that missing pipeline_id raises ValueError."""
        component = DataValidationComponent({
            "input_key": "data",
        })
        
        context = {
            "data": pl.DataFrame({"id": [1]}),
        }
        
        with pytest.raises(ValueError, match="pipeline_id must be configured"):
            component.execute(context)

    def test_execute_missing_input_data(self, component_with_pipeline):
        """Test execution with missing input data."""
        context = {}  # No data key
        
        result = component_with_pipeline.execute(context)
        
        assert result["validation_passed"] is False
        assert len(result["validation_errors"]) > 0
        assert "No input data" in result["validation_errors"][0]["message"]

    def test_execute_invalid_input_type(self, component_with_pipeline):
        """Test execution with invalid input data type."""
        context = {
            "data": "not a valid data type",
        }
        
        result = component_with_pipeline.execute(context)
        
        assert result["validation_passed"] is False
        assert "conversion error" in result["validation_errors"][0]["message"].lower()

    def test_fail_on_validation_error_flag(self, simple_pipeline_config):
        """Test that fail_on_validation_error raises exception."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
            "fail_on_validation_error": True,
        })
        component.add_pipeline("test_pipeline", simple_pipeline_config)
        
        # Data with null ID (violates schema)
        context = {
            "data": pl.DataFrame({
                "id": [1, None, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, None, 30.0],
            }),
        }
        
        with pytest.raises(ValueError, match="Validation failed"):
            component.execute(context)


class TestDataValidationComponentCustomChecks:
    """Test DataValidationComponent with custom checks."""

    @pytest.fixture
    def positive_value_check(self):
        """Create a simple positive value check."""
        class PositiveValueCheck(BaseCheck):
            def __init__(self, column: str = "value", **kwargs):
                self.column = column
            
            def validate_dataframe(self, df: pl.DataFrame) -> CheckResult:
                """Validate that all values in the configured column are positive."""
                column = self.column
                
                if column not in df.columns:
                    return self._create_failure(f"Column '{column}' not found")
                
                # Find non-positive values (handle nulls)
                mask = df[column].fill_null(1) > 0  # Treat nulls as valid
                
                if mask.all():
                    return self._create_success()
                
                # Get failing rows
                failing_indices = self._get_failing_indices(df, mask)
                failing_values = self._get_failing_values(df, column, mask)
                
                return self._create_failure(
                    message=f"Found {len(failing_indices)} non-positive values in '{column}'",
                    failure_cases=failing_values,
                    row_indices=failing_indices,
                )
        
        return PositiveValueCheck

    def test_register_and_use_custom_check(self, positive_value_check):
        """Test registering and using a custom check."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
        })
        
        # Register the custom check
        component.register_check("positive_value", positive_value_check)
        
        # Add pipeline that uses the check
        component.add_pipeline("test_pipeline", {
            "description": "Test custom check",
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "validate_positive",
                    "type": "custom_rules",
                    "config": {
                        "rules": [
                            {
                                "check_id": "positive_value",
                                "params": {"column": "amount"},
                            },
                        ],
                    },
                },
            ],
        })
        
        # Valid data
        context = {
            "data": pl.DataFrame({
                "id": [1, 2, 3],
                "amount": [10.0, 20.0, 30.0],
            }),
        }
        
        result = component.execute(context)
        assert result["validation_passed"] is True

    def test_custom_check_with_failures(self, positive_value_check):
        """Test custom check that detects failures."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
        })
        
        component.register_check("positive_value", positive_value_check)
        
        component.add_pipeline("test_pipeline", {
            "description": "Test custom check",
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "validate_positive",
                    "type": "custom_rules",
                    "config": {
                        "rules": [
                            {
                                "check_id": "positive_value",
                                "params": {"column": "amount"},
                            },
                        ],
                    },
                },
            ],
        })
        
        # Data with negative values
        context = {
            "data": pl.DataFrame({
                "id": [1, 2, 3],
                "amount": [10.0, -5.0, 0.0],  # Two non-positive values
            }),
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is False
        assert len(result["validation_errors"]) > 0


class TestDataValidationComponentReferentialValidation:
    """Test DataValidationComponent with referential validation."""

    @pytest.fixture
    def referential_pipeline_config(self) -> Dict[str, Any]:
        """Create a pipeline config with referential validation."""
        return {
            "description": "Referential validation pipeline",
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "validate_schema",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "order_id": {"dtype": "Int64", "nullable": False},
                            "customer_id": {"dtype": "Int64", "nullable": False},
                            "amount": {"dtype": "Float64", "nullable": False},
                        },
                    },
                },
            ],
        }

    def test_execute_with_reference_data(self, referential_pipeline_config):
        """Test execution with reference data in context."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "orders",
            "reference_data_key": "reference_data",
        })
        
        component.add_pipeline("test_pipeline", referential_pipeline_config)
        
        context = {
            "orders": pl.DataFrame({
                "order_id": [1001, 1002, 1003],
                "customer_id": [1, 2, 3],
                "amount": [100.0, 200.0, 300.0],
            }),
            "reference_data": {
                "customers": pl.DataFrame({
                    "customer_id": [1, 2, 3, 4, 5],
                    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                }),
            },
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is True
        assert result["validated_data"] is not None


class TestDataValidationComponentMultiStage:
    """Test DataValidationComponent with multi-stage pipelines."""

    @pytest.fixture
    def multi_stage_pipeline_config(self) -> Dict[str, Any]:
        """Create a multi-stage pipeline configuration."""
        return {
            "description": "Multi-stage validation pipeline",
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "validate_schema",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                            "email": {"dtype": "Utf8", "nullable": False},
                            "age": {"dtype": "Int64", "nullable": True},
                            "status": {"dtype": "Utf8", "nullable": False},
                        },
                    },
                },
            ],
        }

    def test_multi_stage_validation_all_pass(self, multi_stage_pipeline_config):
        """Test multi-stage validation where all stages pass."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
        })
        
        component.add_pipeline("test_pipeline", multi_stage_pipeline_config)
        
        context = {
            "data": pl.DataFrame({
                "id": [1, 2, 3],
                "email": ["a@test.com", "b@test.com", "c@test.com"],
                "age": [25, 35, None],
                "status": ["active", "active", "inactive"],
            }),
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is True
        
        # Check that validation_result contains stage results
        validation_result = result["validation_result"]
        assert "stage_results" in validation_result
        assert len(validation_result["stage_results"]) == 1  # Only schema stage

    def test_fail_fast_stops_at_first_failure(self):
        """Test that fail_fast stops at first failing stage."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
        })
        
        # Pipeline with fail_fast - second stage should not run if first fails
        component.add_pipeline("test_pipeline", {
            "description": "Fail fast pipeline",
            "on_failure": "fail_fast",
            "stages": [
                {
                    "name": "first_stage",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                        },
                    },
                },
                {
                    "name": "second_stage",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "name": {"dtype": "Utf8", "nullable": False},
                        },
                    },
                },
            ],
        })
        
        # Data that fails first stage (null id)
        context = {
            "data": pl.DataFrame({
                "id": [1, None, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }),
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is False
        
        # Only first stage should have run
        validation_result = result["validation_result"]
        assert len(validation_result["stage_results"]) == 1


class TestDataValidationComponentContextKeys:
    """Test DataValidationComponent context key configuration."""

    @pytest.fixture
    def basic_pipeline_config(self) -> Dict[str, Any]:
        """Create basic pipeline config."""
        return {
            "description": "Basic pipeline",
            "stages": [
                {
                    "name": "validate",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "value": {"dtype": "Int64", "nullable": True},
                        },
                    },
                },
            ],
        }

    def test_custom_input_key(self, basic_pipeline_config):
        """Test using custom input key."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "my_custom_data",
        })
        
        component.add_pipeline("test_pipeline", basic_pipeline_config)
        
        context = {
            "my_custom_data": pl.DataFrame({"value": [1, 2, 3]}),
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is True

    def test_custom_output_keys(self, basic_pipeline_config):
        """Test using custom output keys."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
            "output_key": "clean_data",
            "errors_key": "problems",
            "result_key": "full_result",
        })
        
        component.add_pipeline("test_pipeline", basic_pipeline_config)
        
        context = {
            "data": pl.DataFrame({"value": [1, 2, 3]}),
        }
        
        result = component.execute(context)
        
        assert "clean_data" in result
        assert "problems" in result
        assert "full_result" in result
        assert result["clean_data"] is not None


class TestDataValidationComponentSchemaCoercion:
    """Test DataValidationComponent with schema coercion."""

    def test_coerce_modifies_data(self):
        """Test that coerce=True modifies the output data."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
        })
        
        component.add_pipeline("test_pipeline", {
            "description": "Coercion pipeline",
            "stages": [
                {
                    "name": "coerce_schema",
                    "type": "schema_validation",
                    "config": {
                        "coerce": True,
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                            "value": {"dtype": "Float64", "nullable": True},
                        },
                    },
                },
            ],
        })
        
        # Input with int values for 'value' column
        context = {
            "data": pl.DataFrame({
                "id": [1, 2, 3],
                "value": [10, 20, 30],  # Int, should be coerced to Float64
            }),
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is True
        
        # Check that value column was coerced to Float64
        validated = result["validated_data"]
        assert validated["value"].dtype == pl.Float64

    def test_drop_invalid_rows(self):
        """Test that drop_invalid_rows removes failing rows."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
        })
        
        component.add_pipeline("test_pipeline", {
            "description": "Drop invalid rows pipeline",
            "stages": [
                {
                    "name": "validate_with_drop",
                    "type": "schema_validation",
                    "config": {
                        "drop_invalid_rows": True,
                        "treat_dropped_as_failure": False,
                        "columns": {
                            "id": {"dtype": "Int64", "nullable": False},
                            "name": {"dtype": "Utf8", "nullable": False},
                        },
                    },
                },
            ],
        })
        
        # Data with some null IDs
        context = {
            "data": pl.DataFrame({
                "id": [1, None, 3, None, 5],
                "name": ["A", "B", "C", "D", "E"],
            }),
        }
        
        result = component.execute(context)
        
        # Should pass because invalid rows were dropped and treat_dropped_as_failure=False
        assert result["validation_passed"] is True
        
        # Check that invalid rows were removed
        validated = result["validated_data"]
        assert len(validated) == 3  # Only rows with valid IDs


class TestDataValidationComponentEndToEnd:
    """End-to-end integration tests for realistic scenarios."""

    def test_customer_data_validation_scenario(self):
        """Test a realistic customer data validation scenario."""
        component = DataValidationComponent({
            "pipeline_id": "customer_validation",
            "input_key": "customers",
        })
        
        # Create a comprehensive customer validation pipeline
        component.add_pipeline("customer_validation", {
            "description": "Customer data validation",
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "schema_check",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "customer_id": {"dtype": "Int64", "nullable": False},
                            "email": {"dtype": "Utf8", "nullable": False},
                            "first_name": {"dtype": "Utf8", "nullable": False},
                            "last_name": {"dtype": "Utf8", "nullable": False},
                            "age": {"dtype": "Int64", "nullable": True},
                            "created_at": {"dtype": "Date", "nullable": False},
                        },
                    },
                },
            ],
        })
        
        # Valid customer data
        context = {
            "customers": pl.DataFrame({
                "customer_id": [1, 2, 3],
                "email": ["john@example.com", "jane@example.com", "bob@example.com"],
                "first_name": ["John", "Jane", "Bob"],
                "last_name": ["Doe", "Smith", "Johnson"],
                "age": [30, 25, None],
                "created_at": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)],
            }),
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is True
        assert result["validation_result"]["rows_validated"] == 3

    def test_order_data_with_invalid_records(self):
        """Test order data validation with some invalid records."""
        component = DataValidationComponent({
            "pipeline_id": "order_validation",
            "input_key": "orders",
        })
        
        component.add_pipeline("order_validation", {
            "description": "Order validation",
            "on_failure": "collect_all",
            "stages": [
                {
                    "name": "schema_check",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "order_id": {"dtype": "Int64", "nullable": False},
                            "customer_id": {"dtype": "Int64", "nullable": False},
                            "amount": {"dtype": "Float64", "nullable": False},
                        },
                    },
                },
            ],
        })
        
        # Order data with invalid records (null values in non-nullable columns)
        context = {
            "orders": pl.DataFrame({
                "order_id": [1001, None, 1003],  # Null order_id
                "customer_id": [1, 2, None],  # Null customer_id
                "amount": [100.0, 200.0, 300.0],
            }),
        }
        
        result = component.execute(context)
        
        assert result["validation_passed"] is False
        assert len(result["validation_errors"]) > 0

    def test_validation_result_structure(self):
        """Test that validation result has expected structure."""
        component = DataValidationComponent({
            "pipeline_id": "test_pipeline",
            "input_key": "data",
        })
        
        component.add_pipeline("test_pipeline", {
            "description": "Test pipeline",
            "stages": [
                {
                    "name": "check_schema",
                    "type": "schema_validation",
                    "config": {
                        "columns": {
                            "id": {"dtype": "Int64"},
                        },
                    },
                },
            ],
        })
        
        context = {
            "data": pl.DataFrame({"id": [1, 2, 3]}),
        }
        
        result = component.execute(context)
        
        # Check result structure
        assert "validation_passed" in result
        assert "validated_data" in result
        assert "validation_errors" in result
        assert "validation_result" in result
        
        # Check validation_result structure
        vr = result["validation_result"]
        assert "pipeline_id" in vr
        assert "is_valid" in vr
        assert "stage_results" in vr
        assert "total_errors" in vr
        assert "total_warnings" in vr
        assert "rows_validated" in vr
        assert "execution_time_ms" in vr
        
        # Check stage result structure
        if vr["stage_results"]:
            sr = vr["stage_results"][0]
            assert "name" in sr
            assert "type" in sr
            assert "is_valid" in sr
            assert "errors" in sr
            assert "warnings" in sr
