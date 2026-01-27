"""Service Pipeline adapter for the Data Validation Framework."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import polars as pl

from frameworks.service_pipeline.implementation.base_component import BaseComponent
from frameworks.data_validation.contract.check import CustomCheck
from frameworks.data_validation.engine.validation_engine import ValidationEngine
from frameworks.data_validation.engine.validation_result import ValidationResult


class DataValidationComponent(BaseComponent):
    """
    Service Pipeline adapter component for data validation.
    
    This component integrates the Data Validation Framework into the Service Pipeline,
    allowing validation pipelines to be executed as part of a service workflow.
    
    Configuration options:
        pipeline_config_path: Path to validation pipelines JSON/YAML config
        check_config_path: Path to custom checks JSON/YAML config
        pipeline_id: ID of the validation pipeline to execute
        input_key: Context key containing input data (default: 'data')
        output_key: Context key for validated data (default: 'validated_data')
        errors_key: Context key for validation errors (default: 'validation_errors')
        result_key: Context key for full validation result (default: 'validation_result')
        reference_data_key: Context key for reference datasets (default: 'reference_data')
        fail_on_validation_error: If True, raise exception on validation failure (default: False)
        convert_to_polars: If True, convert dict/list input to Polars DataFrame (default: True)
    
    Example configuration in services.json:
        ```json
        {
          "name": "validate_customers",
          "module": "frameworks.data_validation.adapters.pipeline_adapter",
          "class": "DataValidationComponent",
          "config": {
            "pipeline_config_path": "config/validation_pipelines.json",
            "check_config_path": "config/custom_checks.json",
            "pipeline_id": "customer_validation",
            "input_key": "customers_data",
            "fail_on_validation_error": true
          }
        }
        ```
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the data validation component.
        
        Args:
            config: Component configuration dictionary
        """
        super().__init__(config)
        
        # Configuration options
        self._pipeline_config_path = self.config.get("pipeline_config_path")
        self._check_config_path = self.config.get("check_config_path")
        self._pipeline_id = self.config.get("pipeline_id")
        self._input_key = self.config.get("input_key", "data")
        self._output_key = self.config.get("output_key", "validated_data")
        self._errors_key = self.config.get("errors_key", "validation_errors")
        self._result_key = self.config.get("result_key", "validation_result")
        self._reference_data_key = self.config.get("reference_data_key", "reference_data")
        self._fail_on_error = self.config.get("fail_on_validation_error", False)
        self._convert_to_polars = self.config.get("convert_to_polars", True)
        
        # Lazy-initialized engine
        self._engine: Optional[ValidationEngine] = None
        
        # Additional checks registered programmatically
        self._additional_checks: Dict[str, tuple] = {}

    def _get_engine(self) -> ValidationEngine:
        """
        Get or create the validation engine.
        
        Returns:
            Configured ValidationEngine instance
        """
        if self._engine is None:
            self._engine = ValidationEngine(
                pipeline_config_path=self._pipeline_config_path,
                check_config_path=self._check_config_path,
            )
            
            # Register any additional checks
            for check_id, (check_class, default_params) in self._additional_checks.items():
                self._engine.register_check(check_id, check_class, default_params)
        
        return self._engine

    def register_check(
        self,
        check_id: str,
        check_class: Type[CustomCheck],
        default_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Register a custom check for use in validation.
        
        This method should be called before execute() to register custom checks
        that aren't defined in the check configuration file.
        
        Args:
            check_id: Unique identifier for the check
            check_class: The check class (must be subclass of CustomCheck)
            default_params: Optional default parameters
        """
        self._additional_checks[check_id] = (check_class, default_params)
        
        # If engine already created, register directly
        if self._engine is not None:
            self._engine.register_check(check_id, check_class, default_params)

    def add_pipeline(
        self,
        pipeline_id: str,
        pipeline_config: Dict[str, Any],
    ) -> None:
        """
        Add a validation pipeline programmatically.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            pipeline_config: Pipeline configuration dictionary
        """
        engine = self._get_engine()
        engine.add_pipeline(pipeline_id, pipeline_config)

    def _convert_input_to_dataframe(self, data: Any) -> pl.DataFrame:
        """
        Convert input data to a Polars DataFrame.
        
        Args:
            data: Input data (can be DataFrame, dict, list of dicts)
            
        Returns:
            Polars DataFrame
            
        Raises:
            TypeError: If data cannot be converted
        """
        if isinstance(data, pl.DataFrame):
            return data
        
        if isinstance(data, dict):
            # Single record or column-oriented dict
            if all(isinstance(v, (list, tuple)) for v in data.values()):
                # Column-oriented: {"col1": [1,2], "col2": [3,4]}
                return pl.DataFrame(data)
            else:
                # Single record: {"col1": 1, "col2": 2}
                return pl.DataFrame([data])
        
        if isinstance(data, list):
            if len(data) == 0:
                return pl.DataFrame()
            
            if isinstance(data[0], dict):
                # List of dicts: [{"col1": 1}, {"col1": 2}]
                return pl.DataFrame(data)
        
        raise TypeError(
            f"Cannot convert data of type {type(data).__name__} to DataFrame. "
            f"Expected: pl.DataFrame, dict, or list of dicts"
        )

    def _convert_reference_data(
        self, 
        reference_data: Optional[Dict[str, Any]]
    ) -> Dict[str, pl.DataFrame]:
        """
        Convert reference data to Polars DataFrames.
        
        Args:
            reference_data: Dict of reference datasets
            
        Returns:
            Dict of Polars DataFrames
        """
        if not reference_data:
            return {}
        
        converted = {}
        for name, data in reference_data.items():
            if isinstance(data, pl.DataFrame):
                converted[name] = data
            else:
                converted[name] = self._convert_input_to_dataframe(data)
        
        return converted

    def _format_errors_for_context(self, result: ValidationResult) -> List[Dict[str, Any]]:
        """
        Format validation errors for the context.
        
        Args:
            result: Validation result
            
        Returns:
            List of error dictionaries
        """
        errors = []
        
        for stage_result in result.stage_results:
            for error in stage_result.errors:
                errors.append({
                    "stage": stage_result.stage_name,
                    "stage_type": stage_result.stage_type,
                    "check_name": error.check_name,
                    "column": error.column,
                    "message": error.error_message,
                    "failure_cases": error.failure_cases,
                    "row_indices": error.row_indices,
                })
        
        return errors

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute data validation as part of the service pipeline.
        
        This method:
        1. Extracts input data from context
        2. Converts to Polars DataFrame if needed
        3. Runs the configured validation pipeline
        4. Adds validation results to context
        
        Args:
            context: Service pipeline context dictionary
            
        Returns:
            Updated context dictionary with validation results
            
        Raises:
            ValueError: If pipeline_id not configured or input data missing
            ValidationError: If fail_on_validation_error=True and validation fails
        """
        # Call parent to set up logger
        super().execute(context)
        
        # Validate configuration
        if not self._pipeline_id:
            raise ValueError("pipeline_id must be configured for DataValidationComponent")
        
        # Get input data
        input_data = context.get(self._input_key)
        if input_data is None:
            self.log_warning(f"No input data found at context key '{self._input_key}'")
            context["validation_passed"] = False
            context[self._errors_key] = [{"message": f"No input data at '{self._input_key}'"}]
            return context
        
        # Convert to DataFrame
        try:
            if self._convert_to_polars:
                df = self._convert_input_to_dataframe(input_data)
            else:
                if not isinstance(input_data, pl.DataFrame):
                    raise TypeError("Input must be a Polars DataFrame when convert_to_polars=False")
                df = input_data
        except Exception as e:
            self.log_error(f"Failed to convert input data: {e}")
            context["validation_passed"] = False
            context[self._errors_key] = [{"message": f"Data conversion error: {str(e)}"}]
            return context
        
        # Get reference data if available
        reference_data = context.get(self._reference_data_key)
        converted_reference = self._convert_reference_data(reference_data)
        
        # Execute validation
        self.log_info(f"Executing validation pipeline '{self._pipeline_id}' on {len(df)} rows")
        
        engine = self._get_engine()
        result = engine.validate(
            pipeline_id=self._pipeline_id,
            data=df,
            context={
                "reference_data": converted_reference,
                "metadata": {
                    "service_id": context.get("service_id"),
                    "request_id": context.get("request_id"),
                },
            },
        )
        
        # Update context with results
        context["validation_passed"] = result.is_valid
        context[self._output_key] = result.validated_data
        context[self._errors_key] = self._format_errors_for_context(result)
        context[self._result_key] = result.to_dict()
        
        # Log results
        if result.is_valid:
            self.log_info(
                f"Validation passed: {result.rows_validated} rows validated "
                f"in {result.execution_time_ms:.2f}ms"
            )
        else:
            self.log_warning(
                f"Validation failed: {result.total_errors} errors, "
                f"{result.total_warnings} warnings"
            )
            
            # Optionally raise exception
            if self._fail_on_error:
                from frameworks.data_validation.engine.validation_engine import ConfigurationError
                
                error_summary = "; ".join(
                    f"{e['check_name']}: {e['message']}" 
                    for e in context[self._errors_key][:5]
                )
                raise ValueError(f"Validation failed: {error_summary}")
        
        return context


class DataValidationError(Exception):
    """Exception raised when validation fails and fail_on_validation_error is True."""
    
    def __init__(self, message: str, result: ValidationResult) -> None:
        super().__init__(message)
        self.result = result
