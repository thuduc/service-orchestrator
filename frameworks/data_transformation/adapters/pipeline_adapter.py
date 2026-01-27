"""Service Pipeline adapter for the Data Transformation Framework."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import polars as pl

from frameworks.service_pipeline.implementation.base_component import BaseComponent
from frameworks.data_transformation.engine.transformation_engine import TransformationEngine
from frameworks.data_transformation.engine.pipeline_builder import Pipeline
from frameworks.data_transformation.contract.result import TransformationResult


class DataTransformationComponent(BaseComponent):
    """
    Service Pipeline adapter component for data transformation.
    
    This component integrates the Data Transformation Framework into the Service Pipeline,
    allowing transformation pipelines to be executed as part of a service workflow.
    
    Configuration options:
        pipeline_config_path: Path to transformation pipelines JSON/YAML config
        pipeline_id: ID of the transformation pipeline to execute
        input_key: Context key containing input data (default: 'data')
        output_key: Context key for transformed data (default: 'transformed_data')
        result_key: Context key for full transformation result (default: 'transformation_result')
        datasets_key: Context key for additional datasets for joins (default: 'datasets')
        fail_on_error: If True, raise exception on transformation failure (default: True)
        convert_to_polars: If True, convert dict/list input to Polars DataFrame (default: True)
    
    Example configuration in services.json:
        ```json
        {
          "name": "transform_orders",
          "module": "frameworks.data_transformation.adapters.pipeline_adapter",
          "class": "DataTransformationComponent",
          "config": {
            "pipeline_config_path": "config/transformation_pipelines.json",
            "pipeline_id": "order_enrichment",
            "input_key": "raw_orders",
            "output_key": "enriched_orders",
            "datasets_key": "reference_data",
            "fail_on_error": true
          }
        }
        ```
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the data transformation component.
        
        Args:
            config: Component configuration dictionary
        """
        super().__init__(config)
        
        # Configuration options
        self._pipeline_config_path = self.config.get("pipeline_config_path")
        self._pipeline_id = self.config.get("pipeline_id")
        self._input_key = self.config.get("input_key", "data")
        self._output_key = self.config.get("output_key", "transformed_data")
        self._result_key = self.config.get("result_key", "transformation_result")
        self._datasets_key = self.config.get("datasets_key", "datasets")
        self._fail_on_error = self.config.get("fail_on_error", True)
        self._convert_to_polars = self.config.get("convert_to_polars", True)
        
        # Lazy-initialized engine
        self._engine: Optional[TransformationEngine] = None
        
        # Pipelines added programmatically
        self._additional_pipelines: Dict[str, Dict[str, Any]] = {}

    def _get_engine(self) -> TransformationEngine:
        """
        Get or create the transformation engine.
        
        Returns:
            Configured TransformationEngine instance
        """
        if self._engine is None:
            self._engine = TransformationEngine(
                pipeline_config_path=self._pipeline_config_path,
            )
            
            # Register any additional pipelines
            for pipeline_id, pipeline_config in self._additional_pipelines.items():
                self._engine.add_pipeline(pipeline_id, pipeline_config, overwrite=True)
        
        return self._engine

    def add_pipeline(
        self,
        pipeline_id: str,
        pipeline_config: Dict[str, Any],
    ) -> None:
        """
        Add a transformation pipeline programmatically.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            pipeline_config: Pipeline configuration dictionary
        """
        self._additional_pipelines[pipeline_id] = pipeline_config
        
        # If engine already created, add directly
        if self._engine is not None:
            self._engine.add_pipeline(pipeline_id, pipeline_config, overwrite=True)

    def add_pipeline_from_builder(self, pipeline: Pipeline) -> None:
        """
        Add a transformation pipeline from a Pipeline builder.
        
        Args:
            pipeline: Pipeline builder instance
        """
        self.add_pipeline(pipeline.pipeline_id, pipeline.to_config())

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

    def _convert_datasets(
        self, 
        datasets: Optional[Dict[str, Any]]
    ) -> Dict[str, pl.DataFrame]:
        """
        Convert datasets to Polars DataFrames.
        
        Args:
            datasets: Dict of datasets for joins/lookups
            
        Returns:
            Dict of Polars DataFrames
        """
        if not datasets:
            return {}
        
        converted = {}
        for name, data in datasets.items():
            if isinstance(data, pl.DataFrame):
                converted[name] = data
            else:
                converted[name] = self._convert_input_to_dataframe(data)
        
        return converted

    def _format_step_results(self, result: TransformationResult) -> List[Dict[str, Any]]:
        """
        Format step results for the context.
        
        Args:
            result: Transformation result
            
        Returns:
            List of step result dictionaries
        """
        steps = []
        for step_result in result.step_results:
            steps.append({
                "step_name": step_result.step_name,
                "transformer_type": step_result.transformer_type,
                "success": step_result.success,
                "rows_in": step_result.rows_in,
                "rows_out": step_result.rows_out,
                "columns_in": step_result.columns_in,
                "columns_out": step_result.columns_out,
                "execution_time_ms": step_result.execution_time_ms,
                "error_message": step_result.error_message,
            })
        return steps

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute data transformation as part of the service pipeline.
        
        This method:
        1. Extracts input data from context
        2. Converts to Polars DataFrame if needed
        3. Runs the configured transformation pipeline
        4. Adds transformation results to context
        
        Args:
            context: Service pipeline context dictionary
            
        Returns:
            Updated context dictionary with transformation results
            
        Raises:
            ValueError: If pipeline_id not configured or input data missing
            DataTransformationError: If fail_on_error=True and transformation fails
        """
        # Call parent to set up logger
        super().execute(context)
        
        # Validate configuration
        if not self._pipeline_id:
            raise ValueError("pipeline_id must be configured for DataTransformationComponent")
        
        # Get input data
        input_data = context.get(self._input_key)
        if input_data is None:
            self.log_warning(f"No input data found at context key '{self._input_key}'")
            context["transformation_success"] = False
            context[self._result_key] = {"error": f"No input data at '{self._input_key}'"}
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
            context["transformation_success"] = False
            context[self._result_key] = {"error": f"Data conversion error: {str(e)}"}
            return context
        
        # Get additional datasets if available
        datasets = context.get(self._datasets_key)
        converted_datasets = self._convert_datasets(datasets)
        
        # Execute transformation
        self.log_info(f"Executing transformation pipeline '{self._pipeline_id}' on {len(df)} rows")
        
        engine = self._get_engine()
        
        try:
            result = engine.transform(
                pipeline_id=self._pipeline_id,
                data=df,
                datasets=converted_datasets,
                metadata={
                    "service_id": context.get("service_id"),
                    "request_id": context.get("request_id"),
                },
            )
        except Exception as e:
            self.log_error(f"Transformation pipeline failed with exception: {e}")
            context["transformation_success"] = False
            context[self._result_key] = {"error": str(e)}
            
            if self._fail_on_error:
                raise DataTransformationError(f"Transformation failed: {e}") from e
            return context
        
        # Update context with results
        context["transformation_success"] = result.success
        context[self._result_key] = {
            "pipeline_id": result.pipeline_id,
            "success": result.success,
            "rows_in": result.rows_in,
            "rows_out": result.rows_out,
            "execution_time_ms": result.total_execution_time_ms,
            "steps": self._format_step_results(result),
            "error_message": result.error_message,
        }
        
        if result.success:
            # Store transformed data
            context[self._output_key] = result.data
            
            self.log_info(
                f"Transformation succeeded: {result.rows_in} rows in, "
                f"{result.rows_out} rows out, {result.total_execution_time_ms:.2f}ms"
            )
        else:
            self.log_warning(
                f"Transformation failed at step: {result.error_message}"
            )
            
            # Optionally raise exception
            if self._fail_on_error:
                raise DataTransformationError(
                    f"Transformation failed: {result.error_message}"
                )
        
        return context


class DataTransformationError(Exception):
    """Exception raised when transformation fails and fail_on_error is True."""
    pass
