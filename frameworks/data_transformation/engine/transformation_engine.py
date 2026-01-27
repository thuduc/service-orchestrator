"""TransformationEngine - Main orchestration engine for transformation pipelines."""

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import polars as pl

from frameworks.data_transformation.contract.transformer import Transformer
from frameworks.data_transformation.contract.result import StepResult, TransformationResult
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.registries.transformer_registry import TransformerRegistry
from frameworks.data_transformation.exceptions import (
    ConfigurationError,
    DatasetNotFoundError,
    PipelineNotFoundError,
)

# Import all transformers
from frameworks.data_transformation.transformers import (
    SelectTransformer,
    DropTransformer,
    RenameTransformer,
    CastTransformer,
    WithColumnsTransformer,
    FilterTransformer,
    SortTransformer,
    UniqueTransformer,
    HeadTransformer,
    TailTransformer,
    SliceTransformer,
    SampleTransformer,
    DropNullsTransformer,
    PivotTransformer,
    UnpivotTransformer,
    ExplodeTransformer,
    GroupByTransformer,
    JoinTransformer,
    ConcatTransformer,
    UnionTransformer,
    FillNullTransformer,
    FillNanTransformer,
)


logger = logging.getLogger(__name__)


class TransformationEngine:
    """
    Main orchestration engine for transformation pipelines.
    
    Responsibilities:
    - Load pipeline configurations from JSON/YAML
    - Manage transformer registry
    - Execute pipelines step-by-step
    - Aggregate results
    
    Example usage:
        engine = TransformationEngine(
            pipeline_config_path="config/transformations.json"
        )
        
        result = engine.transform(
            pipeline_id="customer_enrichment",
            data=customers_df,
            datasets={"orders": orders_df}
        )
    """
    
    # Built-in transformer types
    BUILTIN_TRANSFORMERS: Dict[str, Type[Transformer]] = {
        # Column operations
        "select": SelectTransformer,
        "drop": DropTransformer,
        "rename": RenameTransformer,
        "cast": CastTransformer,
        "with_columns": WithColumnsTransformer,
        # Row operations
        "filter": FilterTransformer,
        "sort": SortTransformer,
        "unique": UniqueTransformer,
        "head": HeadTransformer,
        "tail": TailTransformer,
        "slice": SliceTransformer,
        "sample": SampleTransformer,
        "drop_nulls": DropNullsTransformer,
        # Reshape operations
        "pivot": PivotTransformer,
        "unpivot": UnpivotTransformer,
        "explode": ExplodeTransformer,
        # Aggregate operations
        "group_by": GroupByTransformer,
        # Combine operations
        "join": JoinTransformer,
        "concat": ConcatTransformer,
        "union": UnionTransformer,
        # Fill operations
        "fill_null": FillNullTransformer,
        "fill_nan": FillNanTransformer,
    }
    
    def __init__(
        self,
        pipeline_config_path: Optional[Union[str, Path]] = None,
    ) -> None:
        """
        Initialize the transformation engine.
        
        Args:
            pipeline_config_path: Path to pipeline configuration file
        """
        self._pipelines: Dict[str, Any] = {}
        self._transformer_registry = TransformerRegistry()
        
        # Register built-in transformers
        self._register_builtin_transformers()
        
        # Load configurations
        if pipeline_config_path:
            self._load_pipeline_config(pipeline_config_path)
    
    def _register_builtin_transformers(self) -> None:
        """Register built-in transformer types."""
        for transformer_type, transformer_class in self.BUILTIN_TRANSFORMERS.items():
            self._transformer_registry.register(transformer_type, transformer_class)
    
    def _load_pipeline_config(self, path: Union[str, Path]) -> None:
        """
        Load pipeline configurations from file.
        
        Args:
            path: Path to the pipelines configuration file
        """
        from frameworks.data_transformation.loaders.config_loader import ConfigLoader
        
        config = ConfigLoader.load(path)
        self._pipelines = config.get("pipelines", {})
        logger.info(f"Loaded {len(self._pipelines)} transformation pipelines from {path}")
    
    def transform(
        self,
        pipeline_id: str,
        data: pl.DataFrame,
        datasets: Optional[Dict[str, pl.DataFrame]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> TransformationResult:
        """
        Execute a transformation pipeline.
        
        Args:
            pipeline_id: ID of the pipeline to execute
            data: Primary DataFrame to transform
            datasets: Additional named DataFrames for joins/lookups
            metadata: Metadata to include in context
            
        Returns:
            TransformationResult with transformed data and execution info
        """
        start_time = time.time()
        step_results: List[StepResult] = []
        rows_in = len(data)
        current_data = data.clone()  # Clone to avoid mutating input
        
        # Validate pipeline exists
        if pipeline_id not in self._pipelines:
            raise PipelineNotFoundError(pipeline_id)
        
        pipeline_config = self._pipelines[pipeline_id]
        
        # Create context
        context = TransformationContext(
            data=current_data,
            datasets=datasets or {},
            metadata=metadata or {},
            pipeline_id=pipeline_id,
        )
        
        # Execute each step
        for step_config in pipeline_config.get("steps", []):
            step_name = step_config["name"]
            transformer_type = step_config["type"]
            config = step_config.get("config", {})
            
            step_start = time.time()
            rows_before = len(current_data)
            cols_before = len(current_data.columns)
            
            try:
                # Get transformer class
                transformer_class = self._transformer_registry.get(transformer_type)
                if transformer_class is None:
                    raise ConfigurationError(
                        f"Unknown transformer type: {transformer_type}",
                        step_name=step_name,
                    )
                
                # Instantiate transformer
                transformer = transformer_class(step_name, config)
                
                # Validate config
                validation_error = transformer.validate_config(config)
                if validation_error:
                    raise ConfigurationError(
                        validation_error,
                        step_name=step_name,
                        transformer_type=transformer_type,
                    )
                
                # Check required datasets
                required_datasets = transformer.get_required_datasets()
                for ds_name in required_datasets:
                    if ds_name and ds_name not in context.datasets:
                        raise DatasetNotFoundError(ds_name, step_name)
                
                # Execute transform
                context.current_step = step_name
                current_data = transformer.transform(current_data, context)
                context.data = current_data
                
                # Record success
                step_result = StepResult(
                    step_name=step_name,
                    transformer_type=transformer_type,
                    success=True,
                    rows_in=rows_before,
                    rows_out=len(current_data),
                    columns_in=cols_before,
                    columns_out=len(current_data.columns),
                    execution_time_ms=(time.time() - step_start) * 1000,
                )
                
            except Exception as e:
                # Record failure
                step_result = StepResult(
                    step_name=step_name,
                    transformer_type=transformer_type,
                    success=False,
                    rows_in=rows_before,
                    rows_out=rows_before,
                    columns_in=cols_before,
                    columns_out=cols_before,
                    execution_time_ms=(time.time() - step_start) * 1000,
                    error_message=str(e),
                )
                step_results.append(step_result)
                
                # Return failure result
                return TransformationResult(
                    pipeline_id=pipeline_id,
                    success=False,
                    data=None,
                    step_results=step_results,
                    total_execution_time_ms=(time.time() - start_time) * 1000,
                    rows_in=rows_in,
                    rows_out=rows_before,
                    error_message=str(e),
                )
            
            step_results.append(step_result)
        
        # Return success result
        return TransformationResult(
            pipeline_id=pipeline_id,
            success=True,
            data=current_data,
            step_results=step_results,
            total_execution_time_ms=(time.time() - start_time) * 1000,
            rows_in=rows_in,
            rows_out=len(current_data),
        )
    
    def add_pipeline(
        self,
        pipeline_id: str,
        pipeline_config: Dict[str, Any],
        overwrite: bool = False,
    ) -> None:
        """
        Add a pipeline configuration programmatically.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            pipeline_config: Pipeline configuration dictionary
            overwrite: If True, replace existing pipeline
            
        Raises:
            ValueError: If pipeline_id already exists and overwrite is False
        """
        if pipeline_id in self._pipelines and not overwrite:
            raise ValueError(
                f"Pipeline '{pipeline_id}' already exists. Use overwrite=True to replace."
            )
        
        self._pipelines[pipeline_id] = pipeline_config
        logger.info(f"Added pipeline '{pipeline_id}'")
    
    def register_transformer(
        self,
        transformer_type: str,
        transformer_class: Type[Transformer],
        overwrite: bool = False,
    ) -> None:
        """
        Register a custom transformer type.
        
        Args:
            transformer_type: Unique identifier for the transformer
            transformer_class: Class implementing Transformer ABC
            overwrite: If True, replace existing registration
        """
        self._transformer_registry.register(
            transformer_type, 
            transformer_class, 
            overwrite,
        )
    
    def list_pipelines(self) -> List[str]:
        """List all registered transformation pipelines."""
        return list(self._pipelines.keys())
    
    def list_transformers(self) -> List[str]:
        """List all registered transformer types."""
        return self._transformer_registry.list_transformers()
    
    def get_pipeline_config(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific pipeline."""
        return self._pipelines.get(pipeline_id)
