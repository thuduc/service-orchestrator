"""
Data Transformation Framework

A configuration-driven, contract-based system for building and executing
data transformation pipelines using Polars as the underlying DataFrame engine.

Example usage:

    # Using the TransformationEngine with config file
    from frameworks.data_transformation import TransformationEngine
    
    engine = TransformationEngine(
        pipeline_config_path="config/transformations.yaml"
    )
    
    result = engine.transform(
        pipeline_id="customer_enrichment",
        data=customers_df,
        datasets={"orders": orders_df},
    )
    
    # Using the fluent Pipeline builder
    from frameworks.data_transformation import Pipeline
    import polars as pl
    
    pipeline = (
        Pipeline("my_pipeline")
        .filter(pl.col("status") == "active")
        .select(["id", "name", "email"])
        .sort(by="name")
    )
    
    result = pipeline.execute(data=my_df)
"""

from frameworks.data_transformation.contract.transformer import Transformer
from frameworks.data_transformation.contract.result import StepResult, TransformationResult
from frameworks.data_transformation.engine.transformation_engine import TransformationEngine
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.engine.expression_parser import ExpressionParser
from frameworks.data_transformation.engine.pipeline_builder import Pipeline
from frameworks.data_transformation.adapters.pipeline_adapter import DataTransformationComponent
from frameworks.data_transformation.registries.transformer_registry import TransformerRegistry
from frameworks.data_transformation.loaders.config_loader import ConfigLoader
from frameworks.data_transformation.exceptions import (
    TransformationError,
    ConfigurationError,
    ExpressionParseError,
    DatasetNotFoundError,
    PipelineNotFoundError,
)


def register_with_service_pipeline(
    component_registry,
    engine: TransformationEngine,
) -> None:
    """
    Register the DataTransformationComponent with Service Pipeline.
    
    Args:
        component_registry: Service Pipeline component registry
        engine: Configured TransformationEngine instance
    """
    def component_factory(name: str, config: dict):
        component = DataTransformationComponent(name, config, engine)
        return component
    
    component_registry.register(
        "data_transformation",
        component_factory,
    )


__all__ = [
    # Core contracts
    "Transformer",
    "StepResult",
    "TransformationResult",
    # Engine
    "TransformationEngine",
    "TransformationContext",
    "ExpressionParser",
    "Pipeline",
    # Adapters
    "DataTransformationComponent",
    "register_with_service_pipeline",
    # Registries
    "TransformerRegistry",
    # Loaders
    "ConfigLoader",
    # Exceptions
    "TransformationError",
    "ConfigurationError",
    "ExpressionParseError",
    "DatasetNotFoundError",
    "PipelineNotFoundError",
]
