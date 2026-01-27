"""Exception classes for the Data Transformation Framework."""

from typing import Optional


class TransformationError(Exception):
    """
    Base exception for transformation errors.
    
    Raised when a transformation fails during execution.
    """
    
    def __init__(
        self,
        message: str,
        step_name: Optional[str] = None,
        transformer_type: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        self.step_name = step_name
        self.transformer_type = transformer_type
        self.cause = cause
        
        full_message = message
        if step_name:
            full_message = f"[{step_name}] {full_message}"
        if transformer_type:
            full_message = f"({transformer_type}) {full_message}"
        
        super().__init__(full_message)


class ConfigurationError(TransformationError):
    """
    Exception for configuration errors.
    
    Raised when a transformer receives invalid configuration.
    """
    pass


class ExpressionParseError(TransformationError):
    """
    Exception for expression parsing errors.
    
    Raised when an expression string cannot be parsed into a Polars expression.
    """
    
    def __init__(
        self,
        message: str,
        expression: str,
        position: Optional[int] = None,
    ):
        self.expression = expression
        self.position = position
        
        full_message = f"{message}\nExpression: {expression}"
        if position is not None:
            full_message += f"\nPosition: {position}"
        
        super().__init__(full_message)


class DatasetNotFoundError(TransformationError):
    """
    Exception when a required dataset is not found in context.
    """
    
    def __init__(self, dataset_name: str, step_name: Optional[str] = None):
        self.dataset_name = dataset_name
        super().__init__(
            f"Dataset '{dataset_name}' not found in context",
            step_name=step_name,
        )


class PipelineNotFoundError(TransformationError):
    """
    Exception when a requested pipeline ID is not found.
    """
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        super().__init__(f"Pipeline '{pipeline_id}' not found")
