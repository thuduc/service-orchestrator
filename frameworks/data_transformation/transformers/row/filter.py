"""FilterTransformer - Filter rows based on a condition."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.engine.expression_parser import ExpressionParser
from frameworks.data_transformation.exceptions import ConfigurationError


class FilterTransformer(BaseTransformer):
    """
    Filter rows based on a condition.
    
    Configuration:
        condition: Filter condition (string expression or Polars expression)
        
    Example configs:
        {"condition": "col('age') >= 18"}
        {"condition": "col('status') == 'active'"}
        {"condition": "col('amount').is_not_null()"}
    """
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        super().__init__(name, config)
        self._parser = ExpressionParser()
    
    @property
    def transformer_type(self) -> str:
        return "filter"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Filter rows based on the condition."""
        condition = self._get_required("condition")
        
        if isinstance(condition, str):
            expr = self._parser.parse(condition)
        elif isinstance(condition, pl.Expr):
            expr = condition
        else:
            raise ConfigurationError(
                "filter condition must be string or Polars expression"
            )
        
        return data.filter(expr)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the filter configuration."""
        if "condition" not in config:
            return "FilterTransformer requires 'condition' configuration"
        return None
