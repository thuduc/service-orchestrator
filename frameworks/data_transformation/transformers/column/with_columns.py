"""WithColumnsTransformer - Add or modify columns using expressions."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.engine.expression_parser import ExpressionParser


class WithColumnsTransformer(BaseTransformer):
    """
    Add or modify columns using expressions.
    
    Configuration:
        columns: Dictionary mapping column names to expressions
        
    Expressions can be:
        - String expression: "col('price') * col('quantity')"
        - Literal value: 100, "default", True
        
    Example config:
        {
            "columns": {
                "full_name": "col('first_name') + ' ' + col('last_name')",
                "total": "col('price') * col('quantity')",
                "status": "'active'"
            }
        }
    """
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        super().__init__(name, config)
        self._parser = ExpressionParser()
    
    @property
    def transformer_type(self) -> str:
        return "with_columns"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Add or modify columns using expressions."""
        columns = self._get_required("columns", dict)
        
        exprs = []
        for col_name, expr_def in columns.items():
            if isinstance(expr_def, str):
                expr = self._parser.parse(expr_def)
            elif isinstance(expr_def, pl.Expr):
                expr = expr_def
            else:
                # Literal value
                expr = pl.lit(expr_def)
            exprs.append(expr.alias(col_name))
        
        return data.with_columns(exprs)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the with_columns configuration."""
        if "columns" not in config:
            return "WithColumnsTransformer requires 'columns' configuration"
        if not isinstance(config["columns"], dict):
            return "'columns' must be a dictionary"
        return None
