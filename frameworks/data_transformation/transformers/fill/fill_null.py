"""FillNullTransformer - Fill null values in columns."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError


class FillNullTransformer(BaseTransformer):
    """
    Fill null values in columns.
    
    Configuration:
        value: Literal value to fill nulls with
        strategy: Fill strategy ("forward", "backward", "min", "max", "mean", "zero", "one")
        columns: List of columns to fill (default: all columns with nulls)
        
    Note: Use either 'value' or 'strategy', not both.
        
    Example configs:
        {"value": 0}
        {"strategy": "forward", "columns": ["price"]}
        {"value": "unknown", "columns": ["category"]}
    """
    
    VALID_STRATEGIES = {"forward", "backward", "min", "max", "mean", "zero", "one"}
    
    @property
    def transformer_type(self) -> str:
        return "fill_null"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Fill null values."""
        value = self._get_optional("value", None)
        strategy = self._get_optional("strategy", None)
        columns = self._get_optional("columns", None)
        
        if value is None and strategy is None:
            raise ConfigurationError(
                "fill_null requires either 'value' or 'strategy'"
            )
        
        if value is not None and strategy is not None:
            raise ConfigurationError(
                "fill_null cannot have both 'value' and 'strategy'"
            )
        
        if strategy is not None and strategy not in self.VALID_STRATEGIES:
            raise ConfigurationError(
                f"Invalid strategy '{strategy}'. Allowed: {self.VALID_STRATEGIES}"
            )
        
        if columns is not None:
            # Fill specific columns
            if value is not None:
                exprs = [pl.col(c).fill_null(value) for c in columns]
            else:
                exprs = [pl.col(c).fill_null(strategy=strategy) for c in columns]
            return data.with_columns(exprs)
        else:
            # Fill all columns
            if value is not None:
                return data.fill_null(value)
            else:
                return data.fill_null(strategy=strategy)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the fill_null configuration."""
        value = config.get("value")
        strategy = config.get("strategy")
        
        if value is None and strategy is None:
            return "FillNullTransformer requires either 'value' or 'strategy'"
        if value is not None and strategy is not None:
            return "FillNullTransformer cannot have both 'value' and 'strategy'"
        
        return None
