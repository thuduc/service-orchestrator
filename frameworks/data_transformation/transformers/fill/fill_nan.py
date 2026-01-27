"""FillNanTransformer - Fill NaN values in floating-point columns."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class FillNanTransformer(BaseTransformer):
    """
    Fill NaN values in floating-point columns.
    
    Configuration:
        value: Value to replace NaN with
        columns: List of columns to fill (default: all float columns)
        
    Example configs:
        {"value": 0.0}
        {"value": 0.0, "columns": ["temperature", "humidity"]}
    """
    
    @property
    def transformer_type(self) -> str:
        return "fill_nan"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Fill NaN values."""
        value = self._get_required("value")
        columns = self._get_optional("columns", None)
        
        if columns is not None:
            exprs = [pl.col(c).fill_nan(value) for c in columns]
            return data.with_columns(exprs)
        else:
            return data.fill_nan(value)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the fill_nan configuration."""
        if "value" not in config:
            return "FillNanTransformer requires 'value' configuration"
        return None
