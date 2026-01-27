"""ExplodeTransformer - Explode list columns into separate rows."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class ExplodeTransformer(BaseTransformer):
    """
    Explode list columns into separate rows.
    
    Configuration:
        columns: Column name or list of columns to explode
        
    Example config:
        {"columns": "tags"}
        {"columns": ["tags", "categories"]}
    """
    
    @property
    def transformer_type(self) -> str:
        return "explode"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Explode list columns into separate rows."""
        columns = self._get_required("columns")
        
        # Normalize to list
        if isinstance(columns, str):
            columns = [columns]
        
        return data.explode(columns)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the explode configuration."""
        if "columns" not in config:
            return "ExplodeTransformer requires 'columns' configuration"
        return None
