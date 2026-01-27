"""UniqueTransformer - Remove duplicate rows."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class UniqueTransformer(BaseTransformer):
    """
    Remove duplicate rows.
    
    Configuration:
        subset: List of columns to consider for uniqueness (default: all)
        keep: Which duplicate to keep: "first", "last", "any", "none"
        maintain_order: Maintain original row order (default: True)
        
    Example configs:
        {"subset": ["customer_id"], "keep": "last"}
        {"keep": "first"}
    """
    
    @property
    def transformer_type(self) -> str:
        return "unique"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Remove duplicate rows."""
        subset = self._get_optional("subset", None)
        keep = self._get_optional("keep", "first", str)
        maintain_order = self._get_optional("maintain_order", True, bool)
        
        return data.unique(
            subset=subset,
            keep=keep,
            maintain_order=maintain_order,
        )
