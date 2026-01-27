"""SortTransformer - Sort rows by one or more columns."""

from typing import Any, Dict, List, Optional, Union

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class SortTransformer(BaseTransformer):
    """
    Sort rows by one or more columns.
    
    Configuration:
        by: Column name or list of column names to sort by
        descending: Boolean or list of booleans for sort direction
        nulls_last: If True, nulls appear last (default: False)
        
    Example configs:
        {"by": "date", "descending": true}
        {"by": ["category", "date"], "descending": [false, true]}
    """
    
    @property
    def transformer_type(self) -> str:
        return "sort"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Sort rows by specified columns."""
        by = self._get_required("by")
        descending = self._get_optional("descending", False)
        nulls_last = self._get_optional("nulls_last", False, bool)
        
        # Normalize to lists
        if isinstance(by, str):
            by = [by]
        if isinstance(descending, bool):
            descending = [descending] * len(by)
        
        return data.sort(by, descending=descending, nulls_last=nulls_last)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the sort configuration."""
        if "by" not in config:
            return "SortTransformer requires 'by' configuration"
        return None
