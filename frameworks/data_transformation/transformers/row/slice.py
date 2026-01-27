"""SliceTransformer - Slice rows from offset with given length."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class SliceTransformer(BaseTransformer):
    """
    Slice rows from a given offset with a given length.
    
    Configuration:
        offset: Starting row index
        length: Number of rows to take
        
    Example config:
        {"offset": 10, "length": 50}
    """
    
    @property
    def transformer_type(self) -> str:
        return "slice"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Slice rows from offset with given length."""
        offset = self._get_required("offset", int)
        length = self._get_required("length", int)
        return data.slice(offset, length)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the slice configuration."""
        if "offset" not in config:
            return "SliceTransformer requires 'offset' configuration"
        if "length" not in config:
            return "SliceTransformer requires 'length' configuration"
        return None
