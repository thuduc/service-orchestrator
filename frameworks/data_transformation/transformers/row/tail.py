"""TailTransformer - Take the last n rows."""

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class TailTransformer(BaseTransformer):
    """
    Take the last n rows.
    
    Configuration:
        n: Number of rows to take (default: 5)
        
    Example config:
        {"n": 100}
    """
    
    @property
    def transformer_type(self) -> str:
        return "tail"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Take last n rows."""
        n = self._get_optional("n", 5, int)
        return data.tail(n)
