"""HeadTransformer - Take the first n rows."""

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class HeadTransformer(BaseTransformer):
    """
    Take the first n rows.
    
    Configuration:
        n: Number of rows to take (default: 5)
        
    Example config:
        {"n": 100}
    """
    
    @property
    def transformer_type(self) -> str:
        return "head"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Take first n rows."""
        n = self._get_optional("n", 5, int)
        return data.head(n)
