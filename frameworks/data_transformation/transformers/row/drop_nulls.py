"""DropNullsTransformer - Drop rows with null values."""

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class DropNullsTransformer(BaseTransformer):
    """
    Drop rows with null values.
    
    Configuration:
        subset: List of columns to check for nulls (default: all columns)
        
    Example configs:
        {"subset": ["required_field_1", "required_field_2"]}
        {}  # Check all columns
    """
    
    @property
    def transformer_type(self) -> str:
        return "drop_nulls"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Drop rows with null values."""
        subset = self._get_optional("subset", None)
        return data.drop_nulls(subset=subset)
