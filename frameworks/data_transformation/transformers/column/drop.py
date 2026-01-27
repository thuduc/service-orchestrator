"""DropTransformer - Drop specific columns from DataFrame."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class DropTransformer(BaseTransformer):
    """
    Drop specific columns from the DataFrame.
    
    Configuration:
        columns: List of column names to drop
        
    Example config:
        {"columns": ["temp_col", "debug_info"]}
    """
    
    @property
    def transformer_type(self) -> str:
        return "drop"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Drop specified columns from the DataFrame."""
        columns = self._get_required("columns", list)
        return data.drop(columns)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the drop configuration."""
        if "columns" not in config:
            return "DropTransformer requires 'columns' configuration"
        if not isinstance(config["columns"], list):
            return "'columns' must be a list"
        return None
