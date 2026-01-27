"""SelectTransformer - Select specific columns from DataFrame."""

from typing import Any, Dict, List, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class SelectTransformer(BaseTransformer):
    """
    Select specific columns from the DataFrame.
    
    Configuration:
        columns: List of column names to select
        
    Example config:
        {"columns": ["id", "name", "email"]}
    """
    
    @property
    def transformer_type(self) -> str:
        return "select"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Select specified columns from the DataFrame."""
        columns = self._get_required("columns", list)
        return data.select(columns)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the select configuration."""
        if "columns" not in config:
            return "SelectTransformer requires 'columns' configuration"
        if not isinstance(config["columns"], list):
            return "'columns' must be a list"
        if len(config["columns"]) == 0:
            return "'columns' cannot be empty"
        return None
