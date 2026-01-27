"""RenameTransformer - Rename columns in DataFrame."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class RenameTransformer(BaseTransformer):
    """
    Rename columns in the DataFrame.
    
    Configuration:
        mapping: Dictionary mapping old names to new names
        
    Example config:
        {"mapping": {"old_name": "new_name", "col1": "column_1"}}
    """
    
    @property
    def transformer_type(self) -> str:
        return "rename"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Rename columns according to the mapping."""
        mapping = self._get_required("mapping", dict)
        return data.rename(mapping)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the rename configuration."""
        if "mapping" not in config:
            return "RenameTransformer requires 'mapping' configuration"
        if not isinstance(config["mapping"], dict):
            return "'mapping' must be a dictionary"
        return None
