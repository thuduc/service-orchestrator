"""UnionTransformer - Union with another DataFrame (vertical concat + unique)."""

from typing import Any, Dict, List, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import TransformationError


class UnionTransformer(BaseTransformer):
    """
    Union with another DataFrame (vertical concat + unique).
    
    This is equivalent to SQL UNION (not UNION ALL).
    
    Configuration:
        dataset: Name of dataset to union with
        
    Example config:
        {"dataset": "additional_records"}
    """
    
    @property
    def transformer_type(self) -> str:
        return "union"
    
    @property
    def input_type(self) -> str:
        return "multi"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Union with another dataset from context."""
        dataset = self._get_required("dataset", str)
        
        other_df = context.get_dataset(dataset)
        if other_df is None:
            raise TransformationError(
                f"Dataset '{dataset}' not found in context"
            )
        
        # Concat then unique
        return pl.concat([data, other_df], how="vertical").unique()
    
    def get_required_datasets(self) -> List[str]:
        """Return list of required datasets."""
        dataset = self.config.get("dataset")
        return [dataset] if dataset else []
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the union configuration."""
        if "dataset" not in config:
            return "UnionTransformer requires 'dataset' configuration"
        return None
