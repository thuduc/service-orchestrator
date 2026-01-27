"""ConcatTransformer - Concatenate with other DataFrames from context."""

from typing import Any, Dict, List, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError, TransformationError


class ConcatTransformer(BaseTransformer):
    """
    Concatenate with other DataFrames from context.
    
    Configuration:
        datasets: List of dataset names to concatenate
        how: "vertical" (stack rows) or "horizontal" (stack columns)
        rechunk: Reallocate memory after concat (default: True)
        
    Example configs:
        {"datasets": ["df2", "df3"], "how": "vertical"}
        {"datasets": ["features"], "how": "horizontal"}
    """
    
    @property
    def transformer_type(self) -> str:
        return "concat"
    
    @property
    def input_type(self) -> str:
        return "multi"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Concatenate with other datasets from context."""
        datasets = self._get_required("datasets", list)
        how = self._get_optional("how", "vertical", str)
        rechunk = self._get_optional("rechunk", True, bool)
        
        # Collect DataFrames
        dfs = [data]
        for ds_name in datasets:
            df = context.get_dataset(ds_name)
            if df is None:
                raise TransformationError(
                    f"Dataset '{ds_name}' not found in context"
                )
            dfs.append(df)
        
        if how == "vertical":
            return pl.concat(dfs, how="vertical", rechunk=rechunk)
        elif how == "horizontal":
            return pl.concat(dfs, how="horizontal", rechunk=rechunk)
        else:
            raise ConfigurationError(
                f"Invalid concat direction '{how}'. Use 'vertical' or 'horizontal'"
            )
    
    def get_required_datasets(self) -> List[str]:
        """Return list of required datasets."""
        return self.config.get("datasets", [])
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the concat configuration."""
        if "datasets" not in config:
            return "ConcatTransformer requires 'datasets' configuration"
        if not isinstance(config["datasets"], list):
            return "'datasets' must be a list"
        return None
