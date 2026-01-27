"""JoinTransformer - Join with another DataFrame from context."""

from typing import Any, Dict, List, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError, TransformationError


class JoinTransformer(BaseTransformer):
    """
    Join with another DataFrame from context.
    
    Configuration:
        right_dataset: Name of the dataset in context to join with
        on: Column(s) to join on (if same name in both)
        left_on: Column(s) from left DataFrame
        right_on: Column(s) from right DataFrame
        how: Join type ("inner", "left", "right", "outer", "semi", "anti", "cross")
        suffix: Suffix for duplicate column names from right (default: "_right")
        
    Example configs:
        {"right_dataset": "orders", "on": "customer_id", "how": "left"}
        {
            "right_dataset": "products",
            "left_on": "product_code",
            "right_on": "code",
            "how": "inner"
        }
    """
    
    VALID_JOIN_TYPES = {"inner", "left", "right", "outer", "full", "semi", "anti", "cross"}
    
    @property
    def transformer_type(self) -> str:
        return "join"
    
    @property
    def input_type(self) -> str:
        return "multi"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Join with another dataset from context."""
        right_dataset = self._get_required("right_dataset", str)
        on = self._get_optional("on", None)
        left_on = self._get_optional("left_on", None)
        right_on = self._get_optional("right_on", None)
        how = self._get_optional("how", "inner", str)
        suffix = self._get_optional("suffix", "_right", str)
        
        # Validate join type
        if how not in self.VALID_JOIN_TYPES:
            raise ConfigurationError(
                f"Invalid join type '{how}'. Allowed: {self.VALID_JOIN_TYPES}"
            )
        
        # Get right dataset from context
        right_df = context.get_dataset(right_dataset)
        if right_df is None:
            raise TransformationError(
                f"Dataset '{right_dataset}' not found in context"
            )
        
        # Validate join keys
        if on is None and (left_on is None or right_on is None):
            if how != "cross":
                raise ConfigurationError(
                    "Join requires either 'on' or both 'left_on' and 'right_on'"
                )
        
        return data.join(
            right_df,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffix=suffix,
        )
    
    def get_required_datasets(self) -> List[str]:
        """Return list of required datasets."""
        dataset = self.config.get("right_dataset")
        return [dataset] if dataset else []
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the join configuration."""
        if "right_dataset" not in config:
            return "JoinTransformer requires 'right_dataset' configuration"
        return None
