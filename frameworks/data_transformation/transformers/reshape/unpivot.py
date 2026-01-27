"""UnpivotTransformer - Unpivot (melt) table from wide to long format."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext


class UnpivotTransformer(BaseTransformer):
    """
    Unpivot (melt) table from wide to long format.
    
    Configuration:
        on: Column(s) to unpivot (their names become values)
        index: Column(s) to keep as identifiers (optional)
        variable_name: Name for the new column containing original column names
        value_name: Name for the new column containing values
        
    Example config:
        {
            "on": ["jan_sales", "feb_sales", "mar_sales"],
            "index": ["product_id", "product_name"],
            "variable_name": "month",
            "value_name": "sales"
        }
    """
    
    @property
    def transformer_type(self) -> str:
        return "unpivot"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Unpivot (melt) table from wide to long format."""
        on = self._get_required("on", list)
        index = self._get_optional("index", None)
        variable_name = self._get_optional("variable_name", "variable", str)
        value_name = self._get_optional("value_name", "value", str)
        
        return data.unpivot(
            on=on,
            index=index,
            variable_name=variable_name,
            value_name=value_name,
        )
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the unpivot configuration."""
        if "on" not in config:
            return "UnpivotTransformer requires 'on' configuration"
        if not isinstance(config["on"], list):
            return "'on' must be a list"
        return None
