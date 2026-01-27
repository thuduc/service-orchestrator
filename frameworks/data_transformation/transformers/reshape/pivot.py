"""PivotTransformer - Pivot table from long to wide format."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError


class PivotTransformer(BaseTransformer):
    """
    Pivot table from long to wide format.
    
    Configuration:
        on: Column(s) whose values become new column names
        index: Column(s) to use as row identifiers
        values: Column(s) containing values to fill the matrix
        aggregate_function: How to aggregate if multiple values
                           ("first", "sum", "mean", "min", "max", "count")
        
    Example config:
        {
            "on": "month",
            "index": "product",
            "values": "sales",
            "aggregate_function": "sum"
        }
    """
    
    AGGREGATE_FUNCTIONS = {
        "first": "first",
        "last": "last",
        "sum": "sum",
        "mean": "mean",
        "min": "min",
        "max": "max",
        "count": "count",
        "len": "len",
    }
    
    @property
    def transformer_type(self) -> str:
        return "pivot"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Pivot table from long to wide format."""
        on = self._get_required("on")
        index = self._get_required("index")
        values = self._get_required("values")
        agg_fn = self._get_optional("aggregate_function", "first", str)
        
        if agg_fn not in self.AGGREGATE_FUNCTIONS:
            raise ConfigurationError(
                f"Unknown aggregate_function '{agg_fn}'. "
                f"Allowed: {list(self.AGGREGATE_FUNCTIONS.keys())}"
            )
        
        return data.pivot(
            on=on,
            index=index,
            values=values,
            aggregate_function=agg_fn,
        )
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the pivot configuration."""
        if "on" not in config:
            return "PivotTransformer requires 'on' configuration"
        if "index" not in config:
            return "PivotTransformer requires 'index' configuration"
        if "values" not in config:
            return "PivotTransformer requires 'values' configuration"
        return None
