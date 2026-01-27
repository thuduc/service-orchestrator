"""GroupByTransformer - Group by columns and aggregate."""

from typing import Any, Dict, List, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.engine.expression_parser import ExpressionParser
from frameworks.data_transformation.exceptions import ConfigurationError


class GroupByTransformer(BaseTransformer):
    """
    Group by columns and aggregate.
    
    Configuration:
        by: Column(s) to group by
        aggregations: Dictionary of output columns to aggregation specs
        maintain_order: Maintain order of groups (default: True)
        
    Aggregation specs can be:
        - String shorthand: "sum", "mean", "count", "min", "max", etc.
        - Dict: {"column": "amount", "agg": "sum"}
        - Expression string: "col('amount').sum()"
        
    Example config:
        {
            "by": ["customer_id"],
            "aggregations": {
                "total_orders": "count",
                "total_spent": {"column": "amount", "agg": "sum"},
                "avg_order": {"column": "amount", "agg": "mean"},
                "first_order": {"column": "order_date", "agg": "min"},
                "last_order": {"column": "order_date", "agg": "max"}
            }
        }
    """
    
    AGGREGATION_FUNCTIONS = {
        "sum": lambda c: pl.col(c).sum(),
        "mean": lambda c: pl.col(c).mean(),
        "avg": lambda c: pl.col(c).mean(),
        "min": lambda c: pl.col(c).min(),
        "max": lambda c: pl.col(c).max(),
        "count": lambda c: pl.col(c).count(),
        "n_unique": lambda c: pl.col(c).n_unique(),
        "first": lambda c: pl.col(c).first(),
        "last": lambda c: pl.col(c).last(),
        "std": lambda c: pl.col(c).std(),
        "var": lambda c: pl.col(c).var(),
        "median": lambda c: pl.col(c).median(),
    }
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        super().__init__(name, config)
        self._parser = ExpressionParser()
    
    @property
    def transformer_type(self) -> str:
        return "group_by"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Group by columns and aggregate."""
        by = self._get_required("by")
        aggregations = self._get_required("aggregations", dict)
        maintain_order = self._get_optional("maintain_order", True, bool)
        
        # Normalize by to list
        if isinstance(by, str):
            by = [by]
        
        # Build aggregation expressions
        agg_exprs = []
        for output_name, agg_spec in aggregations.items():
            expr = self._parse_aggregation(agg_spec, output_name)
            agg_exprs.append(expr)
        
        return data.group_by(by, maintain_order=maintain_order).agg(agg_exprs)
    
    def _parse_aggregation(
        self, 
        agg_spec: Any, 
        output_name: str,
    ) -> pl.Expr:
        """Parse an aggregation specification into a Polars expression."""
        if isinstance(agg_spec, str):
            # Check if it's a shorthand (just the function name)
            if agg_spec in self.AGGREGATION_FUNCTIONS:
                # For shorthand, use the output name as the column
                return self.AGGREGATION_FUNCTIONS[agg_spec](output_name).alias(output_name)
            else:
                # Try parsing as expression
                return self._parser.parse(agg_spec).alias(output_name)
        
        elif isinstance(agg_spec, dict):
            column = agg_spec.get("column")
            agg = agg_spec.get("agg")
            
            if column is None or agg is None:
                raise ConfigurationError(
                    "Aggregation dict must have 'column' and 'agg' keys"
                )
            
            if agg not in self.AGGREGATION_FUNCTIONS:
                raise ConfigurationError(
                    f"Unknown aggregation '{agg}'. "
                    f"Allowed: {list(self.AGGREGATION_FUNCTIONS.keys())}"
                )
            
            return self.AGGREGATION_FUNCTIONS[agg](column).alias(output_name)
        
        elif isinstance(agg_spec, pl.Expr):
            return agg_spec.alias(output_name)
        
        else:
            raise ConfigurationError(
                f"Invalid aggregation spec type: {type(agg_spec)}"
            )
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the group_by configuration."""
        if "by" not in config:
            return "GroupByTransformer requires 'by' configuration"
        if "aggregations" not in config:
            return "GroupByTransformer requires 'aggregations' configuration"
        if not isinstance(config["aggregations"], dict):
            return "'aggregations' must be a dictionary"
        return None
