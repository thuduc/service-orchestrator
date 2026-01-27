"""CastTransformer - Cast columns to specified data types."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError


class CastTransformer(BaseTransformer):
    """
    Cast columns to specified data types.
    
    Configuration:
        schema: Dictionary mapping column names to target types
        strict: If True, raise on cast failure (default: True)
        
    Supported types: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
                     Float32, Float64, Boolean, Utf8, Date, Datetime, Time
        
    Example config:
        {"schema": {"age": "Int64", "price": "Float64", "active": "Boolean"}}
    """
    
    TYPE_MAP = {
        "Int8": pl.Int8, "Int16": pl.Int16, "Int32": pl.Int32, "Int64": pl.Int64,
        "UInt8": pl.UInt8, "UInt16": pl.UInt16, "UInt32": pl.UInt32, "UInt64": pl.UInt64,
        "Float32": pl.Float32, "Float64": pl.Float64,
        "Boolean": pl.Boolean, "Bool": pl.Boolean,
        "Utf8": pl.Utf8, "String": pl.Utf8,
        "Date": pl.Date, "Datetime": pl.Datetime, "Time": pl.Time,
    }
    
    @property
    def transformer_type(self) -> str:
        return "cast"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Cast columns to specified types."""
        schema = self._get_required("schema", dict)
        strict = self._get_optional("strict", True, bool)
        
        cast_exprs = []
        for col_name, type_name in schema.items():
            if type_name not in self.TYPE_MAP:
                raise ConfigurationError(
                    f"Unknown type '{type_name}' for column '{col_name}'. "
                    f"Supported types: {list(self.TYPE_MAP.keys())}"
                )
            cast_exprs.append(
                pl.col(col_name).cast(self.TYPE_MAP[type_name], strict=strict)
            )
        
        return data.with_columns(cast_exprs)
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the cast configuration."""
        if "schema" not in config:
            return "CastTransformer requires 'schema' configuration"
        if not isinstance(config["schema"], dict):
            return "'schema' must be a dictionary"
        
        schema = config["schema"]
        for col_name, type_name in schema.items():
            if type_name not in self.TYPE_MAP:
                return f"Unknown type '{type_name}' for column '{col_name}'"
        
        return None
