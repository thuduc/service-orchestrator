"""BaseTransformer - Base class for transformer implementations."""

from typing import Any, Dict, Optional, Type, TYPE_CHECKING

from frameworks.data_transformation.contract.transformer import Transformer
from frameworks.data_transformation.exceptions import ConfigurationError

if TYPE_CHECKING:
    import polars as pl
    from frameworks.data_transformation.engine.transformation_context import (
        TransformationContext,
    )


class BaseTransformer(Transformer):
    """
    Base class for transformer implementations.
    
    Provides common utilities and default implementations.
    Subclasses should override transformer_type and transform().
    """
    
    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the transformer.
        
        Args:
            name: Instance name for this transformer step
            config: Configuration dictionary
        """
        self.name = name
        self.config = config
    
    @property
    def transformer_type(self) -> str:
        """Must be overridden by subclasses."""
        raise NotImplementedError("Subclasses must define transformer_type")
    
    def transform(
        self,
        data: "pl.DataFrame",
        context: "TransformationContext",
    ) -> "pl.DataFrame":
        """Must be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement transform()")
    
    def _get_required(self, key: str, type_: Optional[Type] = None) -> Any:
        """
        Get a required config value with optional type checking.
        
        Args:
            key: Configuration key
            type_: Optional type to validate against
            
        Returns:
            The configuration value
            
        Raises:
            ConfigurationError: If key is missing or type is wrong
        """
        if key not in self.config:
            raise ConfigurationError(
                f"{self.transformer_type}: missing required config '{key}'"
            )
        value = self.config[key]
        if type_ is not None and not isinstance(value, type_):
            raise ConfigurationError(
                f"{self.transformer_type}: '{key}' must be {type_.__name__}, "
                f"got {type(value).__name__}"
            )
        return value
    
    def _get_optional(
        self, 
        key: str, 
        default: Any = None,
        type_: Optional[Type] = None,
    ) -> Any:
        """
        Get an optional config value with optional type checking.
        
        Args:
            key: Configuration key
            default: Default value if not present
            type_: Optional type to validate against
            
        Returns:
            The configuration value or default
            
        Raises:
            ConfigurationError: If value is present but type is wrong
        """
        value = self.config.get(key, default)
        if value is not None and type_ is not None:
            if not isinstance(value, type_):
                raise ConfigurationError(
                    f"{self.transformer_type}: '{key}' must be {type_.__name__}, "
                    f"got {type(value).__name__}"
                )
        return value
