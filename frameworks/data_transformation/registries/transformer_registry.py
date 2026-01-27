"""TransformerRegistry - Registry for transformer classes."""

from typing import Dict, List, Optional, Type

from frameworks.data_transformation.contract.transformer import Transformer


class TransformerRegistry:
    """
    Registry for transformer classes.
    
    Manages the mapping between transformer type identifiers and their
    implementing classes. Supports both built-in and custom transformers.
    """
    
    def __init__(self) -> None:
        """Initialize an empty transformer registry."""
        self._transformers: Dict[str, Type[Transformer]] = {}
    
    def register(
        self,
        transformer_type: str,
        transformer_class: Type[Transformer],
        overwrite: bool = False,
    ) -> None:
        """
        Register a transformer class.
        
        Args:
            transformer_type: Unique identifier for this transformer
            transformer_class: Class implementing Transformer ABC
            overwrite: If True, replace existing registration
            
        Raises:
            ValueError: If transformer_type already registered and not overwrite
            TypeError: If transformer_class doesn't implement Transformer
        """
        if transformer_type in self._transformers and not overwrite:
            raise ValueError(
                f"Transformer '{transformer_type}' already registered"
            )
        
        if not (isinstance(transformer_class, type) and 
                issubclass(transformer_class, Transformer)):
            raise TypeError(
                f"Expected Transformer subclass, got {type(transformer_class)}"
            )
        
        self._transformers[transformer_type] = transformer_class
    
    def get(self, transformer_type: str) -> Optional[Type[Transformer]]:
        """Get a transformer class by type."""
        return self._transformers.get(transformer_type)
    
    def has(self, transformer_type: str) -> bool:
        """Check if a transformer type is registered."""
        return transformer_type in self._transformers
    
    def list_transformers(self) -> List[str]:
        """List all registered transformer types."""
        return list(self._transformers.keys())
    
    def unregister(self, transformer_type: str) -> None:
        """Remove a transformer registration."""
        if transformer_type not in self._transformers:
            raise KeyError(f"Transformer '{transformer_type}' not registered")
        del self._transformers[transformer_type]
    
    def clear(self) -> None:
        """Remove all registrations."""
        self._transformers.clear()
    
    def __contains__(self, transformer_type: str) -> bool:
        """Check if a transformer type is registered."""
        return self.has(transformer_type)
    
    def __len__(self) -> int:
        """Return number of registered transformers."""
        return len(self._transformers)
