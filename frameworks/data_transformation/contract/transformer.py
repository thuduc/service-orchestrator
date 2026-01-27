"""Transformer abstract base class for data transformations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    from frameworks.data_transformation.engine.transformation_context import (
        TransformationContext,
    )


class Transformer(ABC):
    """
    Abstract base class for all transformers.
    
    A transformer is a single unit of data transformation that takes
    a DataFrame (and optionally additional context) and produces a
    transformed DataFrame.
    
    Transformers are stateless - all configuration is passed via the
    config dictionary during initialization.
    """
    
    @property
    @abstractmethod
    def transformer_type(self) -> str:
        """
        Unique identifier for this transformer type.
        
        This is used to register the transformer and reference it in
        configuration files.
        
        Returns:
            String identifier (e.g., "select", "filter", "join")
        """
        pass
    
    @property
    def input_type(self) -> str:
        """
        Type of input this transformer expects.
        
        Returns:
            "single" - expects one DataFrame (default)
            "multi" - expects multiple DataFrames (e.g., join, concat)
        """
        return "single"
    
    @abstractmethod
    def transform(
        self,
        data: "pl.DataFrame",
        context: "TransformationContext",
    ) -> "pl.DataFrame":
        """
        Execute the transformation.
        
        Args:
            data: Primary input DataFrame to transform
            context: Transformation context containing:
                - Additional datasets (for joins, lookups)
                - Metadata and configuration
                - Intermediate results from previous steps
        
        Returns:
            Transformed DataFrame
            
        Raises:
            TransformationError: If transformation fails
        """
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the transformer configuration.
        
        Override this method to add custom validation logic.
        
        Args:
            config: Configuration dictionary for this transformer
            
        Returns:
            None if valid, error message string if invalid
        """
        return None
    
    def get_required_datasets(self) -> List[str]:
        """
        Return list of additional dataset names required from context.
        
        Override for transformers that need external datasets (e.g., join).
        
        Returns:
            List of dataset names that must be present in context
        """
        return []
