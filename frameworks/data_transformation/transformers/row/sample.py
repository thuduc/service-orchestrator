"""SampleTransformer - Sample rows from the DataFrame."""

from typing import Any, Dict, Optional

import polars as pl

from frameworks.data_transformation.transformers.base_transformer import BaseTransformer
from frameworks.data_transformation.engine.transformation_context import TransformationContext
from frameworks.data_transformation.exceptions import ConfigurationError


class SampleTransformer(BaseTransformer):
    """
    Sample rows from the DataFrame.
    
    Configuration:
        n: Number of rows to sample (mutually exclusive with fraction)
        fraction: Fraction of rows to sample (mutually exclusive with n)
        with_replacement: Allow sampling same row multiple times (default: False)
        shuffle: Shuffle the result (default: True)
        seed: Random seed for reproducibility
        
    Example configs:
        {"n": 100, "seed": 42}
        {"fraction": 0.1}
    """
    
    @property
    def transformer_type(self) -> str:
        return "sample"
    
    def transform(
        self, 
        data: pl.DataFrame, 
        context: TransformationContext,
    ) -> pl.DataFrame:
        """Sample rows from the DataFrame."""
        n = self._get_optional("n", None)
        fraction = self._get_optional("fraction", None)
        with_replacement = self._get_optional("with_replacement", False, bool)
        shuffle = self._get_optional("shuffle", True, bool)
        seed = self._get_optional("seed", None)
        
        if n is None and fraction is None:
            raise ConfigurationError("sample requires either 'n' or 'fraction'")
        if n is not None and fraction is not None:
            raise ConfigurationError("sample cannot have both 'n' and 'fraction'")
        
        return data.sample(
            n=n,
            fraction=fraction,
            with_replacement=with_replacement,
            shuffle=shuffle,
            seed=seed,
        )
    
    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """Validate the sample configuration."""
        n = config.get("n")
        fraction = config.get("fraction")
        
        if n is None and fraction is None:
            return "SampleTransformer requires either 'n' or 'fraction'"
        if n is not None and fraction is not None:
            return "SampleTransformer cannot have both 'n' and 'fraction'"
        
        return None
