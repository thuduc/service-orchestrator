"""ValidationContext - Context passed through validation stages."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from frameworks.data_validation.engine.validation_result import StageResult


@dataclass
class ValidationContext:
    """Context passed through validation stages."""

    data: pl.DataFrame  # Primary data being validated (mutable)
    reference_data: Dict[str, pl.DataFrame] = field(default_factory=dict)  # Reference datasets for lookups
    metadata: Dict[str, Any] = field(default_factory=dict)  # Additional metadata
    stage_results: List["StageResult"] = field(default_factory=list)  # Results from completed stages
    current_stage: Optional[str] = None  # Currently executing stage
    pipeline_id: str = ""  # ID of the pipeline being executed

    def add_stage_result(self, result: "StageResult") -> None:
        """Add a stage result to the context."""
        self.stage_results.append(result)

    def get_validated_data(self) -> pl.DataFrame:
        """Get the data after all transformations/coercions."""
        return self.data
