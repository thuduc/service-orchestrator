"""TransformationContext - Context object passed through transformation pipeline."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    from frameworks.data_transformation.contract.result import StepResult


@dataclass
class TransformationContext:
    """
    Context object passed through the transformation pipeline.
    
    Contains the primary data being transformed, additional reference
    datasets, metadata, and intermediate results.
    
    Attributes:
        data: The primary DataFrame being transformed
        datasets: Named additional DataFrames (for joins, lookups, unions)
        metadata: Arbitrary metadata (request_id, timestamps, etc.)
        step_results: Results from each completed step (for debugging)
        current_step: Name of the currently executing step
        pipeline_id: ID of the pipeline being executed
    """
    data: "pl.DataFrame"
    datasets: Dict[str, "pl.DataFrame"] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    step_results: List["StepResult"] = field(default_factory=list)
    current_step: Optional[str] = None
    pipeline_id: Optional[str] = None
    
    def get_dataset(self, name: str) -> Optional["pl.DataFrame"]:
        """Get a named dataset from context."""
        return self.datasets.get(name)
    
    def set_dataset(self, name: str, df: "pl.DataFrame") -> None:
        """Store a named dataset in context."""
        self.datasets[name] = df
    
    def add_step_result(self, result: "StepResult") -> None:
        """Add a step result to the history."""
        self.step_results.append(result)
