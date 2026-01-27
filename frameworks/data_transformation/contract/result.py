"""Result classes for transformation pipelines."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


@dataclass
class StepResult:
    """Result from a single transformation step."""
    
    step_name: str
    transformer_type: str
    success: bool
    rows_in: int
    rows_out: int
    columns_in: int
    columns_out: int
    execution_time_ms: float
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "step_name": self.step_name,
            "transformer_type": self.transformer_type,
            "success": self.success,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "columns_in": self.columns_in,
            "columns_out": self.columns_out,
            "execution_time_ms": self.execution_time_ms,
            "error_message": self.error_message,
        }


@dataclass
class TransformationResult:
    """Aggregated result from a transformation pipeline."""
    
    pipeline_id: str
    success: bool
    data: Optional["pl.DataFrame"]  # Final transformed data
    step_results: List[StepResult]
    total_execution_time_ms: float
    rows_in: int
    rows_out: int
    error_message: Optional[str] = None
    
    @property
    def steps_completed(self) -> int:
        """Number of steps that completed successfully."""
        return sum(1 for s in self.step_results if s.success)
    
    @property
    def total_steps(self) -> int:
        """Total number of steps in the pipeline."""
        return len(self.step_results)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "pipeline_id": self.pipeline_id,
            "success": self.success,
            "steps_completed": self.steps_completed,
            "total_steps": self.total_steps,
            "total_execution_time_ms": self.total_execution_time_ms,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "error_message": self.error_message,
            "step_results": [s.to_dict() for s in self.step_results],
        }
