"""Validation result classes for the Data Validation Framework."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import polars as pl


@dataclass
class ValidationError:
    """Individual validation error."""

    check_name: str
    column: Optional[str]
    error_message: str
    failure_cases: Optional[List[Any]] = None  # Sample of failing values
    row_indices: Optional[List[int]] = None  # Indices of failing rows


@dataclass
class ValidationWarning:
    """Individual validation warning (non-fatal issue)."""

    check_name: str
    column: Optional[str]
    message: str
    row_count: int = 0  # Number of affected rows (for dropped rows)


@dataclass
class StageResult:
    """Result from a single validation stage."""

    stage_name: str
    stage_type: str
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[ValidationWarning]
    execution_time_ms: float
    rows_validated: int
    rows_failed: int
    output_data: Optional[pl.DataFrame] = None  # Transformed data (only for coercing stages)


@dataclass
class ValidationResult:
    """Aggregated result from a validation pipeline."""

    pipeline_id: str
    is_valid: bool
    stage_results: List[StageResult]
    total_errors: int
    total_warnings: int
    rows_validated: int  # Total rows processed (final context.data.height)
    execution_time_ms: float
    validated_data: Optional[pl.DataFrame] = None  # Data after coercion (if applicable)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "is_valid": self.is_valid,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "rows_validated": self.rows_validated,
            "execution_time_ms": self.execution_time_ms,
            "stage_results": [
                {
                    "name": s.stage_name,
                    "type": s.stage_type,
                    "is_valid": s.is_valid,
                    "errors": [
                        {
                            "check_name": e.check_name,
                            "column": e.column,
                            "error_message": e.error_message,
                            "failure_cases": e.failure_cases,
                            "row_indices": e.row_indices,
                        }
                        for e in s.errors
                    ],
                    "warnings": [
                        {
                            "check_name": w.check_name,
                            "column": w.column,
                            "message": w.message,
                            "row_count": w.row_count,
                        }
                        for w in s.warnings
                    ],
                    "rows_validated": s.rows_validated,
                    "rows_failed": s.rows_failed,
                    "execution_time_ms": s.execution_time_ms,
                }
                for s in self.stage_results
            ],
        }

    def get_errors_by_stage(self) -> Dict[str, List[ValidationError]]:
        """Get errors grouped by stage."""
        return {
            stage.stage_name: stage.errors
            for stage in self.stage_results
            if stage.errors
        }

    def get_warnings_by_stage(self) -> Dict[str, List[ValidationWarning]]:
        """Get warnings grouped by stage."""
        return {
            stage.stage_name: stage.warnings
            for stage in self.stage_results
            if stage.warnings
        }
