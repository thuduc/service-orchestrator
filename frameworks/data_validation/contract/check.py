"""CustomCheck abstract base class for custom validation checks."""

from abc import ABC
from dataclasses import dataclass
from typing import Any, List, Optional

import polars as pl


@dataclass
class CheckResult:
    """Result of a custom check execution."""

    is_valid: bool
    message: str
    failure_cases: Optional[List[Any]] = None
    row_indices: Optional[List[int]] = None


class CustomCheck(ABC):
    """
    Abstract base class for custom validation checks.

    Note: The check's identity is determined by its registry key, not by a class
    attribute. This allows the same check class to be registered under multiple
    IDs with different default parameters if needed.
    """

    @property
    def description(self) -> str:
        """Human-readable description of what this check validates."""
        return ""

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate a single column.

        Override this method for column-level checks.
        """
        raise NotImplementedError("Column validation not implemented for this check")

    def validate_columns(self, df: pl.DataFrame, columns: List[str]) -> CheckResult:
        """
        Validate multiple columns together.

        Override this method for cross-field checks.
        """
        raise NotImplementedError(
            "Multi-column validation not implemented for this check"
        )

    def validate_dataframe(self, df: pl.DataFrame) -> CheckResult:
        """
        Validate the entire dataframe.

        Override this method for dataframe-level checks.
        """
        raise NotImplementedError("DataFrame validation not implemented for this check")
