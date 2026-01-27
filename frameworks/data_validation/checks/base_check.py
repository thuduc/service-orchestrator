"""Base class for custom validation checks."""

from typing import Any, List, Optional

import polars as pl

from frameworks.data_validation.contract.check import CheckResult, CustomCheck


class BaseCheck(CustomCheck):
    """
    Base class for implementing custom validation checks.
    
    Provides common utilities and default implementations for custom checks.
    Subclasses should override the appropriate validate_* methods.
    """

    @property
    def description(self) -> str:
        """Human-readable description of what this check validates."""
        return self.__class__.__doc__ or ""

    def _create_success(self) -> CheckResult:
        """Create a successful check result."""
        return CheckResult(
            is_valid=True,
            message="Check passed",
            failure_cases=None,
            row_indices=None,
        )

    def _create_failure(
        self,
        message: str,
        failure_cases: Optional[List[Any]] = None,
        row_indices: Optional[List[int]] = None,
    ) -> CheckResult:
        """
        Create a failure check result.
        
        Args:
            message: Error message describing the failure
            failure_cases: Sample of values that failed validation
            row_indices: Indices of rows that failed
            
        Returns:
            CheckResult indicating failure
        """
        return CheckResult(
            is_valid=False,
            message=message,
            failure_cases=failure_cases[:10] if failure_cases and len(failure_cases) > 10 else failure_cases,
            row_indices=row_indices,
        )

    def _get_failing_indices(
        self, 
        df: pl.DataFrame, 
        mask: pl.Series
    ) -> List[int]:
        """
        Get row indices where mask is False.
        
        Args:
            df: The DataFrame being validated
            mask: Boolean series where True = valid, False = invalid
            
        Returns:
            List of row indices that failed
        """
        # Create index column and filter
        with_idx = df.with_row_index("__idx__")
        failing = with_idx.filter(~mask)
        return failing["__idx__"].to_list()

    def _get_failing_values(
        self,
        df: pl.DataFrame,
        column: str,
        mask: pl.Series,
    ) -> List[Any]:
        """
        Get values from a column where mask is False.
        
        Args:
            df: The DataFrame being validated
            column: Column to get values from
            mask: Boolean series where True = valid, False = invalid
            
        Returns:
            List of failing values (limited to 10)
        """
        failing = df.filter(~mask)[column].to_list()
        return failing[:10] if len(failing) > 10 else failing
