"""Referential validation checks for cross-dataset validation."""

from typing import Any, List, Optional

import polars as pl

from frameworks.data_validation.contract.check import CheckResult
from frameworks.data_validation.checks.base_check import BaseCheck


class ExistsInCheck(BaseCheck):
    """Validates that values exist in a reference dataset."""

    def __init__(
        self,
        reference_data: pl.DataFrame,
        reference_column: str,
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the exists-in check.
        
        Args:
            reference_data: Reference DataFrame containing valid values
            reference_column: Column in reference dataset to match against
            allow_null: If True, null values are considered valid
        """
        self._reference_data = reference_data
        self._reference_column = reference_column
        self._allow_null = allow_null
        
        # Pre-compute the set of valid values for efficient lookup
        self._valid_values = set(
            self._reference_data[self._reference_column].drop_nulls().to_list()
        )

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that values exist in the reference dataset.
        
        Args:
            df: DataFrame to validate
            column: Column containing values to check
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        col_expr = pl.col(column)
        
        # Check if value is in the set of valid values
        exists = col_expr.is_in(list(self._valid_values))
        
        if self._allow_null:
            mask = exists | col_expr.is_null()
        else:
            mask = exists
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        # Deduplicate failing values
        unique_failing = list(set(failing_values))[:10]
        
        return self._create_failure(
            message=(
                f"Values in '{column}' must exist in reference column "
                f"'{self._reference_column}'"
            ),
            failure_cases=unique_failing,
            row_indices=failing_indices,
        )


class ExistsInWithConditionCheck(BaseCheck):
    """Validates that values exist in a reference dataset with additional conditions."""

    def __init__(
        self,
        reference_data: pl.DataFrame,
        reference_column: str,
        condition_column: Optional[str] = None,
        condition_value: Optional[Any] = None,
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the exists-in-with-condition check.
        
        Args:
            reference_data: Reference DataFrame containing valid values
            reference_column: Column in reference dataset to match against
            condition_column: Column in reference dataset to filter on
            condition_value: Value that condition_column must match
            allow_null: If True, null values are considered valid
        """
        self._reference_data = reference_data
        self._reference_column = reference_column
        self._condition_column = condition_column
        self._condition_value = condition_value
        self._allow_null = allow_null
        
        # Pre-compute the set of valid values with condition applied
        filtered_ref = reference_data
        if condition_column and condition_value is not None:
            filtered_ref = reference_data.filter(
                pl.col(condition_column) == condition_value
            )
        
        self._valid_values = set(
            filtered_ref[reference_column].drop_nulls().to_list()
        )

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that values exist in the filtered reference dataset.
        
        Args:
            df: DataFrame to validate
            column: Column containing values to check
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        col_expr = pl.col(column)
        
        # Check if value is in the set of valid values
        exists = col_expr.is_in(list(self._valid_values))
        
        if self._allow_null:
            mask = exists | col_expr.is_null()
        else:
            mask = exists
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        # Deduplicate failing values
        unique_failing = list(set(failing_values))[:10]
        
        condition_desc = ""
        if self._condition_column and self._condition_value is not None:
            condition_desc = f" where {self._condition_column}={self._condition_value}"
        
        return self._create_failure(
            message=(
                f"Values in '{column}' must exist in reference column "
                f"'{self._reference_column}'{condition_desc}"
            ),
            failure_cases=unique_failing,
            row_indices=failing_indices,
        )
