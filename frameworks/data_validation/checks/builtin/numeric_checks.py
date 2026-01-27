"""Numeric validation checks."""

from typing import List, Optional

import polars as pl

from frameworks.data_validation.contract.check import CheckResult
from frameworks.data_validation.checks.base_check import BaseCheck


class WorkingAgeCheck(BaseCheck):
    """Validates that age values are within typical working age range."""

    def __init__(
        self, 
        min_age: int = 18, 
        max_age: int = 65,
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the working age check.
        
        Args:
            min_age: Minimum working age (default: 18)
            max_age: Maximum working age (default: 65)
            allow_null: If True, null values are considered valid
        """
        self._min_age = min_age
        self._max_age = max_age
        self._allow_null = allow_null

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that age values are within working age range.
        
        Args:
            df: DataFrame to validate
            column: Column containing age values
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        col_expr = pl.col(column)
        
        # Check if value is in range
        in_range = (col_expr >= self._min_age) & (col_expr <= self._max_age)
        
        if self._allow_null:
            mask = in_range | col_expr.is_null()
        else:
            mask = in_range
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        return self._create_failure(
            message=f"Age must be between {self._min_age} and {self._max_age}",
            failure_cases=failing_values,
            row_indices=failing_indices,
        )


class PositiveNumberCheck(BaseCheck):
    """Validates that numeric values are positive."""

    def __init__(
        self, 
        include_zero: bool = False,
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the positive number check.
        
        Args:
            include_zero: If True, zero is considered positive
            allow_null: If True, null values are considered valid
        """
        self._include_zero = include_zero
        self._allow_null = allow_null

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that values are positive.
        
        Args:
            df: DataFrame to validate
            column: Column to check
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        col_expr = pl.col(column)
        
        if self._include_zero:
            positive = col_expr >= 0
        else:
            positive = col_expr > 0
        
        if self._allow_null:
            mask = positive | col_expr.is_null()
        else:
            mask = positive
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        comparison = ">= 0" if self._include_zero else "> 0"
        return self._create_failure(
            message=f"Values must be positive ({comparison})",
            failure_cases=failing_values,
            row_indices=failing_indices,
        )


class SumEqualsCheck(BaseCheck):
    """Validates that the sum of specified columns equals a target value or column."""

    def __init__(
        self, 
        target_value: Optional[float] = None,
        target_column: Optional[str] = None,
        tolerance: float = 0.0001,
    ) -> None:
        """
        Initialize the sum equals check.
        
        Args:
            target_value: Fixed value that the sum should equal
            target_column: Column whose values the sum should equal
            tolerance: Tolerance for floating point comparison
            
        Note: Either target_value or target_column must be provided, not both.
        """
        if target_value is None and target_column is None:
            raise ValueError("Either target_value or target_column must be provided")
        if target_value is not None and target_column is not None:
            raise ValueError("Cannot provide both target_value and target_column")
        
        self._target_value = target_value
        self._target_column = target_column
        self._tolerance = tolerance

    def validate_columns(self, df: pl.DataFrame, columns: List[str]) -> CheckResult:
        """
        Validate that the sum of columns equals the target.
        
        Args:
            df: DataFrame to validate
            columns: Columns to sum together
            
        Returns:
            CheckResult with validation outcome
        """
        # Validate all columns exist
        missing = [c for c in columns if c not in df.columns]
        if missing:
            return self._create_failure(f"Columns not found: {missing}")
        
        # Calculate row-wise sum
        sum_expr = pl.lit(0.0)
        for col in columns:
            sum_expr = sum_expr + pl.col(col).fill_null(0)
        
        # Compare with target
        if self._target_value is not None:
            diff = (sum_expr - self._target_value).abs()
            mask = diff <= self._tolerance
        else:
            if self._target_column not in df.columns:
                return self._create_failure(
                    f"Target column '{self._target_column}' not found"
                )
            diff = (sum_expr - pl.col(self._target_column)).abs()
            mask = diff <= self._tolerance
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        
        # Get actual sums for failing rows
        failing_df = df.with_row_index("__idx__").filter(~valid_mask)
        actual_sums = failing_df.select(sum_expr.alias("sum"))["sum"].to_list()
        
        target_desc = (
            str(self._target_value) 
            if self._target_value is not None 
            else f"column '{self._target_column}'"
        )
        
        return self._create_failure(
            message=f"Sum of {columns} must equal {target_desc}",
            failure_cases=actual_sums[:10],
            row_indices=failing_indices,
        )


class PercentageRangeCheck(BaseCheck):
    """Validates that values are valid percentages (0-100 or 0-1)."""

    def __init__(
        self, 
        scale: str = "percent",
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the percentage range check.
        
        Args:
            scale: Either "percent" (0-100) or "decimal" (0-1)
            allow_null: If True, null values are considered valid
        """
        if scale not in ("percent", "decimal"):
            raise ValueError("scale must be 'percent' or 'decimal'")
        
        self._scale = scale
        self._allow_null = allow_null
        self._min = 0.0
        self._max = 100.0 if scale == "percent" else 1.0

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that values are valid percentages.
        
        Args:
            df: DataFrame to validate
            column: Column to check
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        col_expr = pl.col(column)
        
        in_range = (col_expr >= self._min) & (col_expr <= self._max)
        
        if self._allow_null:
            mask = in_range | col_expr.is_null()
        else:
            mask = in_range
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        return self._create_failure(
            message=f"Percentage values must be between {self._min} and {self._max} ({self._scale} scale)",
            failure_cases=failing_values,
            row_indices=failing_indices,
        )
