"""Date validation checks."""

from datetime import date, datetime, timedelta
from typing import List, Optional, Union

import polars as pl

from frameworks.data_validation.contract.check import CheckResult
from frameworks.data_validation.checks.base_check import BaseCheck


class FutureDateCheck(BaseCheck):
    """Validates that date values are not in the future."""

    def __init__(
        self, 
        allow_today: bool = True,
        reference_date: Optional[Union[date, datetime]] = None,
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the future date check.
        
        Args:
            allow_today: If True, today's date is considered valid
            reference_date: Reference date for comparison (default: today)
            allow_null: If True, null values are considered valid
        """
        self._allow_today = allow_today
        self._reference_date = reference_date
        self._allow_null = allow_null

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that date values are not in the future.
        
        Args:
            df: DataFrame to validate
            column: Column containing date values
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        ref_date = self._reference_date or date.today()
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()
        
        col_expr = pl.col(column)
        
        # Cast to date if datetime
        if df[column].dtype == pl.Datetime:
            col_expr = col_expr.dt.date()
        
        if self._allow_today:
            not_future = col_expr <= ref_date
        else:
            not_future = col_expr < ref_date
        
        if self._allow_null:
            mask = not_future | pl.col(column).is_null()
        else:
            mask = not_future
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        today_clause = "including today" if self._allow_today else "before today"
        return self._create_failure(
            message=f"Dates must not be in the future ({today_clause})",
            failure_cases=[str(v) for v in failing_values],
            row_indices=failing_indices,
        )


class DateOrderCheck(BaseCheck):
    """Validates that one date column is before/after another."""

    def __init__(
        self, 
        comparison: str = "before",
        allow_equal: bool = True,
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the date order check.
        
        Args:
            comparison: Either "before" (first < second) or "after" (first > second)
            allow_equal: If True, dates can be equal
            allow_null: If True, null values are considered valid
        """
        if comparison not in ("before", "after"):
            raise ValueError("comparison must be 'before' or 'after'")
        
        self._comparison = comparison
        self._allow_equal = allow_equal
        self._allow_null = allow_null

    def validate_columns(self, df: pl.DataFrame, columns: List[str]) -> CheckResult:
        """
        Validate date ordering between two columns.
        
        Args:
            df: DataFrame to validate
            columns: Two columns to compare [first_date, second_date]
            
        Returns:
            CheckResult with validation outcome
        """
        if len(columns) != 2:
            return self._create_failure("DateOrderCheck requires exactly 2 columns")
        
        first_col, second_col = columns
        
        if first_col not in df.columns:
            return self._create_failure(f"Column '{first_col}' not found")
        if second_col not in df.columns:
            return self._create_failure(f"Column '{second_col}' not found")
        
        first_expr = pl.col(first_col)
        second_expr = pl.col(second_col)
        
        # Cast to date if datetime
        if df[first_col].dtype == pl.Datetime:
            first_expr = first_expr.dt.date()
        if df[second_col].dtype == pl.Datetime:
            second_expr = second_expr.dt.date()
        
        if self._comparison == "before":
            if self._allow_equal:
                order_check = first_expr <= second_expr
            else:
                order_check = first_expr < second_expr
        else:  # after
            if self._allow_equal:
                order_check = first_expr >= second_expr
            else:
                order_check = first_expr > second_expr
        
        if self._allow_null:
            mask = order_check | pl.col(first_col).is_null() | pl.col(second_col).is_null()
        else:
            mask = order_check
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        
        # Get pairs of failing values
        failing_df = df.filter(~valid_mask)
        first_vals = failing_df[first_col].to_list()[:10]
        second_vals = failing_df[second_col].to_list()[:10]
        failure_cases = [
            f"{first_col}={f}, {second_col}={s}" 
            for f, s in zip(first_vals, second_vals)
        ]
        
        comparison_op = "<=" if self._allow_equal else "<"
        if self._comparison == "after":
            comparison_op = ">=" if self._allow_equal else ">"
        
        return self._create_failure(
            message=f"'{first_col}' must be {self._comparison} '{second_col}' ({first_col} {comparison_op} {second_col})",
            failure_cases=failure_cases,
            row_indices=failing_indices,
        )


class DateInRangeCheck(BaseCheck):
    """Validates that date values are within a specified range."""

    def __init__(
        self,
        min_date: Optional[Union[date, datetime, str]] = None,
        max_date: Optional[Union[date, datetime, str]] = None,
        min_days_ago: Optional[int] = None,
        max_days_ago: Optional[int] = None,
        allow_null: bool = True,
    ) -> None:
        """
        Initialize the date range check.
        
        Args:
            min_date: Minimum allowed date (inclusive)
            max_date: Maximum allowed date (inclusive)
            min_days_ago: Minimum days ago from today
            max_days_ago: Maximum days ago from today
            allow_null: If True, null values are considered valid
            
        Note: Either provide min_date/max_date OR min_days_ago/max_days_ago
        """
        self._allow_null = allow_null
        
        # Convert string dates to date objects
        if isinstance(min_date, str):
            min_date = datetime.strptime(min_date, "%Y-%m-%d").date()
        if isinstance(max_date, str):
            max_date = datetime.strptime(max_date, "%Y-%m-%d").date()
        if isinstance(min_date, datetime):
            min_date = min_date.date()
        if isinstance(max_date, datetime):
            max_date = max_date.date()
        
        # Calculate dates from days_ago
        today = date.today()
        if min_days_ago is not None:
            max_date = today - timedelta(days=min_days_ago)
        if max_days_ago is not None:
            min_date = today - timedelta(days=max_days_ago)
        
        self._min_date = min_date
        self._max_date = max_date

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that date values are within the specified range.
        
        Args:
            df: DataFrame to validate
            column: Column containing date values
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        col_expr = pl.col(column)
        
        # Cast to date if datetime
        if df[column].dtype == pl.Datetime:
            col_expr = col_expr.dt.date()
        
        # Build range check
        conditions = []
        if self._min_date is not None:
            conditions.append(col_expr >= self._min_date)
        if self._max_date is not None:
            conditions.append(col_expr <= self._max_date)
        
        if not conditions:
            # No constraints, everything is valid
            return self._create_success()
        
        in_range = conditions[0]
        for cond in conditions[1:]:
            in_range = in_range & cond
        
        if self._allow_null:
            mask = in_range | pl.col(column).is_null()
        else:
            mask = in_range
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        range_desc = []
        if self._min_date:
            range_desc.append(f"after {self._min_date}")
        if self._max_date:
            range_desc.append(f"before {self._max_date}")
        
        return self._create_failure(
            message=f"Dates must be {' and '.join(range_desc)}",
            failure_cases=[str(v) for v in failing_values],
            row_indices=failing_indices,
        )
