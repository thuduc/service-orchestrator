"""Business rule validation checks."""

from typing import Any, List, Optional

import polars as pl

from frameworks.data_validation.contract.check import CheckResult
from frameworks.data_validation.checks.base_check import BaseCheck


class ConditionalRequiredCheck(BaseCheck):
    """Validates that a field is required when a condition is met."""

    def __init__(
        self,
        condition_column: str,
        condition_value: Any,
        condition_operator: str = "equals",
    ) -> None:
        """
        Initialize the conditional required check.
        
        Args:
            condition_column: Column to check the condition on
            condition_value: Value that triggers the requirement
            condition_operator: Comparison operator ("equals", "not_equals", "in", "not_in")
        """
        valid_operators = ("equals", "not_equals", "in", "not_in")
        if condition_operator not in valid_operators:
            raise ValueError(f"condition_operator must be one of {valid_operators}")
        
        self._condition_column = condition_column
        self._condition_value = condition_value
        self._condition_operator = condition_operator

    def validate_columns(self, df: pl.DataFrame, columns: List[str]) -> CheckResult:
        """
        Validate that the target column is not null when condition is met.
        
        Args:
            df: DataFrame to validate
            columns: [condition_column, target_column] - the target column
                    that must be non-null when condition is met
            
        Returns:
            CheckResult with validation outcome
        """
        if len(columns) != 2:
            return self._create_failure(
                "ConditionalRequiredCheck requires exactly 2 columns: "
                "[condition_column, target_column]"
            )
        
        condition_col, target_col = columns
        
        if condition_col not in df.columns:
            return self._create_failure(f"Condition column '{condition_col}' not found")
        if target_col not in df.columns:
            return self._create_failure(f"Target column '{target_col}' not found")
        
        # Build condition expression
        cond_expr = pl.col(condition_col)
        
        if self._condition_operator == "equals":
            condition_met = cond_expr == self._condition_value
        elif self._condition_operator == "not_equals":
            condition_met = cond_expr != self._condition_value
        elif self._condition_operator == "in":
            condition_met = cond_expr.is_in(self._condition_value)
        else:  # not_in
            condition_met = ~cond_expr.is_in(self._condition_value)
        
        # When condition is met, target must not be null
        # Valid if: condition is not met OR (condition is met AND target is not null)
        target_not_null = pl.col(target_col).is_not_null()
        mask = ~condition_met | target_not_null
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        
        # Get failing row details
        failing_df = df.filter(~valid_mask)
        cond_values = failing_df[condition_col].to_list()[:10]
        
        return self._create_failure(
            message=(
                f"'{target_col}' is required when '{condition_col}' "
                f"{self._condition_operator} {self._condition_value}"
            ),
            failure_cases=[f"{condition_col}={v}, {target_col}=null" for v in cond_values],
            row_indices=failing_indices,
        )


class UniqueCombinationCheck(BaseCheck):
    """Validates that a combination of columns is unique across all rows."""

    def __init__(self, allow_null: bool = True) -> None:
        """
        Initialize the unique combination check.
        
        Args:
            allow_null: If True, null values are allowed but still counted for uniqueness
        """
        self._allow_null = allow_null

    def validate_columns(self, df: pl.DataFrame, columns: List[str]) -> CheckResult:
        """
        Validate that the combination of columns is unique.
        
        Args:
            df: DataFrame to validate
            columns: Columns that should form a unique combination
            
        Returns:
            CheckResult with validation outcome
        """
        if len(columns) < 1:
            return self._create_failure("At least one column is required")
        
        missing = [c for c in columns if c not in df.columns]
        if missing:
            return self._create_failure(f"Columns not found: {missing}")
        
        # Find duplicate combinations
        # First, add a count column for each unique combination
        grouped = (
            df.with_row_index("__idx__")
            .group_by(columns)
            .agg(
                pl.col("__idx__").alias("__indices__"),
                pl.len().alias("__count__")
            )
        )
        
        # Filter to combinations that appear more than once
        duplicates = grouped.filter(pl.col("__count__") > 1)
        
        if len(duplicates) == 0:
            return self._create_success()
        
        # Collect all indices that are part of duplicate combinations
        failing_indices = []
        for indices in duplicates["__indices__"].to_list():
            failing_indices.extend(indices)
        failing_indices = sorted(set(failing_indices))
        
        # Get sample of duplicate values
        duplicate_samples = []
        for row in duplicates.head(5).iter_rows(named=True):
            values = {col: row[col] for col in columns}
            count = row["__count__"]
            duplicate_samples.append(f"{values} appears {count} times")
        
        return self._create_failure(
            message=f"Combination of {columns} must be unique",
            failure_cases=duplicate_samples,
            row_indices=failing_indices,
        )
