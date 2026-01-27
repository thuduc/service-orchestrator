"""String validation checks."""

import re
from typing import List, Optional

import polars as pl

from frameworks.data_validation.contract.check import CheckResult
from frameworks.data_validation.checks.base_check import BaseCheck


class ValidEmailDomainCheck(BaseCheck):
    """Validates that email addresses have domains from an allowed list."""

    def __init__(self, allowed_domains: Optional[List[str]] = None) -> None:
        """
        Initialize the email domain check.
        
        Args:
            allowed_domains: List of allowed email domains (e.g., ["gmail.com", "company.com"])
                           If None, any domain is accepted
        """
        self._allowed_domains = allowed_domains

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that email values have allowed domains.
        
        Args:
            df: DataFrame to validate
            column: Column containing email addresses
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        # Extract domain from email
        domain_expr = pl.col(column).str.extract(r"@(.+)$", group_index=1)
        
        if self._allowed_domains:
            # Check if domain is in allowed list
            mask = domain_expr.is_in(self._allowed_domains) | pl.col(column).is_null()
        else:
            # Just check that there's a valid domain pattern
            mask = domain_expr.is_not_null() | pl.col(column).is_null()
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        if self._allowed_domains:
            allowed_str = ", ".join(self._allowed_domains)
            message = f"Email domains must be one of: {allowed_str}"
        else:
            message = "Invalid email format - no domain found"
        
        return self._create_failure(
            message=message,
            failure_cases=failing_values,
            row_indices=failing_indices,
        )


class NonEmptyStringCheck(BaseCheck):
    """Validates that string values are not empty or whitespace-only."""

    def __init__(self, allow_whitespace: bool = False) -> None:
        """
        Initialize the non-empty string check.
        
        Args:
            allow_whitespace: If True, whitespace-only strings are considered valid
        """
        self._allow_whitespace = allow_whitespace

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that string values are not empty.
        
        Args:
            df: DataFrame to validate
            column: Column to check
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        col_expr = pl.col(column)
        
        if self._allow_whitespace:
            # Only check for empty strings and null
            mask = (col_expr.str.len_chars() > 0) | col_expr.is_null()
        else:
            # Check for empty and whitespace-only strings
            mask = (col_expr.str.strip_chars().str.len_chars() > 0) | col_expr.is_null()
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        return self._create_failure(
            message="String values must not be empty",
            failure_cases=failing_values,
            row_indices=failing_indices,
        )


class StringPatternCheck(BaseCheck):
    """Validates that string values match a regex pattern."""

    def __init__(self, pattern: str, case_insensitive: bool = False) -> None:
        """
        Initialize the string pattern check.
        
        Args:
            pattern: Regex pattern to match against
            case_insensitive: If True, perform case-insensitive matching
        """
        self._pattern = pattern
        self._case_insensitive = case_insensitive
        
        # Validate the pattern
        try:
            flags = re.IGNORECASE if case_insensitive else 0
            re.compile(pattern, flags)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")

    def validate_column(self, df: pl.DataFrame, column: str) -> CheckResult:
        """
        Validate that string values match the pattern.
        
        Args:
            df: DataFrame to validate
            column: Column to check
            
        Returns:
            CheckResult with validation outcome
        """
        if column not in df.columns:
            return self._create_failure(f"Column '{column}' not found in DataFrame")
        
        # Polars doesn't have a direct case-insensitive flag for str.contains
        # We handle it by converting to lowercase if needed
        col_expr = pl.col(column)
        
        if self._case_insensitive:
            # Use (?i) flag in pattern for case insensitivity
            pattern_with_flag = f"(?i){self._pattern}"
            mask = col_expr.str.contains(pattern_with_flag) | col_expr.is_null()
        else:
            mask = col_expr.str.contains(self._pattern) | col_expr.is_null()
        
        valid_mask = df.select(mask).to_series()
        
        if valid_mask.all():
            return self._create_success()
        
        failing_indices = self._get_failing_indices(df, valid_mask)
        failing_values = self._get_failing_values(df, column, valid_mask)
        
        return self._create_failure(
            message=f"String values must match pattern: {self._pattern}",
            failure_cases=failing_values,
            row_indices=failing_indices,
        )
