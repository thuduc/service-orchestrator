"""Unit tests for built-in checks."""

from datetime import date, timedelta

import polars as pl
import pytest

from frameworks.data_validation.checks.builtin.string_checks import (
    ValidEmailDomainCheck,
    NonEmptyStringCheck,
    StringPatternCheck,
)
from frameworks.data_validation.checks.builtin.numeric_checks import (
    WorkingAgeCheck,
    PositiveNumberCheck,
    SumEqualsCheck,
    PercentageRangeCheck,
)
from frameworks.data_validation.checks.builtin.date_checks import (
    FutureDateCheck,
    DateOrderCheck,
    DateInRangeCheck,
)
from frameworks.data_validation.checks.builtin.business_checks import (
    ConditionalRequiredCheck,
    UniqueCombinationCheck,
)


class TestValidEmailDomainCheck:
    """Tests for ValidEmailDomainCheck."""

    def test_valid_emails_with_allowed_domains(self) -> None:
        """Test that valid emails with allowed domains pass."""
        df = pl.DataFrame({
            "email": ["user@gmail.com", "admin@company.com", "test@gmail.com"]
        })
        check = ValidEmailDomainCheck(allowed_domains=["gmail.com", "company.com"])
        
        result = check.validate_column(df, "email")
        
        assert result.is_valid

    def test_invalid_email_domain(self) -> None:
        """Test that emails with invalid domains fail."""
        df = pl.DataFrame({
            "email": ["user@gmail.com", "admin@unknown.com"]
        })
        check = ValidEmailDomainCheck(allowed_domains=["gmail.com", "company.com"])
        
        result = check.validate_column(df, "email")
        
        assert not result.is_valid
        assert "unknown.com" in str(result.failure_cases)

    def test_null_emails_allowed(self) -> None:
        """Test that null emails are allowed by default."""
        df = pl.DataFrame({
            "email": ["user@gmail.com", None, "test@gmail.com"]
        })
        check = ValidEmailDomainCheck(allowed_domains=["gmail.com"])
        
        result = check.validate_column(df, "email")
        
        assert result.is_valid


class TestNonEmptyStringCheck:
    """Tests for NonEmptyStringCheck."""

    def test_non_empty_strings_pass(self) -> None:
        """Test that non-empty strings pass."""
        df = pl.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
        check = NonEmptyStringCheck()
        
        result = check.validate_column(df, "name")
        
        assert result.is_valid

    def test_empty_string_fails(self) -> None:
        """Test that empty strings fail."""
        df = pl.DataFrame({"name": ["Alice", "", "Charlie"]})
        check = NonEmptyStringCheck()
        
        result = check.validate_column(df, "name")
        
        assert not result.is_valid

    def test_whitespace_only_fails_by_default(self) -> None:
        """Test that whitespace-only strings fail by default."""
        df = pl.DataFrame({"name": ["Alice", "   ", "Charlie"]})
        check = NonEmptyStringCheck()
        
        result = check.validate_column(df, "name")
        
        assert not result.is_valid

    def test_whitespace_allowed_when_configured(self) -> None:
        """Test that whitespace-only strings pass when allow_whitespace=True."""
        df = pl.DataFrame({"name": ["Alice", "   ", "Charlie"]})
        check = NonEmptyStringCheck(allow_whitespace=True)
        
        result = check.validate_column(df, "name")
        
        assert result.is_valid


class TestWorkingAgeCheck:
    """Tests for WorkingAgeCheck."""

    def test_valid_ages_pass(self) -> None:
        """Test that ages within range pass."""
        df = pl.DataFrame({"age": [25, 35, 45, 55]})
        check = WorkingAgeCheck(min_age=18, max_age=65)
        
        result = check.validate_column(df, "age")
        
        assert result.is_valid

    def test_age_below_minimum_fails(self) -> None:
        """Test that ages below minimum fail."""
        df = pl.DataFrame({"age": [15, 25, 35]})
        check = WorkingAgeCheck(min_age=18, max_age=65)
        
        result = check.validate_column(df, "age")
        
        assert not result.is_valid
        assert 15 in result.failure_cases

    def test_age_above_maximum_fails(self) -> None:
        """Test that ages above maximum fail."""
        df = pl.DataFrame({"age": [25, 35, 70]})
        check = WorkingAgeCheck(min_age=18, max_age=65)
        
        result = check.validate_column(df, "age")
        
        assert not result.is_valid
        assert 70 in result.failure_cases


class TestPositiveNumberCheck:
    """Tests for PositiveNumberCheck."""

    def test_positive_numbers_pass(self) -> None:
        """Test that positive numbers pass."""
        df = pl.DataFrame({"value": [1, 2, 3, 100]})
        check = PositiveNumberCheck()
        
        result = check.validate_column(df, "value")
        
        assert result.is_valid

    def test_zero_fails_by_default(self) -> None:
        """Test that zero fails by default."""
        df = pl.DataFrame({"value": [1, 0, 3]})
        check = PositiveNumberCheck()
        
        result = check.validate_column(df, "value")
        
        assert not result.is_valid

    def test_zero_passes_when_included(self) -> None:
        """Test that zero passes when include_zero=True."""
        df = pl.DataFrame({"value": [1, 0, 3]})
        check = PositiveNumberCheck(include_zero=True)
        
        result = check.validate_column(df, "value")
        
        assert result.is_valid

    def test_negative_numbers_fail(self) -> None:
        """Test that negative numbers fail."""
        df = pl.DataFrame({"value": [1, -2, 3]})
        check = PositiveNumberCheck()
        
        result = check.validate_column(df, "value")
        
        assert not result.is_valid


class TestSumEqualsCheck:
    """Tests for SumEqualsCheck."""

    def test_sum_equals_target_value(self) -> None:
        """Test that sum equals target value passes."""
        df = pl.DataFrame({
            "a": [10.0, 20.0, 30.0],
            "b": [5.0, 10.0, 15.0],
        })
        check = SumEqualsCheck(target_value=15.0)
        
        result = check.validate_columns(df, ["a", "b"])
        
        # First row: 10+5=15, second: 20+10=30, third: 30+15=45
        # Only first row matches
        assert not result.is_valid

    def test_sum_equals_target_column(self) -> None:
        """Test that sum equals target column passes."""
        df = pl.DataFrame({
            "a": [10.0, 20.0],
            "b": [5.0, 10.0],
            "total": [15.0, 30.0],
        })
        check = SumEqualsCheck(target_column="total")
        
        result = check.validate_columns(df, ["a", "b"])
        
        assert result.is_valid


class TestFutureDateCheck:
    """Tests for FutureDateCheck."""

    def test_past_dates_pass(self) -> None:
        """Test that past dates pass."""
        df = pl.DataFrame({
            "date": [
                date(2020, 1, 1),
                date(2021, 6, 15),
                date(2022, 12, 31),
            ]
        })
        check = FutureDateCheck()
        
        result = check.validate_column(df, "date")
        
        assert result.is_valid

    def test_future_dates_fail(self) -> None:
        """Test that future dates fail."""
        future_date = date.today() + timedelta(days=365)
        df = pl.DataFrame({
            "date": [date(2020, 1, 1), future_date]
        })
        check = FutureDateCheck()
        
        result = check.validate_column(df, "date")
        
        assert not result.is_valid


class TestDateOrderCheck:
    """Tests for DateOrderCheck."""

    def test_correct_date_order_passes(self) -> None:
        """Test that correct date ordering passes."""
        df = pl.DataFrame({
            "start_date": [date(2023, 1, 1), date(2023, 3, 1)],
            "end_date": [date(2023, 2, 1), date(2023, 4, 1)],
        })
        check = DateOrderCheck(comparison="before")
        
        result = check.validate_columns(df, ["start_date", "end_date"])
        
        assert result.is_valid

    def test_incorrect_date_order_fails(self) -> None:
        """Test that incorrect date ordering fails."""
        df = pl.DataFrame({
            "start_date": [date(2023, 1, 1), date(2023, 5, 1)],  # Second row: start > end
            "end_date": [date(2023, 2, 1), date(2023, 4, 1)],
        })
        check = DateOrderCheck(comparison="before", allow_equal=False)
        
        result = check.validate_columns(df, ["start_date", "end_date"])
        
        assert not result.is_valid


class TestConditionalRequiredCheck:
    """Tests for ConditionalRequiredCheck."""

    def test_required_field_present_when_condition_met(self) -> None:
        """Test that required field present when condition is met passes."""
        df = pl.DataFrame({
            "status": ["active", "inactive", "active"],
            "email": ["a@b.com", None, "c@d.com"],
        })
        check = ConditionalRequiredCheck(
            condition_column="status",
            condition_value="active",
        )
        
        result = check.validate_columns(df, ["status", "email"])
        
        assert result.is_valid

    def test_required_field_missing_when_condition_met_fails(self) -> None:
        """Test that missing required field when condition is met fails."""
        df = pl.DataFrame({
            "status": ["active", "inactive", "active"],
            "email": ["a@b.com", None, None],  # Third row: active but no email
        })
        check = ConditionalRequiredCheck(
            condition_column="status",
            condition_value="active",
        )
        
        result = check.validate_columns(df, ["status", "email"])
        
        assert not result.is_valid


class TestUniqueCombinationCheck:
    """Tests for UniqueCombinationCheck."""

    def test_unique_combinations_pass(self) -> None:
        """Test that unique combinations pass."""
        df = pl.DataFrame({
            "first_name": ["John", "Jane", "John"],
            "last_name": ["Doe", "Doe", "Smith"],
        })
        check = UniqueCombinationCheck()
        
        result = check.validate_columns(df, ["first_name", "last_name"])
        
        assert result.is_valid

    def test_duplicate_combinations_fail(self) -> None:
        """Test that duplicate combinations fail."""
        df = pl.DataFrame({
            "first_name": ["John", "Jane", "John"],
            "last_name": ["Doe", "Doe", "Doe"],  # John Doe appears twice
        })
        check = UniqueCombinationCheck()
        
        result = check.validate_columns(df, ["first_name", "last_name"])
        
        assert not result.is_valid
