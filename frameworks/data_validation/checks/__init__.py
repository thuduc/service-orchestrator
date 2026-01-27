"""Custom checks for the Data Validation Framework."""

from frameworks.data_validation.checks.base_check import BaseCheck
from frameworks.data_validation.checks.builtin import (
    # String checks
    ValidEmailDomainCheck,
    NonEmptyStringCheck,
    StringPatternCheck,
    # Numeric checks
    WorkingAgeCheck,
    PositiveNumberCheck,
    SumEqualsCheck,
    PercentageRangeCheck,
    # Date checks
    FutureDateCheck,
    DateOrderCheck,
    DateInRangeCheck,
    # Business checks
    ConditionalRequiredCheck,
    UniqueCombinationCheck,
    # Referential checks
    ExistsInCheck,
    ExistsInWithConditionCheck,
)

__all__ = [
    "BaseCheck",
    # String checks
    "ValidEmailDomainCheck",
    "NonEmptyStringCheck",
    "StringPatternCheck",
    # Numeric checks
    "WorkingAgeCheck",
    "PositiveNumberCheck",
    "SumEqualsCheck",
    "PercentageRangeCheck",
    # Date checks
    "FutureDateCheck",
    "DateOrderCheck",
    "DateInRangeCheck",
    # Business checks
    "ConditionalRequiredCheck",
    "UniqueCombinationCheck",
    # Referential checks
    "ExistsInCheck",
    "ExistsInWithConditionCheck",
]
