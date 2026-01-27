"""Built-in custom checks for common validation scenarios."""

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
from frameworks.data_validation.checks.builtin.referential_checks import (
    ExistsInCheck,
    ExistsInWithConditionCheck,
)

__all__ = [
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
