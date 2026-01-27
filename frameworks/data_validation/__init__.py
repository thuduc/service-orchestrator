"""
Data Validation Framework - A configuration-driven, multi-stage validation engine.

Built on top of Pandera with Polars as the DataFrame backend.
"""

from frameworks.data_validation.engine.validation_engine import ValidationEngine
from frameworks.data_validation.engine.validation_result import (
    ValidationResult,
    StageResult,
    ValidationError,
    ValidationWarning,
)
from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.contract.check import CustomCheck, CheckResult
from frameworks.data_validation.contract.validation_stage import ValidationStage
from frameworks.data_validation.contract.validator import Validator
from frameworks.data_validation.registries.check_registry import CheckRegistry
from frameworks.data_validation.registries.stage_registry import StageRegistry

__all__ = [
    # Main engine
    "ValidationEngine",
    # Results
    "ValidationResult",
    "StageResult",
    "ValidationError",
    "ValidationWarning",
    # Context
    "ValidationContext",
    # Contracts
    "CustomCheck",
    "CheckResult",
    "ValidationStage",
    "Validator",
    # Registries
    "CheckRegistry",
    "StageRegistry",
]
