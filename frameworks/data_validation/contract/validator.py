"""Validator abstract base class for validation engines."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import polars as pl

from frameworks.data_validation.engine.validation_result import ValidationResult


class Validator(ABC):
    """
    Abstract base class for validation engines.

    Defines the core interface for validating DataFrames through pipelines.
    """

    @abstractmethod
    def validate(
        self,
        pipeline_id: str,
        data: pl.DataFrame,
        context: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """
        Execute a validation pipeline on the given data.

        Args:
            pipeline_id: ID of the validation pipeline to execute
            data: Polars DataFrame to validate
            context: Optional context data (e.g., reference datasets)

        Returns:
            ValidationResult containing all stage results
        """
        pass

    @abstractmethod
    def register_check(self, check_id: str, check_class: type) -> None:
        """
        Register a custom check class with the validator.

        Args:
            check_id: Unique identifier for the check
            check_class: The check class (must implement CustomCheck)
        """
        pass

    @abstractmethod
    def register_stage(self, stage_type: str, stage_class: type) -> None:
        """
        Register a custom stage type with the validator.

        Args:
            stage_type: Unique identifier for the stage type
            stage_class: The stage class (must implement ValidationStage)
        """
        pass

    @abstractmethod
    def list_pipelines(self) -> List[str]:
        """
        List all registered validation pipeline IDs.

        Returns:
            List of pipeline ID strings
        """
        pass

    @abstractmethod
    def list_checks(self) -> List[str]:
        """
        List all registered custom check IDs.

        Returns:
            List of check ID strings
        """
        pass
