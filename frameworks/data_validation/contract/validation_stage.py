"""ValidationStage abstract base class for validation stages."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.engine.validation_result import StageResult


class ValidationStage(ABC):
    """
    Abstract base class for validation stages.

    Each stage type (schema_validation, custom_rules, etc.) must implement
    this contract to be usable in validation pipelines.
    """

    @property
    @abstractmethod
    def stage_type(self) -> str:
        """
        Unique identifier for this stage type.

        This must match the 'type' field in pipeline configuration.
        Examples: 'schema_validation', 'custom_rules', 'cross_field_validation'
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Instance name for this stage.

        This is the name specified in the pipeline configuration.
        """
        pass

    @abstractmethod
    def execute(self, context: ValidationContext) -> StageResult:
        """
        Execute this validation stage.

        Args:
            context: The validation context containing data and metadata

        Returns:
            StageResult containing validation outcome for this stage
        """
        pass

    def validate_config(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Validate the stage configuration before execution.

        Override this method to add custom configuration validation.

        Args:
            config: The stage configuration dictionary

        Returns:
            None if valid, error message string if invalid
        """
        return None
