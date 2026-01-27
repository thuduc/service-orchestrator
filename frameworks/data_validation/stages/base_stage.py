"""Base validation stage implementation."""

from abc import abstractmethod
from typing import Any, Dict, Optional

from frameworks.data_validation.contract.validation_stage import ValidationStage
from frameworks.data_validation.engine.validation_context import ValidationContext
from frameworks.data_validation.engine.validation_result import StageResult


class BaseValidationStage(ValidationStage):
    """
    Base implementation for validation stages.
    
    Provides common functionality for all validation stage types.
    Subclasses must implement the abstract methods and the execute method.
    """

    def __init__(self, name: str, config: Dict[str, Any]) -> None:
        """
        Initialize the base validation stage.
        
        Args:
            name: Instance name for this stage
            config: Stage configuration dictionary
        """
        self._name = name
        self._config = config
        self._fail_fast = config.get("fail_fast", False)

    @property
    def name(self) -> str:
        """Instance name for this stage."""
        return self._name

    @property
    @abstractmethod
    def stage_type(self) -> str:
        """Unique identifier for this stage type."""
        pass

    @abstractmethod
    def execute(self, context: ValidationContext) -> StageResult:
        """Execute this validation stage."""
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

    @property
    def fail_fast(self) -> bool:
        """Whether to stop executing rules after first failure."""
        return self._fail_fast
