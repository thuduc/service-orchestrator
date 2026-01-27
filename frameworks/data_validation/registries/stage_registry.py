"""StageRegistry - Registry for validation stage types."""

from typing import Dict, Optional, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from frameworks.data_validation.contract.validation_stage import ValidationStage


class StageRegistry:
    """
    Registry for validation stage types.

    Manages registration and lookup of stage classes that can be used
    in validation pipelines.
    """

    def __init__(self) -> None:
        """Initialize an empty stage registry."""
        self._stages: Dict[str, Type["ValidationStage"]] = {}

    def register(
        self,
        stage_type: str,
        stage_class: Type["ValidationStage"],
        overwrite: bool = False,
    ) -> None:
        """
        Register a stage class.

        Args:
            stage_type: Unique identifier for the stage type
            stage_class: The stage class
            overwrite: If True, replace existing registration

        Raises:
            ValueError: If stage_type is already registered and overwrite is False
        """
        if stage_type in self._stages and not overwrite:
            raise ValueError(
                f"Stage type '{stage_type}' is already registered. "
                f"Use overwrite=True to replace it."
            )

        self._stages[stage_type] = stage_class

    def unregister(self, stage_type: str) -> None:
        """
        Unregister a stage type.

        Args:
            stage_type: Type of the stage to unregister

        Raises:
            KeyError: If stage_type is not registered
        """
        if stage_type not in self._stages:
            raise KeyError(f"Stage type '{stage_type}' is not registered")
        del self._stages[stage_type]

    def get(self, stage_type: str) -> Optional[Type["ValidationStage"]]:
        """
        Get a stage class by type.

        Args:
            stage_type: Type of the stage to retrieve

        Returns:
            The stage class, or None if not found
        """
        return self._stages.get(stage_type)

    def has(self, stage_type: str) -> bool:
        """
        Check if a stage type is registered.

        Args:
            stage_type: Type to check

        Returns:
            True if registered, False otherwise
        """
        return stage_type in self._stages

    def list_stages(self) -> list[str]:
        """
        List all registered stage types.

        Returns:
            List of stage type strings
        """
        return list(self._stages.keys())

    def clear(self) -> None:
        """Remove all registered stages."""
        self._stages.clear()

    def __len__(self) -> int:
        """Return the number of registered stages."""
        return len(self._stages)

    def __contains__(self, stage_type: str) -> bool:
        """Check if a stage type is registered."""
        return stage_type in self._stages
