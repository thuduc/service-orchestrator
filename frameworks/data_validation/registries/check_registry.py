"""CheckRegistry - Registry for custom validation checks."""

from typing import Any, Dict, Optional, Type

from frameworks.data_validation.contract.check import CustomCheck


class CheckRegistry:
    """
    Registry for custom validation checks.

    Manages registration and lookup of custom check classes that can be
    referenced by ID in validation pipeline configurations.
    """

    def __init__(self) -> None:
        """Initialize an empty check registry."""
        self._checks: Dict[str, Type[CustomCheck]] = {}
        self._default_params: Dict[str, Dict[str, Any]] = {}

    def register(
        self,
        check_id: str,
        check_class: Type[CustomCheck],
        default_params: Optional[Dict[str, Any]] = None,
        overwrite: bool = False,
    ) -> None:
        """
        Register a custom check class.

        Args:
            check_id: Unique identifier for the check
            check_class: The check class (must be subclass of CustomCheck)
            default_params: Optional default parameters for the check
            overwrite: If True, replace existing registration

        Raises:
            ValueError: If check_id is already registered and overwrite is False
            TypeError: If check_class is not a subclass of CustomCheck
        """
        if check_id in self._checks and not overwrite:
            raise ValueError(
                f"Check '{check_id}' is already registered. "
                f"Use overwrite=True to replace it."
            )

        if not (isinstance(check_class, type) and issubclass(check_class, CustomCheck)):
            raise TypeError(
                f"check_class must be a subclass of CustomCheck, got {type(check_class)}"
            )

        self._checks[check_id] = check_class
        if default_params:
            self._default_params[check_id] = default_params

    def unregister(self, check_id: str) -> None:
        """
        Unregister a check.

        Args:
            check_id: ID of the check to unregister

        Raises:
            KeyError: If check_id is not registered
        """
        if check_id not in self._checks:
            raise KeyError(f"Check '{check_id}' is not registered")
        del self._checks[check_id]
        self._default_params.pop(check_id, None)

    def get(self, check_id: str) -> Optional[Type[CustomCheck]]:
        """
        Get a check class by ID.

        Args:
            check_id: ID of the check to retrieve

        Returns:
            The check class, or None if not found
        """
        return self._checks.get(check_id)

    def get_default_params(self, check_id: str) -> Dict[str, Any]:
        """
        Get default parameters for a check.

        Args:
            check_id: ID of the check

        Returns:
            Dictionary of default parameters (empty if none)
        """
        return self._default_params.get(check_id, {})

    def get_merged_params(
        self,
        check_id: str,
        rule_params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Merge default params with rule-level params.

        Rule-level params override check-level defaults.

        Args:
            check_id: ID of the check
            rule_params: Parameters from the rule configuration

        Returns:
            Merged parameters dictionary
        """
        default_params = self.get_default_params(check_id)
        return {**default_params, **rule_params}

    def has(self, check_id: str) -> bool:
        """
        Check if a check ID is registered.

        Args:
            check_id: ID to check

        Returns:
            True if registered, False otherwise
        """
        return check_id in self._checks

    def list_checks(self) -> list[str]:
        """
        List all registered check IDs.

        Returns:
            List of check ID strings
        """
        return list(self._checks.keys())

    def clear(self) -> None:
        """Remove all registered checks."""
        self._checks.clear()
        self._default_params.clear()

    def __len__(self) -> int:
        """Return the number of registered checks."""
        return len(self._checks)

    def __contains__(self, check_id: str) -> bool:
        """Check if a check ID is registered."""
        return check_id in self._checks
