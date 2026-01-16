from abc import ABC
from typing import Dict, Any, Optional


class Interceptor(ABC):
    """
    Base class for all interceptors.

    Interceptors provide hooks for cross-cutting concerns that execute
    before and/or after service component execution.

    Subclasses can override any of the three methods:
    - before(): Called before component execution
    - after(): Called after successful component execution
    - on_error(): Called when an error occurs during execution

    All methods have default implementations that pass through unchanged,
    so subclasses only need to override the methods they need.
    """

    def before(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Called before the component is executed.

        Args:
            context: The request context

        Returns:
            The (possibly modified) context to pass to the next interceptor/component.
            Return None or raise an exception to short-circuit execution.
        """
        return context

    def after(self, context: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Called after successful component execution.

        This method is guaranteed to be called after successful execution,
        even if earlier interceptors' after() methods raise exceptions.

        Args:
            context: The request context
            result: The result from the component execution

        Returns:
            The (possibly modified) result
        """
        return result

    def on_error(self, context: Dict[str, Any], error: Exception) -> Optional[Dict[str, Any]]:
        """
        Called when an error occurs during execution.

        Args:
            context: The request context
            error: The exception that was raised

        Returns:
            Optional recovery result. If None is returned, the error is re-raised.
            If a dict is returned, it's used as the result (error is suppressed).
        """
        return None  # Re-raise the error by default
