from typing import Dict, Any, List
from .interceptor import Interceptor
from .component import Component


class InterceptorPipeline:
    """
    Manages the interceptor execution pipeline.

    Interceptors are executed in order:
    1. All before() methods are called in order
    2. The component is executed
    3. All after() methods are called in reverse order
    4. If any error occurs, on_error() methods are called in reverse order
    """

    def __init__(self):
        """Initialize the interceptor pipeline"""
        self.interceptors: List[Interceptor] = []

    def add_interceptor(self, interceptor: Interceptor):
        """
        Add an interceptor to the pipeline

        Args:
            interceptor: The interceptor instance to add
        """
        self.interceptors.append(interceptor)

    def clear_interceptors(self):
        """Clear all interceptors from the pipeline"""
        self.interceptors = []

    def execute(self, context: Dict[str, Any], component: Component) -> Dict[str, Any]:
        """
        Execute the component through the interceptor pipeline

        Args:
            context: The request context
            component: The component to execute

        Returns:
            The response from the component execution
        """
        # Track which interceptors have had their before() called successfully
        # so we know which after() methods to call on cleanup
        executed_interceptors: List[Interceptor] = []

        try:
            # Phase 1: Execute all before() methods in order
            current_context = context
            for interceptor in self.interceptors:
                current_context = interceptor.before(current_context)
                if current_context is None:
                    # Interceptor short-circuited by returning None
                    raise InterceptorShortCircuit(
                        f"Interceptor {interceptor.__class__.__name__} short-circuited execution"
                    )
                executed_interceptors.append(interceptor)

            # Phase 2: Execute the component
            result = component.execute(current_context)

            # Phase 3: Execute all after() methods in reverse order
            for interceptor in reversed(executed_interceptors):
                result = interceptor.after(current_context, result)

            return result

        except InterceptorShortCircuit:
            # Short-circuit is a special case - don't call on_error
            raise

        except Exception as e:
            # Phase 4: Error handling - call on_error() in reverse order
            recovery_result = None

            for interceptor in reversed(executed_interceptors):
                try:
                    recovery_result = interceptor.on_error(current_context, e)
                    if recovery_result is not None:
                        # Interceptor recovered from the error
                        # Still call remaining after() methods with the recovery result
                        return self._execute_remaining_after(
                            executed_interceptors,
                            interceptor,
                            current_context,
                            recovery_result
                        )
                except Exception:
                    # on_error raised another exception, continue to next interceptor
                    pass

            # No interceptor recovered, re-raise the original exception
            raise

    def _execute_remaining_after(
        self,
        executed_interceptors: List[Interceptor],
        recovery_interceptor: Interceptor,
        context: Dict[str, Any],
        result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute remaining after() methods after error recovery.

        Args:
            executed_interceptors: List of interceptors that had before() called
            recovery_interceptor: The interceptor that recovered from the error
            context: The request context
            result: The recovery result

        Returns:
            The final result after all after() methods
        """
        # Find the index of the recovery interceptor
        recovery_index = executed_interceptors.index(recovery_interceptor)

        # Execute after() for interceptors before the recovery point (in reverse)
        for interceptor in reversed(executed_interceptors[:recovery_index]):
            result = interceptor.after(context, result)

        return result


class InterceptorShortCircuit(Exception):
    """Exception raised when an interceptor short-circuits execution"""
    pass
