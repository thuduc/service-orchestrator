from typing import Dict, Any, Optional
import copy
import logging

logger = logging.getLogger(__name__)


class StepContext:
    """Manages execution context across steps with isolation and tracking."""
    
    def __init__(self, initial_context: Dict[str, Any]):
        """
        Initialize the step context manager.
        
        Args:
            initial_context: Initial execution context
        """
        self.global_context = copy.deepcopy(initial_context)
        self.step_results = {}
        self.execution_history = []
    
    def get_step_input(self, step_name: str, input_mapping: Dict[str, str]) -> Dict[str, Any]:
        """
        Get input for a step based on input mapping.
        
        Args:
            step_name: Name of the step
            input_mapping: Mapping of input keys to context keys
            
        Returns:
            Input dictionary for the step
        """
        if not input_mapping:
            # No mapping, return copy of global context
            return copy.deepcopy(self.global_context)
        
        step_input = {}
        for input_key, context_key in input_mapping.items():
            value = self._get_nested_value(self.global_context, context_key)
            if value is not None:
                step_input[input_key] = copy.deepcopy(value)
            else:
                logger.warning(f"Step '{step_name}': Input key '{context_key}' not found in context")
        
        return step_input
    
    def update_with_step_output(self, step_name: str, output: Dict[str, Any], 
                               output_mapping: Dict[str, str]) -> None:
        """
        Update context with step output based on output mapping.
        
        Args:
            step_name: Name of the step
            output: Output from step execution
            output_mapping: Mapping of output keys to context keys
        """
        # Store raw step result
        self.step_results[step_name] = copy.deepcopy(output)
        
        # Record in execution history
        self.execution_history.append({
            'step': step_name,
            'output_keys': list(output.keys()) if output else []
        })
        
        if not output_mapping:
            # No mapping, merge entire output into context
            self.global_context.update(output)
        else:
            # Apply output mapping
            for output_key, context_key in output_mapping.items():
                if output_key in output:
                    self._set_nested_value(
                        self.global_context, 
                        context_key, 
                        copy.deepcopy(output[output_key])
                    )
                else:
                    logger.warning(f"Step '{step_name}': Output key '{output_key}' not found in step output")
    
    def get_final_context(self) -> Dict[str, Any]:
        """
        Get the final execution context.
        
        Returns:
            Final context with all step results
        """
        return copy.deepcopy(self.global_context)
    
    def get_step_result(self, step_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the raw result from a specific step.
        
        Args:
            step_name: Name of the step
            
        Returns:
            Step result or None if step hasn't executed
        """
        return self.step_results.get(step_name)
    
    def _get_nested_value(self, data: Dict[str, Any], key_path: str) -> Any:
        """
        Get a value from nested dictionary using dot notation.
        
        Args:
            data: Dictionary to search
            key_path: Dot-separated path (e.g., "user.profile.name")
            
        Returns:
            Value at the path or None if not found
        """
        keys = key_path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        
        return current
    
    def _set_nested_value(self, data: Dict[str, Any], key_path: str, value: Any) -> None:
        """
        Set a value in nested dictionary using dot notation.
        
        Args:
            data: Dictionary to modify
            key_path: Dot-separated path (e.g., "user.profile.name")
            value: Value to set
        """
        keys = key_path.split('.')
        current = data
        
        # Navigate to the parent of the target
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        # Set the final value
        current[keys[-1]] = value
    
    def create_snapshot(self) -> Dict[str, Any]:
        """
        Create a snapshot of the current context state.
        
        Returns:
            Snapshot containing context, results, and history
        """
        return {
            'global_context': copy.deepcopy(self.global_context),
            'step_results': copy.deepcopy(self.step_results),
            'execution_history': copy.deepcopy(self.execution_history)
        }