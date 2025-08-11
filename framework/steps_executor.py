from typing import Dict, Any, List, Optional
import importlib
import logging

logger = logging.getLogger(__name__)


class StepsExecutor:
    """Executes a sequence of steps for a service."""
    
    def __init__(self, steps_config: List[Dict[str, Any]]):
        """
        Initialize the steps executor with service configuration.
        
        Args:
            steps_config: List of step configurations from services.json
        """
        self.steps_config = steps_config
        self.steps = []
        self._load_steps()
    
    def _load_steps(self):
        """Load and instantiate all step components."""
        for step_config in self.steps_config:
            step_name = step_config.get('name', 'unnamed')
            module_name = step_config['module']
            class_name = step_config['class']
            config = step_config.get('config', {})
            
            try:
                module = importlib.import_module(module_name)
                component_class = getattr(module, class_name)
                component_instance = component_class(config)
                
                self.steps.append({
                    'name': step_name,
                    'component': component_instance,
                    'on_error': step_config.get('on_error', 'fail_fast'),
                    'fallback_output': step_config.get('fallback_output', {})
                })
                
                logger.info(f"Loaded step '{step_name}': {module_name}.{class_name}")
            except (ImportError, AttributeError) as e:
                raise RuntimeError(f"Failed to load step '{step_name}': {e}")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute all steps in sequence.
        
        Args:
            context: Initial execution context
            
        Returns:
            The same context after all steps have executed (modified in place)
        """
        for i, step in enumerate(self.steps):
            step_name = step['name']
            logger.info(f"Executing step {i+1}/{len(self.steps)}: {step_name}")
            
            try:
                # Execute the component with the shared context
                # Each component modifies the context in place
                result = step['component'].execute(context)
                
                # Merge result back into context if it's a dictionary
                if isinstance(result, dict):
                    context.update(result)
                
                logger.info(f"Step '{step_name}' completed successfully")
                
            except Exception as e:
                logger.error(f"Step '{step_name}' failed: {e}")
                
                # Handle error based on configured strategy
                if step['on_error'] == 'fail_fast':
                    raise
                elif step['on_error'] == 'skip':
                    logger.warning(f"Skipping failed step '{step_name}'")
                    # Apply fallback output if configured
                    if step['fallback_output']:
                        context.update(step['fallback_output'])
                    continue
                elif step['on_error'] == 'compensate':
                    # TODO: Implement compensation logic in Phase 3
                    raise NotImplementedError("Compensation not yet implemented")
        
        return context