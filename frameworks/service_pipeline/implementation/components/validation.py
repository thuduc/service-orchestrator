from typing import Dict, Any
from ..base_component import BaseComponent


class ValidationComponent(BaseComponent):
    """Component for data validation"""
    
    def __init__(self, config: Dict[str, Any] | None = None):
        """
        Initialize the Validation component
        
        Args:
            config: Optional configuration parameters
        """
        super().__init__(config)
        self.required_fields = self.config.get('required_fields', [])
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate input data
        
        Args:
            context: Input context dictionary
            
        Returns:
            Dictionary with validation results (merged into context)
        """
        # Set up logger by calling parent's execute
        super().execute(context)
        
        errors = []
        
        # Check for required fields
        for field in self.required_fields:
            if field not in context:
                errors.append(f"Missing required field: {field}")
                self.log_warning(f"Validation: Missing required field '{field}'")
        
        # Validate data if present
        if 'data' in context:
            data = context['data']
            if not isinstance(data, dict):
                errors.append("Data must be a dictionary")
                self.log_warning("Validation: Data is not a dictionary")
            else:
                self.log_info(f"Validating data with {len(data)} fields")
        
        # Add validation results to context
        context['validation_passed'] = len(errors) == 0
        context['validation_errors'] = errors
        
        # Keep the validated data in context if validation passed
        if len(errors) == 0:
            if 'data' in context:
                context['validated_data'] = context['data']
            self.log_info("Validation passed successfully")
        else:
            self.log_error(f"Validation failed with {len(errors)} error(s)")
        
        return context
