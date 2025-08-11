from typing import Dict, Any
from framework.component import Component


class ValidationComponent(Component):
    """Component for data validation"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the Validation component
        
        Args:
            config: Optional configuration parameters
        """
        self.config = config or {}
        self.required_fields = self.config.get('required_fields', [])
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate input data
        
        Args:
            context: Input context dictionary
            
        Returns:
            Dictionary with validation results
        """
        errors = []
        
        # Check for required fields
        for field in self.required_fields:
            if field not in context:
                errors.append(f"Missing required field: {field}")
        
        # Validate data if present
        if 'data' in context:
            data = context['data']
            if not isinstance(data, dict):
                errors.append("Data must be a dictionary")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "validated_data": context.get('data', {}) if len(errors) == 0 else None
        }