from typing import Dict, Any
from ..base_component import BaseComponent


class TransformationComponent(BaseComponent):
    """Component for data transformation"""
    
    def __init__(self, config: Dict[str, Any] | None = None):
        """
        Initialize the Transformation component
        
        Args:
            config: Optional configuration parameters
        """
        super().__init__(config)
        self.transform_type = self.config.get('transform_type', 'uppercase')
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform input data
        
        Args:
            context: Input context dictionary
            
        Returns:
            Updated context with transformed data
        """
        # Set up logger by calling parent's execute
        super().execute(context)
        
        # Use validated_data if available, otherwise use data
        data = context.get('validated_data', context.get('data', {}))
        
        self.log_info(f"Starting {self.transform_type} transformation")
        
        # Apply transformations based on type
        if self.transform_type == 'uppercase':
            transformed = self._uppercase_transform(data)
            self.log_info("Applied uppercase transformation")
        elif self.transform_type == 'normalize':
            transformed = self._normalize_transform(data)
            self.log_info("Applied normalization transformation")
        else:
            transformed = data
            self.log_info("No transformation applied (unknown type)")
        
        # Update context with transformation results
        context['transformed_data'] = transformed
        context['transform_type'] = self.transform_type
        context['original_keys'] = list(data.keys()) if isinstance(data, dict) else []
        
        # Also store as 'processed' for next steps
        context['processed'] = transformed
        
        if isinstance(data, dict) and isinstance(transformed, dict):
            self.log_info(f"Transformed {len(data)} fields into {len(transformed)} fields")
        
        return context
    
    def _uppercase_transform(self, data: Any) -> Any:
        """Transform string values to uppercase"""
        if isinstance(data, str):
            return data.upper()
        elif isinstance(data, dict):
            return {k: self._uppercase_transform(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._uppercase_transform(item) for item in data]
        else:
            return data
    
    def _normalize_transform(self, data: Any) -> Any:
        """Normalize data structure"""
        if isinstance(data, dict):
            # Flatten nested structures
            result = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        result[f"{key}_{sub_key}"] = sub_value
                else:
                    result[key] = value
            return result
        return data
