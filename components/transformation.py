from typing import Dict, Any
from framework.component import Component


class TransformationComponent(Component):
    """Component for data transformation"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the Transformation component
        
        Args:
            config: Optional configuration parameters
        """
        self.config = config or {}
        self.transform_type = self.config.get('transform_type', 'uppercase')
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform input data
        
        Args:
            context: Input context dictionary
            
        Returns:
            Dictionary with transformed data
        """
        data = context.get('data', {})
        
        # Apply transformations based on type
        if self.transform_type == 'uppercase':
            transformed = self._uppercase_transform(data)
        elif self.transform_type == 'normalize':
            transformed = self._normalize_transform(data)
        else:
            transformed = data
        
        return {
            "transformed_data": transformed,
            "transform_type": self.transform_type,
            "original_keys": list(data.keys()) if isinstance(data, dict) else []
        }
    
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