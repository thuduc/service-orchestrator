from abc import ABC, abstractmethod
from typing import Dict, Any


class Component(ABC):
    """Base contract for all components in the framework"""
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the component logic
        
        Args:
            context: Dictionary containing input data and parameters
            
        Returns:
            Dictionary containing the execution results
        """
        pass
