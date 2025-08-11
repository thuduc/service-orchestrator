from abc import ABC, abstractmethod
from typing import Dict, Any, Callable


class Middleware(ABC):
    """Base class for all middleware"""
    
    @abstractmethod
    def process(self, context: Dict[str, Any], next_handler: Callable) -> Dict[str, Any]:
        """
        Process the request through this middleware
        
        Args:
            context: The request context
            next_handler: The next handler in the chain
            
        Returns:
            The response dictionary
        """
        pass