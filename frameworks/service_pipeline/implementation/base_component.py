"""
Base component class that provides common functionality for all components
"""

import logging
from typing import Dict, Any
from ..contract import Component


class BaseComponent(Component):
    """
    Base component that extends Component with common functionality.
    
    This class provides:
    - Automatic logger configuration from context or fallback
    - Common initialization patterns
    - Shared utility methods for all components
    """
    
    def __init__(self, config: Dict[str, Any] | None = None):
        """
        Initialize the base component with configuration.
        
        Args:
            config: Optional configuration dictionary for the component
        """
        self.config = config or {}
        self.logger = None  # Will be set during execute
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Base execute method that sets up the logger.
        
        Subclasses should call super().execute(context) at the beginning
        of their execute method to ensure logger is properly configured.
        
        Args:
            context: The execution context
            
        Returns:
            The context (unchanged by base class)
        """
        # Set up logger from context or create default
        self._setup_logger(context)
        
        # Return context unchanged - subclasses will implement their logic
        return context
    
    def _setup_logger(self, context: Dict[str, Any]) -> None:
        """
        Set up the logger from context or create a default one.
        
        Args:
            context: The execution context that may contain a logger
        """
        # Get logger from context or use default
        self.logger = context.get('_logger', logging.getLogger(self.__class__.__name__))
        
        # If we got a default logger, ensure it has a handler
        if not isinstance(self.logger, type(context.get('_logger'))):
            # This is a fallback logger, ensure it's configured
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
    
    def log_debug(self, message: str, *args, **kwargs):
        """Convenience method for debug logging"""
        if self.logger:
            self.logger.debug(message, *args, **kwargs)
    
    def log_info(self, message: str, *args, **kwargs):
        """Convenience method for info logging"""
        if self.logger:
            self.logger.info(message, *args, **kwargs)
    
    def log_warning(self, message: str, *args, **kwargs):
        """Convenience method for warning logging"""
        if self.logger:
            self.logger.warning(message, *args, **kwargs)
    
    def log_error(self, message: str, *args, **kwargs):
        """Convenience method for error logging"""
        if self.logger:
            self.logger.error(message, *args, **kwargs)
