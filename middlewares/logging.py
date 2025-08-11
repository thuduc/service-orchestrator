import logging
import time
import os
from typing import Dict, Any, Callable, List, Optional
from framework.middleware import Middleware


class ContextLogger:
    """A context-aware logger that automatically includes request context in log messages"""
    
    def __init__(self, base_logger: logging.Logger, context: Dict[str, Any]):
        """
        Initialize the context logger
        
        Args:
            base_logger: The underlying Python logger
            context: The request context
        """
        self.base_logger = base_logger
        self.context = context
        self.service_id = context.get('service_id', 'unknown')
        self.request_id = context.get('request_id', 'N/A')
    
    def _format_message(self, message: str) -> str:
        """Add context information to log message"""
        return f"[Service: {self.service_id}, Request: {self.request_id}] {message}"
    
    def debug(self, message: str, *args, **kwargs):
        """Log a debug message with context"""
        self.base_logger.debug(self._format_message(message), *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        """Log an info message with context"""
        self.base_logger.info(self._format_message(message), *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        """Log a warning message with context"""
        self.base_logger.warning(self._format_message(message), *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        """Log an error message with context"""
        self.base_logger.error(self._format_message(message), *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        """Log a critical message with context"""
        self.base_logger.critical(self._format_message(message), *args, **kwargs)


class LoggingMiddleware(Middleware):
    """Middleware for logging component execution with multiple destinations"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the logging middleware with configuration
        
        Args:
            config: Configuration dictionary with options:
                - log_level: Logging level (default: INFO)
                - log_request: Whether to log requests (default: True)
                - log_response: Whether to log responses (default: True)
                - log_errors: Whether to log errors (default: True)
                - provide_context_logger: Whether to provide logger in context (default: True)
                - destinations: List of log destinations (default: ['stdout'])
                  Supported: 'stdout', 'file'
                - file_path: Path to log file when 'file' destination is used
                - log_format: Log format string (default: standard format)
                - date_format: Date format string (default: '%Y-%m-%d %H:%M:%S')
        """
        self.config = config or {}
        
        # Configure logging options
        log_level_str = self.config.get('log_level', 'INFO')
        self.log_level = getattr(logging, log_level_str.upper(), logging.INFO)
        self.log_request = self.config.get('log_request', True)
        self.log_response = self.config.get('log_response', True)
        self.log_errors = self.config.get('log_errors', True)
        self.provide_context_logger = self.config.get('provide_context_logger', True)
        
        # Configure destinations
        self.destinations = self.config.get('destinations', ['stdout'])
        self.file_path = self.config.get('file_path', 'logs/service.log')
        
        # Configure format
        self.log_format = self.config.get(
            'log_format',
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.date_format = self.config.get('date_format', '%Y-%m-%d %H:%M:%S')
        
        # Set up logger with multiple handlers
        self.logger = self._setup_logger()
        
        # Set up component logger for context-aware logging
        self.component_logger = self._setup_component_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up the main middleware logger with configured handlers"""
        logger = logging.getLogger(__name__)
        logger.setLevel(self.log_level)
        
        # Remove existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Prevent propagation to avoid duplicates
        logger.propagate = False
        
        # Create formatter
        formatter = logging.Formatter(self.log_format, self.date_format)
        
        # Add handlers based on destinations
        for destination in self.destinations:
            handler = self._create_handler(destination)
            if handler:
                handler.setFormatter(formatter)
                handler.setLevel(self.log_level)
                logger.addHandler(handler)
        
        return logger
    
    def _setup_component_logger(self) -> logging.Logger:
        """Set up the component logger with configured handlers"""
        logger = logging.getLogger('components')
        logger.setLevel(self.log_level)
        
        # Remove existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Prevent propagation to avoid duplicates
        logger.propagate = False
        
        # Create formatter
        formatter = logging.Formatter(self.log_format, self.date_format)
        
        # Add handlers based on destinations
        for destination in self.destinations:
            handler = self._create_handler(destination)
            if handler:
                handler.setFormatter(formatter)
                handler.setLevel(self.log_level)
                logger.addHandler(handler)
        
        return logger
    
    def _create_handler(self, destination: str) -> Optional[logging.Handler]:
        """
        Create a logging handler based on destination type
        
        Args:
            destination: The destination type ('stdout', 'file')
            
        Returns:
            Configured logging handler or None if destination is unknown
        """
        if destination == 'stdout':
            return logging.StreamHandler()
        elif destination == 'file':
            # Create log directory if it doesn't exist
            log_dir = os.path.dirname(self.file_path)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)
            
            # Create file handler with rotation support
            handler = logging.FileHandler(self.file_path, mode='a')
            return handler
        else:
            self.logger.warning(f"Unknown log destination: {destination}")
            return None
    
    def process(self, context: Dict[str, Any], next_handler: Callable) -> Dict[str, Any]:
        """
        Log component execution details and provide context logger
        
        Args:
            context: The request context
            next_handler: The next handler in the chain
            
        Returns:
            The response from the next handler
        """
        service_id = context.get('service_id', 'unknown')
        request_id = context.get('request_id', 'N/A')
        
        # Add context-aware logger to the context for components to use
        if self.provide_context_logger:
            # Use the pre-configured component logger
            context['_logger'] = ContextLogger(self.component_logger, context)
        
        # Log before execution
        if self.log_request:
            self.logger.info(
                f"Starting execution - Service: {service_id}, Request ID: {request_id}"
            )
            if self.logger.isEnabledFor(logging.DEBUG):
                # Don't log the _logger object itself to avoid recursion
                context_copy = {k: v for k, v in context.items() if k != '_logger'}
                self.logger.debug(f"Context: {context_copy}")
        
        start_time = time.time()
        result = None
        error = None
        
        try:
            # Execute next handler in chain
            result = next_handler(context)
            return result
            
        except Exception as e:
            error = e
            if self.log_errors:
                self.logger.error(
                    f"Error in service {service_id}: {str(e)}", 
                    exc_info=True
                )
            raise
            
        finally:
            # Remove the context logger before returning (clean up)
            if '_logger' in context:
                del context['_logger']
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Log after execution
            if error is None:
                if self.log_response:
                    self.logger.info(
                        f"Completed execution - Service: {service_id}, "
                        f"Request ID: {request_id}, "
                        f"Execution time: {execution_time:.3f}s"
                    )
                    if result and self.logger.isEnabledFor(logging.DEBUG):
                        # Don't log internal keys
                        result_copy = {k: v for k, v in result.items() 
                                     if not k.startswith('_')} if isinstance(result, dict) else result
                        self.logger.debug(f"Result: {result_copy}")
            else:
                if self.log_errors:
                    self.logger.error(
                        f"Failed execution - Service: {service_id}, "
                        f"Request ID: {request_id}, "
                        f"Execution time: {execution_time:.3f}s, "
                        f"Error: {str(error)}"
                    )