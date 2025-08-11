import logging
import time
from typing import Dict, Any, Callable
from framework.middleware import Middleware


class LoggingMiddleware(Middleware):
    """Middleware for logging component execution"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the logging middleware with configuration
        
        Args:
            config: Configuration dictionary with options:
                - log_level: Logging level (default: INFO)
                - log_request: Whether to log requests (default: True)
                - log_response: Whether to log responses (default: True)
                - log_errors: Whether to log errors (default: True)
        """
        self.config = config or {}
        
        # Configure logging options
        log_level_str = self.config.get('log_level', 'INFO')
        self.log_level = getattr(logging, log_level_str.upper(), logging.INFO)
        self.log_request = self.config.get('log_request', True)
        self.log_response = self.config.get('log_response', True)
        self.log_errors = self.config.get('log_errors', True)
        
        # Set up logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
    
    def process(self, context: Dict[str, Any], next_handler: Callable) -> Dict[str, Any]:
        """
        Log component execution details
        
        Args:
            context: The request context
            next_handler: The next handler in the chain
            
        Returns:
            The response from the next handler
        """
        service_id = context.get('service_id', 'unknown')
        request_id = context.get('request_id', 'N/A')
        
        # Log before execution
        if self.log_request:
            self.logger.info(
                f"Starting execution - Service: {service_id}, Request ID: {request_id}"
            )
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Context: {context}")
        
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
                        self.logger.debug(f"Result: {result}")
            else:
                if self.log_errors:
                    self.logger.error(
                        f"Failed execution - Service: {service_id}, "
                        f"Request ID: {request_id}, "
                        f"Execution time: {execution_time:.3f}s, "
                        f"Error: {str(error)}"
                    )