import logging
import time
from typing import Dict, Any, Callable
from .middleware import Middleware


class LoggingMiddleware(Middleware):
    """Middleware for logging component execution"""
    
    def __init__(self, logger: logging.Logger = None):
        """
        Initialize the logging middleware
        
        Args:
            logger: Optional logger instance. If not provided, uses default logger
        """
        self.logger = logger or logging.getLogger(__name__)
    
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
        self.logger.info(
            f"Starting execution - Service: {service_id}, Request ID: {request_id}"
        )
        self.logger.debug(f"Context: {context}")
        
        start_time = time.time()
        result = None
        error = None
        
        try:
            # Execute component
            result = next_handler(context)
            return result
            
        except Exception as e:
            error = e
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
                self.logger.info(
                    f"Completed execution - Service: {service_id}, "
                    f"Request ID: {request_id}, "
                    f"Execution time: {execution_time:.3f}s"
                )
                if result:
                    self.logger.debug(f"Result: {result}")
            else:
                self.logger.error(
                    f"Failed execution - Service: {service_id}, "
                    f"Request ID: {request_id}, "
                    f"Execution time: {execution_time:.3f}s, "
                    f"Error: {str(error)}"
                )