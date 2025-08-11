from typing import Dict, Any, Optional
from .service_registry import ServiceRegistry
from .middleware import MiddlewarePipeline
import logging

logger = logging.getLogger(__name__)


class ServiceEntrypoint:
    """Main microservice entrypoint for steps-based services"""
    
    def __init__(self, registry: ServiceRegistry, 
                 middleware_pipeline: Optional[MiddlewarePipeline] = None):
        """
        Initialize the service entrypoint
        
        Args:
            registry: The service registry for executor lookup
            middleware_pipeline: Optional middleware pipeline
        """
        self.registry = registry
        self.middleware_pipeline = middleware_pipeline or MiddlewarePipeline()
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main execution method with middleware support
        
        Args:
            context: Request context containing at minimum 'service_id'
            
        Returns:
            Response dictionary from the service execution
            
        Raises:
            KeyError: If 'service_id' is not in context
            Exception: Any exception from service execution
        """
        # Extract service_id from context
        if 'service_id' not in context:
            raise KeyError("'service_id' is required in the context")
        
        service_id = context['service_id']
        logger.info(f"Executing service: {service_id}")
        
        try:
            # Get steps executor from registry
            executor = self.registry.get_executor(service_id)
            
            # Create a wrapper to make executor compatible with middleware
            class ExecutorWrapper:
                def __init__(self, steps_executor):
                    self._executor = steps_executor
                
                def execute(self, ctx):
                    return self._executor.execute(ctx)
            
            wrapper = ExecutorWrapper(executor)
            
            # Execute through middleware pipeline
            result = self.middleware_pipeline.execute(context, wrapper)
            
            logger.info(f"Service '{service_id}' executed successfully")
            return result
            
        except KeyError as e:
            logger.error(f"Service not found: {e}")
            raise
        except Exception as e:
            logger.error(f"Service execution failed: {e}")
            raise