from typing import Dict, Any, Optional
from .service_registry import ServiceRegistry
from .middleware_pipeline import MiddlewarePipeline
from .middleware_registry import MiddlewareRegistry
import logging

logger = logging.getLogger(__name__)


class ServiceEntrypoint:
    """Main microservice entrypoint for steps-based services"""
    
    def __init__(self, registry: ServiceRegistry, 
                 middleware_pipeline: Optional[MiddlewarePipeline] = None,
                 middleware_config_path: Optional[str] = 'middlewares.json'):
        """
        Initialize the service entrypoint
        
        Args:
            registry: The service registry for executor lookup
            middleware_pipeline: Optional pre-configured middleware pipeline
            middleware_config_path: Path to middleware configuration file
        """
        self.registry = registry
        
        # Use provided pipeline or create new one with auto-registration
        if middleware_pipeline:
            self.middleware_pipeline = middleware_pipeline
        else:
            self.middleware_pipeline = self._build_pipeline(middleware_config_path)
    
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
    
    def _build_pipeline(self, middleware_config_path: Optional[str]) -> MiddlewarePipeline:
        """
        Build middleware pipeline from configuration
        
        Args:
            middleware_config_path: Path to middleware configuration file
            
        Returns:
            Configured middleware pipeline
        """
        pipeline = MiddlewarePipeline()
        
        if middleware_config_path:
            try:
                # Create middleware registry and load configuration
                middleware_registry = MiddlewareRegistry(middleware_config_path)
                
                # Get all enabled middlewares sorted by order
                middlewares = middleware_registry.get_enabled_middlewares()
                
                # Add each middleware to the pipeline
                for middleware in middlewares:
                    pipeline.add_middleware(middleware)
                
                logger.info(f"Loaded {len(middlewares)} middleware(s) from {middleware_config_path}")
            except Exception as e:
                logger.warning(f"Failed to load middleware configuration: {e}")
        
        return pipeline