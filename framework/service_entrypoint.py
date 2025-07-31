from typing import Dict, Any, Optional
from .service_registry import ServiceRegistry
from .middleware import MiddlewarePipeline


class ServiceEntrypoint:
    """Main microservice entrypoint"""
    
    def __init__(self, registry: ServiceRegistry, 
                 middleware_pipeline: Optional[MiddlewarePipeline] = None):
        """
        Initialize the service entrypoint
        
        Args:
            registry: The service registry for component lookup
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
            Response dictionary from the component execution
            
        Raises:
            KeyError: If 'service_id' is not in context
            Exception: Any exception from component execution
        """
        # Extract service_id from context
        if 'service_id' not in context:
            raise KeyError("'service_id' is required in the context")
        
        service_id = context['service_id']
        
        try:
            # Get component from registry
            component = self.registry.get_component(service_id)
            
            # Execute through middleware pipeline
            result = self.middleware_pipeline.execute(context, component)
            
            return result
            
        except Exception as e:
            # Re-raise exceptions for now, but could add error handling here
            raise e