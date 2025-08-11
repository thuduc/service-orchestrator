from typing import Dict, Any, Callable, List
from .middleware import Middleware
from .component import Component


class MiddlewarePipeline:
    """Manages the middleware execution pipeline"""
    
    def __init__(self):
        """Initialize the middleware pipeline"""
        self.middlewares: List[Middleware] = []
    
    def add_middleware(self, middleware: Middleware):
        """
        Add a middleware to the pipeline
        
        Args:
            middleware: The middleware instance to add
        """
        self.middlewares.append(middleware)
    
    def clear_middlewares(self):
        """Clear all middlewares from the pipeline"""
        self.middlewares = []
    
    def execute(self, context: Dict[str, Any], component: Component) -> Dict[str, Any]:
        """
        Execute the component through the middleware pipeline
        
        Args:
            context: The request context
            component: The component to execute
            
        Returns:
            The response from the component execution
        """
        def build_chain(index: int) -> Callable:
            if index >= len(self.middlewares):
                # End of middleware chain, execute the component
                return lambda ctx: component.execute(ctx)
            
            # Build the chain recursively
            middleware = self.middlewares[index]
            next_handler = build_chain(index + 1)
            
            return lambda ctx: middleware.process(ctx, next_handler)
        
        # Start the chain execution
        chain = build_chain(0)
        return chain(context)