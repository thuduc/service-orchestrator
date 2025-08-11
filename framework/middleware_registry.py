import json
import importlib
from typing import Dict, Any, List, Optional
from .middleware import Middleware
import logging

logger = logging.getLogger(__name__)


class MiddlewareRegistry:
    """Manages middleware registration and configuration"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize registry with optional configuration
        
        Args:
            config_path: Path to the middlewares.json configuration file
        """
        self.config_path = config_path
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._middleware_cache: Dict[str, Middleware] = {}
        self.global_config: Dict[str, Any] = {}
        
        if config_path:
            self.load_configuration()
    
    def load_configuration(self):
        """Load middleware definitions from middlewares.json"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Load global configuration
            self.global_config = config.get('global_config', {})
            
            # Load middleware definitions
            if 'middlewares' in config:
                for name, middleware_config in config['middlewares'].items():
                    self.register_middleware(name, middleware_config)
                    
            logger.info(f"Loaded {len(self._registry)} middleware definitions")
            
        except FileNotFoundError:
            logger.warning(f"Middleware configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in middleware configuration: {e}")
            raise
    
    def register_middleware(self, name: str, config: Dict[str, Any]):
        """
        Register a middleware with its configuration
        
        Args:
            name: Unique name for the middleware
            config: Middleware configuration including module, class, and settings
        """
        required_fields = ['module', 'class']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Middleware '{name}' missing required field: {field}")
        
        self._registry[name] = {
            'module': config['module'],
            'class': config['class'],
            'enabled': config.get('enabled', True),
            'order': config.get('order', 999),
            'config': config.get('config', {})
        }
        
        logger.debug(f"Registered middleware '{name}': {config['module']}.{config['class']}")
    
    def get_middleware(self, name: str) -> Optional[Middleware]:
        """
        Get or create a middleware instance
        
        Args:
            name: Name of the middleware
            
        Returns:
            Middleware instance or None if not found/disabled
        """
        if name not in self._registry:
            logger.warning(f"Middleware '{name}' not found in registry")
            return None
        
        middleware_info = self._registry[name]
        
        # Check if middleware is enabled
        if not middleware_info['enabled']:
            logger.debug(f"Middleware '{name}' is disabled")
            return None
        
        # Check cache first
        if name not in self._middleware_cache:
            try:
                # Dynamically import and instantiate
                module = importlib.import_module(middleware_info['module'])
                middleware_class = getattr(module, middleware_info['class'])
                
                # Instantiate with configuration
                config = middleware_info['config']
                middleware_instance = middleware_class(config)
                
                # Verify it implements Middleware interface
                if not isinstance(middleware_instance, Middleware):
                    raise TypeError(
                        f"Class {middleware_info['class']} does not implement Middleware interface"
                    )
                
                self._middleware_cache[name] = middleware_instance
                logger.info(f"Created middleware instance: {name}")
                
            except (ImportError, AttributeError) as e:
                logger.error(f"Failed to load middleware '{name}': {e}")
                return None
            except Exception as e:
                logger.error(f"Failed to instantiate middleware '{name}': {e}")
                return None
        
        return self._middleware_cache[name]
    
    def get_enabled_middlewares(self) -> List[Middleware]:
        """
        Get all enabled middlewares sorted by order
        
        Returns:
            List of middleware instances sorted by their order value
        """
        # Get all enabled middleware configurations
        enabled_configs = [
            (name, info) for name, info in self._registry.items()
            if info['enabled']
        ]
        
        # Sort by order value
        enabled_configs.sort(key=lambda x: x[1]['order'])
        
        # Get middleware instances
        middlewares = []
        for name, _ in enabled_configs:
            middleware = self.get_middleware(name)
            if middleware:
                middlewares.append(middleware)
        
        logger.info(f"Retrieved {len(middlewares)} enabled middleware(s)")
        return middlewares
    
    def list_middlewares(self) -> Dict[str, Dict[str, Any]]:
        """
        List all registered middlewares with their status
        
        Returns:
            Dictionary of middleware information
        """
        result = {}
        for name, info in self._registry.items():
            result[name] = {
                'module': info['module'],
                'class': info['class'],
                'enabled': info['enabled'],
                'order': info['order'],
                'cached': name in self._middleware_cache
            }
        return result
    
    def enable_middleware(self, name: str):
        """Enable a registered middleware"""
        if name in self._registry:
            self._registry[name]['enabled'] = True
            logger.info(f"Enabled middleware: {name}")
    
    def disable_middleware(self, name: str):
        """Disable a registered middleware"""
        if name in self._registry:
            self._registry[name]['enabled'] = False
            # Remove from cache to free resources
            if name in self._middleware_cache:
                del self._middleware_cache[name]
            logger.info(f"Disabled middleware: {name}")