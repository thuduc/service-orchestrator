import json
import importlib
from typing import Dict, Any, Optional
from .component import Component


class ServiceRegistry:
    """Manages component registration and lookup"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the service registry
        
        Args:
            config_path: Path to the JSON configuration file
        """
        self.config_path = config_path
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._component_cache: Dict[str, Component] = {}
        
        if config_path:
            self._load_configuration()
    
    def _load_configuration(self):
        """Load service mappings from JSON configuration"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                
            if 'services' in config:
                for service_id, service_config in config['services'].items():
                    self.register_component(
                        service_id=service_id,
                        module_path=service_config['module'],
                        class_name=service_config['class'],
                        config=service_config.get('config', {})
                    )
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def register_component(self, service_id: str, module_path: str, 
                         class_name: str, config: Optional[Dict[str, Any]] = None):
        """
        Register a new component
        
        Args:
            service_id: Unique identifier for the service
            module_path: Python module path (e.g., 'components.user_service')
            class_name: Name of the component class
            config: Optional configuration for the component
        """
        self._registry[service_id] = {
            'module_path': module_path,
            'class_name': class_name,
            'config': config or {}
        }
    
    def get_component(self, service_id: str) -> Component:
        """
        Dynamically load and instantiate component
        
        Args:
            service_id: Unique identifier for the service
            
        Returns:
            Instantiated component
            
        Raises:
            KeyError: If service_id is not registered
            ImportError: If module cannot be imported
            AttributeError: If class cannot be found in module
        """
        if service_id not in self._registry:
            raise KeyError(f"Service '{service_id}' not found in registry")
        
        # Check cache first
        if service_id in self._component_cache:
            return self._component_cache[service_id]
        
        service_info = self._registry[service_id]
        module_path = service_info['module_path']
        class_name = service_info['class_name']
        config = service_info['config']
        
        try:
            # Dynamically import the module
            module = importlib.import_module(module_path)
            
            # Get the component class
            component_class = getattr(module, class_name)
            
            # Instantiate the component
            if config:
                component = component_class(**config)
            else:
                component = component_class()
            
            # Verify it implements the Component interface
            if not isinstance(component, Component):
                raise TypeError(
                    f"Class {class_name} does not implement the Component interface"
                )
            
            # Cache the component instance
            self._component_cache[service_id] = component
            
            return component
            
        except ImportError as e:
            raise ImportError(f"Failed to import module '{module_path}': {e}")
        except AttributeError as e:
            raise AttributeError(
                f"Class '{class_name}' not found in module '{module_path}': {e}"
            )
    
    def list_services(self) -> Dict[str, str]:
        """
        List all registered services
        
        Returns:
            Dictionary mapping service_id to module_path
        """
        return {
            service_id: info['module_path'] 
            for service_id, info in self._registry.items()
        }