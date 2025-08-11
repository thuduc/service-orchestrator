import json
from typing import Dict, Any, Optional, List
from .steps_executor import StepsExecutor
import logging

logger = logging.getLogger(__name__)


class ServiceRegistry:
    """Manages service registration and lookup for steps-based services"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the service registry
        
        Args:
            config_path: Path to the JSON configuration file
        """
        self.config_path = config_path
        self._registry: Dict[str, List[Dict[str, Any]]] = {}
        self._executor_cache: Dict[str, StepsExecutor] = {}
        
        if config_path:
            self._load_configuration()
    
    def _load_configuration(self):
        """Load service definitions from JSON configuration"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                
            if 'services' not in config:
                raise ValueError("Configuration must contain 'services' key")
            
            for service_id, service_config in config['services'].items():
                if 'steps' not in service_config:
                    raise ValueError(f"Service '{service_id}' must have 'steps' array")
                
                if not isinstance(service_config['steps'], list):
                    raise ValueError(f"Service '{service_id}' steps must be an array")
                
                if len(service_config['steps']) == 0:
                    raise ValueError(f"Service '{service_id}' must have at least one step")
                
                self.register_service(service_id, service_config['steps'])
                logger.info(f"Registered service '{service_id}' with {len(service_config['steps'])} steps")
                
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def register_service(self, service_id: str, steps: List[Dict[str, Any]]):
        """
        Register a new service with its steps configuration
        
        Args:
            service_id: Unique identifier for the service
            steps: List of step configurations
        """
        # Validate each step has required fields
        for i, step in enumerate(steps):
            if 'module' not in step or 'class' not in step:
                raise ValueError(
                    f"Service '{service_id}' step {i} must have 'module' and 'class' fields"
                )
            
            # Ensure step has a name (generate if not provided)
            if 'name' not in step:
                step['name'] = f"step_{i+1}"
        
        self._registry[service_id] = steps
    
    def get_executor(self, service_id: str) -> StepsExecutor:
        """
        Get or create a StepsExecutor for the service
        
        Args:
            service_id: Unique identifier for the service
            
        Returns:
            StepsExecutor instance for the service
            
        Raises:
            KeyError: If service_id is not registered
        """
        if service_id not in self._registry:
            raise KeyError(f"Service '{service_id}' not found in registry")
        
        # Check cache first
        if service_id not in self._executor_cache:
            # Create new executor for this service
            steps_config = self._registry[service_id]
            self._executor_cache[service_id] = StepsExecutor(steps_config)
            logger.info(f"Created executor for service '{service_id}'")
        
        return self._executor_cache[service_id]
    
    def list_services(self) -> Dict[str, int]:
        """
        List all registered services
        
        Returns:
            Dictionary mapping service_id to number of steps
        """
        return {
            service_id: len(steps) 
            for service_id, steps in self._registry.items()
        }
    
    def get_service_info(self, service_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a service
        
        Args:
            service_id: Unique identifier for the service
            
        Returns:
            Service configuration including steps
        """
        if service_id not in self._registry:
            raise KeyError(f"Service '{service_id}' not found in registry")
        
        steps = self._registry[service_id]
        return {
            'service_id': service_id,
            'steps': [
                {
                    'name': step.get('name'),
                    'module': step.get('module'),
                    'class': step.get('class'),
                    'has_input_mapping': bool(step.get('input_mapping')),
                    'has_output_mapping': bool(step.get('output_mapping'))
                }
                for step in steps
            ]
        }