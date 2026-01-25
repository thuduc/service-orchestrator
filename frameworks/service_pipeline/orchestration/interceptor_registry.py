import json
import importlib
from typing import Dict, Any, List, Optional
from ..contract import Interceptor
import logging

logger = logging.getLogger(__name__)


class InterceptorRegistry:
    """Manages interceptor registration and configuration"""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize registry with optional configuration

        Args:
            config_path: Path to the interceptors.json configuration file
        """
        self.config_path = config_path
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._interceptor_cache: Dict[str, Interceptor] = {}
        self.global_config: Dict[str, Any] = {}

        if config_path:
            self.load_configuration()

    def load_configuration(self):
        """Load interceptor definitions from interceptors.json"""
        try:
            if self.config_path is None:
                return
            with open(self.config_path, 'r') as f:
                config = json.load(f)

            # Load global configuration
            self.global_config = config.get('global_config', {})

            # Load interceptor definitions
            if 'interceptors' in config:
                for name, interceptor_config in config['interceptors'].items():
                    self.register_interceptor(name, interceptor_config)

            logger.info(f"Loaded {len(self._registry)} interceptor definitions")

        except FileNotFoundError:
            logger.warning(f"Interceptor configuration file not found: {self.config_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in interceptor configuration: {e}")
            raise

    def register_interceptor(self, name: str, config: Dict[str, Any]):
        """
        Register an interceptor with its configuration

        Args:
            name: Unique name for the interceptor
            config: Interceptor configuration including module, class, and settings
        """
        required_fields = ['module', 'class']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Interceptor '{name}' missing required field: {field}")

        self._registry[name] = {
            'module': config['module'],
            'class': config['class'],
            'enabled': config.get('enabled', True),
            'order': config.get('order', 999),
            'config': config.get('config', {}),
            'scope': config.get('scope', {})
        }

        logger.debug(f"Registered interceptor '{name}': {config['module']}.{config['class']}")

    def get_interceptor(self, name: str) -> Optional[Interceptor]:
        """
        Get or create an interceptor instance

        Args:
            name: Name of the interceptor

        Returns:
            Interceptor instance or None if not found/disabled
        """
        if name not in self._registry:
            logger.warning(f"Interceptor '{name}' not found in registry")
            return None

        interceptor_info = self._registry[name]

        # Check if interceptor is enabled
        if not interceptor_info['enabled']:
            logger.debug(f"Interceptor '{name}' is disabled")
            return None

        # Check cache first
        if name not in self._interceptor_cache:
            try:
                # Dynamically import and instantiate
                module = importlib.import_module(interceptor_info['module'])
                interceptor_class = getattr(module, interceptor_info['class'])

                # Instantiate with configuration
                config = interceptor_info['config']
                interceptor_instance = interceptor_class(config)

                # Verify it implements Interceptor interface
                if not isinstance(interceptor_instance, Interceptor):
                    raise TypeError(
                        f"Class {interceptor_info['class']} does not implement Interceptor interface"
                    )

                self._interceptor_cache[name] = interceptor_instance
                logger.info(f"Created interceptor instance: {name}")

            except (ImportError, AttributeError) as e:
                logger.error(f"Failed to load interceptor '{name}': {e}")
                return None
            except Exception as e:
                logger.error(f"Failed to instantiate interceptor '{name}': {e}")
                return None

        return self._interceptor_cache[name]

    def get_enabled_interceptors(self) -> List[Interceptor]:
        """
        Get all enabled interceptors sorted by order

        Returns:
            List of interceptor instances sorted by their order value
        """
        # Get all enabled interceptor configurations
        enabled_configs = [
            (name, info) for name, info in self._registry.items()
            if info['enabled']
        ]

        # Sort by order value
        enabled_configs.sort(key=lambda x: x[1]['order'])

        # Get interceptor instances
        interceptors = []
        for name, _ in enabled_configs:
            interceptor = self.get_interceptor(name)
            if interceptor:
                interceptors.append(interceptor)

        logger.info(f"Retrieved {len(interceptors)} enabled interceptor(s)")
        return interceptors

    def get_enabled_interceptors_for_service(self, service_id: str) -> List[Interceptor]:
        """
        Get enabled interceptors for a specific service, sorted by order.

        Args:
            service_id: Service identifier to filter interceptors

        Returns:
            List of interceptor instances sorted by their order value
        """
        enabled_configs = [
            (name, info) for name, info in self._registry.items()
            if info['enabled']
        ]

        scoped_configs = []
        for name, info in enabled_configs:
            scope = info.get('scope', {})
            include_services = scope.get('include_services')
            exclude_services = scope.get('exclude_services')

            if isinstance(include_services, list) and include_services:
                if service_id not in include_services:
                    continue

            if isinstance(exclude_services, list) and exclude_services:
                if service_id in exclude_services:
                    continue

            scoped_configs.append((name, info))

        scoped_configs.sort(key=lambda x: x[1]['order'])

        interceptors = []
        for name, _ in scoped_configs:
            interceptor = self.get_interceptor(name)
            if interceptor:
                interceptors.append(interceptor)

        logger.info(
            f"Retrieved {len(interceptors)} enabled interceptor(s) for service '{service_id}'"
        )
        return interceptors

    def list_interceptors(self) -> Dict[str, Dict[str, Any]]:
        """
        List all registered interceptors with their status

        Returns:
            Dictionary of interceptor information
        """
        result = {}
        for name, info in self._registry.items():
            result[name] = {
                'module': info['module'],
                'class': info['class'],
                'enabled': info['enabled'],
                'order': info['order'],
                'cached': name in self._interceptor_cache,
                'scope': info.get('scope', {})
            }
        return result

    def enable_interceptor(self, name: str):
        """Enable a registered interceptor"""
        if name in self._registry:
            self._registry[name]['enabled'] = True
            logger.info(f"Enabled interceptor: {name}")

    def disable_interceptor(self, name: str):
        """Disable a registered interceptor"""
        if name in self._registry:
            self._registry[name]['enabled'] = False
            # Remove from cache to free resources
            if name in self._interceptor_cache:
                del self._interceptor_cache[name]
            logger.info(f"Disabled interceptor: {name}")
