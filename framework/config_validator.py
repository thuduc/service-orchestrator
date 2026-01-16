import json
import importlib
from typing import Dict, Any, List, Set, Optional
import logging

logger = logging.getLogger(__name__)


class ConfigValidator:
    """Validates service and interceptor configurations"""

    def __init__(self, config_path: str, interceptor_config_path: Optional[str] = None):
        """
        Initialize the configuration validator

        Args:
            config_path: Path to the services JSON configuration file
            interceptor_config_path: Optional path to interceptors JSON configuration file
        """
        self.config_path = config_path
        self.interceptor_config_path = interceptor_config_path
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate(self) -> bool:
        """
        Validate the entire configuration file

        Returns:
            True if configuration is valid, False otherwise
        """
        self.errors = []
        self.warnings = []

        # Validate services configuration
        services_valid = self._validate_services_config()

        # Validate interceptor configuration if provided
        interceptor_valid = True
        if self.interceptor_config_path:
            interceptor_valid = self._validate_interceptor_config()

        # Log results
        if self.errors:
            for error in self.errors:
                logger.error(f"Validation error: {error}")

        if self.warnings:
            for warning in self.warnings:
                logger.warning(f"Validation warning: {warning}")

        return len(self.errors) == 0 and services_valid and interceptor_valid

    def _validate_services_config(self) -> bool:
        """Validate services configuration"""
        try:
            # Load configuration
            with open(self.config_path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            self.errors.append(f"Services configuration file not found: {self.config_path}")
            return False
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON in services config: {e}")
            return False

        # Validate structure
        if 'services' not in config:
            self.errors.append("Services configuration must contain 'services' key")
            return False

        if not isinstance(config['services'], dict):
            self.errors.append("'services' must be an object")
            return False

        # Validate each service
        for service_id, service_config in config['services'].items():
            self._validate_service(service_id, service_config)

        return True

    def _validate_service(self, service_id: str, service_config: Dict[str, Any]):
        """Validate a single service configuration"""

        # Check for steps array
        if 'steps' not in service_config:
            self.errors.append(f"Service '{service_id}': Missing 'steps' array")
            return

        if not isinstance(service_config['steps'], list):
            self.errors.append(f"Service '{service_id}': 'steps' must be an array")
            return

        if len(service_config['steps']) == 0:
            self.errors.append(f"Service '{service_id}': Must have at least one step")
            return

        # Track step names for uniqueness
        step_names: Set[str] = set()

        # Validate each step
        for i, step in enumerate(service_config['steps']):
            self._validate_step(service_id, i, step, step_names)

    def _validate_step(self, service_id: str, index: int,
                       step: Dict[str, Any], step_names: Set[str]):
        """Validate a single step configuration"""

        # Check required fields
        if 'module' not in step:
            self.errors.append(
                f"Service '{service_id}' step {index}: Missing 'module' field"
            )

        if 'class' not in step:
            self.errors.append(
                f"Service '{service_id}' step {index}: Missing 'class' field"
            )

        # Check step name uniqueness
        step_name = step.get('name', f'step_{index+1}')
        if step_name in step_names:
            self.errors.append(
                f"Service '{service_id}': Duplicate step name '{step_name}'"
            )
        step_names.add(step_name)

        # Validate module can be imported
        if 'module' in step:
            module_name = step['module']
            class_name = step.get('class', '')

            try:
                module = importlib.import_module(module_name)
                if class_name and not hasattr(module, class_name):
                    self.errors.append(
                        f"Service '{service_id}' step {index}: "
                        f"Class '{class_name}' not found in module '{module_name}'"
                    )
            except ImportError as e:
                self.errors.append(
                    f"Service '{service_id}' step {index}: "
                    f"Cannot import module '{module_name}': {e}"
                )

        # Validate error handling
        if 'on_error' in step:
            valid_strategies = ['fail_fast', 'skip', 'compensate']
            if step['on_error'] not in valid_strategies:
                self.errors.append(
                    f"Service '{service_id}' step {index}: "
                    f"Invalid on_error value '{step['on_error']}'. "
                    f"Must be one of: {valid_strategies}"
                )

            if step['on_error'] == 'skip' and 'fallback_output' not in step:
                self.warnings.append(
                    f"Service '{service_id}' step {index}: "
                    f"Using 'skip' without 'fallback_output' may cause issues"
                )

    def _validate_interceptor_config(self) -> bool:
        """Validate interceptor configuration"""
        try:
            # Load configuration
            with open(self.interceptor_config_path, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            self.warnings.append(f"Interceptor configuration file not found: {self.interceptor_config_path}")
            return True  # Not an error, interceptor config is optional
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON in interceptor config: {e}")
            return False

        # Validate structure
        if 'interceptors' not in config:
            self.warnings.append("Interceptor configuration should contain 'interceptors' key")

        if 'interceptors' in config:
            if not isinstance(config['interceptors'], dict):
                self.errors.append("'interceptors' must be an object")
                return False

            # Validate each interceptor
            orders_used = {}
            for name, interceptor_config in config['interceptors'].items():
                self._validate_interceptor(name, interceptor_config, orders_used)

        return True

    def _validate_interceptor(self, name: str, config: Dict[str, Any], orders_used: Dict[int, str]):
        """Validate a single interceptor configuration"""

        # Check required fields
        if 'module' not in config:
            self.errors.append(f"Interceptor '{name}': Missing 'module' field")

        if 'class' not in config:
            self.errors.append(f"Interceptor '{name}': Missing 'class' field")

        # Check order conflicts
        if 'order' in config:
            order = config['order']
            if order in orders_used:
                self.warnings.append(
                    f"Interceptor '{name}': Order {order} already used by '{orders_used[order]}'"
                )
            else:
                orders_used[order] = name

        # Validate module can be imported
        if 'module' in config:
            module_name = config['module']
            class_name = config.get('class', '')

            try:
                module = importlib.import_module(module_name)
                if class_name and not hasattr(module, class_name):
                    self.errors.append(
                        f"Interceptor '{name}': Class '{class_name}' not found in module '{module_name}'"
                    )
            except ImportError as e:
                self.errors.append(
                    f"Interceptor '{name}': Cannot import module '{module_name}': {e}"
                )

        # Validate enabled flag
        if 'enabled' in config and not isinstance(config['enabled'], bool):
            self.errors.append(f"Interceptor '{name}': 'enabled' must be a boolean")

    def get_report(self) -> Dict[str, Any]:
        """
        Get a detailed validation report

        Returns:
            Dictionary with validation results
        """
        return {
            'valid': len(self.errors) == 0,
            'errors': self.errors,
            'warnings': self.warnings,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings)
        }


def validate_config(config_path: str, interceptor_config_path: Optional[str] = None) -> bool:
    """
    Convenience function to validate configuration files

    Args:
        config_path: Path to the services configuration file
        interceptor_config_path: Optional path to interceptor configuration file

    Returns:
        True if valid, False otherwise
    """
    validator = ConfigValidator(config_path, interceptor_config_path)
    is_valid = validator.validate()

    report = validator.get_report()
    if not is_valid:
        print(f"Configuration validation failed with {report['error_count']} errors:")
        for error in report['errors']:
            print(f"  ERROR: {error}")

    if report['warnings']:
        print(f"Configuration has {report['warning_count']} warnings:")
        for warning in report['warnings']:
            print(f"  WARNING: {warning}")

    return is_valid
